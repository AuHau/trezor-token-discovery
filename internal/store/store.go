package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
)

// ErrSchemaMismatch means the directory holds a store written by a different
// schema version. It is reported rather than migrated: see SchemaVersion.
var ErrSchemaMismatch = errors.New("store: schema version mismatch")

// Pagination bounds for AccountTokens. A caller that asks for nothing gets a
// sane page; a caller that asks for everything gets a bounded one, because the
// response is built in memory.
const (
	DefaultPageLimit = 100
	MaxPageLimit     = 1000
)

// Options configures the store. The zero value is valid and uses the defaults
// below.
type Options struct {
	// CacheBytes is the size of the block cache. The hot working set is the
	// upper levels of the pair index plus the balance cache, so this is the
	// knob that decides whether a discovery read touches the disk.
	CacheBytes int64

	// MemTableBytes is the write buffer. Sized for the backfill, which is one
	// long stream of small merges.
	MemTableBytes uint64

	// ReadOnly opens the store without allowing writes. Pebble still takes the
	// directory lock, so this does not permit a second process alongside a
	// running service.
	ReadOnly bool

	// Logger receives Pebble's own log output. Nil discards it.
	Logger *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.CacheBytes <= 0 {
		o.CacheBytes = 128 << 20
	}
	if o.MemTableBytes == 0 {
		o.MemTableBytes = 64 << 20
	}
	return o
}

// Store is the Pebble-backed implementation of ports.Store.
//
// Every method takes a context for interface symmetry, but Pebble is a
// synchronous local library with no cancellation plumbing, so cancellation is
// honoured at the call boundary only: a cancelled context fails fast instead of
// starting work. That is honest rather than ideal, and it is acceptable because
// these calls are local reads measured in microseconds, not network round trips.
type Store struct {
	db       *pebble.DB
	log      *slog.Logger
	readOnly bool

	// Pebble panics on a double close. Close is made idempotent here instead,
	// because the service closes the store both on the shutdown path and from a
	// defer, and a panic during shutdown would mask the real error.
	closeOnce sync.Once
	closeErr  error
}

var _ ports.Store = (*Store)(nil)

// Open opens or creates the store in dir.
func Open(dir string, opts Options) (*Store, error) {
	opts = opts.withDefaults()
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	popts := &pebble.Options{
		Merger:                      newMerger(),
		CacheSize:                   opts.CacheBytes,
		MemTableSize:                opts.MemTableBytes,
		MemTableStopWritesThreshold: 4,
		ReadOnly:                    opts.ReadOnly,
		Logger:                      pebbleLogger{log: log},
	}
	// Bloom filters on every level. The read path is dominated by point
	// lookups into the balance cache and the metadata table, where a filter
	// turns a miss into zero disk reads; 10 bits per key is the standard ~1%
	// false-positive trade.
	for i := range popts.Levels {
		popts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}

	db, err := pebble.Open(dir, popts)
	if err != nil {
		return nil, fmt.Errorf("open store %q: %w", dir, err)
	}
	s := &Store{db: db, log: log, readOnly: opts.ReadOnly}

	if err := s.checkSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// checkSchema stamps a new store with the current schema version and refuses
// one written by a different version.
func (s *Store) checkSchema() error {
	value, closer, err := s.db.Get(schemaKey)
	switch {
	case errors.Is(err, pebble.ErrNotFound):
		if s.readOnly {
			// An unstamped store is either brand new or not ours. Either way a
			// read-only caller has nothing to read.
			return fmt.Errorf("%w: store is not initialised", ErrSchemaMismatch)
		}
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], SchemaVersion)
		if err := s.db.Set(schemaKey, buf[:], pebble.Sync); err != nil {
			return fmt.Errorf("stamp schema version: %w", err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("read schema version: %w", err)
	}
	defer closer.Close()

	if len(value) != 4 {
		return fmt.Errorf("%w: schema record is %d bytes", ErrSchemaMismatch, len(value))
	}
	if got := binary.BigEndian.Uint32(value); got != SchemaVersion {
		return fmt.Errorf("%w: store is v%d, this build writes v%d (delete %s and re-run backfill)",
			ErrSchemaMismatch, got, SchemaVersion, "the data directory")
	}
	return nil
}

// Close closes the store. It is safe to call more than once.
func (s *Store) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.db.Close() })
	return s.closeErr
}

// Cursor reads the indexer's durable position. An unseeded store yields the
// zero Cursor rather than an error, which is what domain.Cursor.Seeded() is
// for.
func (s *Store) Cursor(ctx context.Context, chainID uint64) (domain.Cursor, error) {
	if err := ctx.Err(); err != nil {
		return domain.Cursor{}, err
	}
	return s.cursor(chainID)
}

func (s *Store) cursor(chainID uint64) (domain.Cursor, error) {
	value, closer, err := s.db.Get(cursorKey(chainID))
	if errors.Is(err, pebble.ErrNotFound) {
		return domain.Cursor{}, nil
	}
	if err != nil {
		return domain.Cursor{}, fmt.Errorf("read cursor: %w", err)
	}
	defer closer.Close()
	c, err := decodeCursor(value)
	if err != nil {
		return domain.Cursor{}, err
	}
	return c, nil
}

// ApplyBatch writes a scanned batch, the balance invalidations it implies, and
// the advanced cursor in one atomic Pebble batch.
//
// Writing them together is what makes a crash mid-write recoverable: on restart
// the cursor and the pairs it describes can never disagree, so the indexer
// resumes from a position it can trust instead of having to re-derive one. The
// commit is synced for the same reason — ~1.8k batches across a full backfill
// makes the fsync cost irrelevant next to the guarantee.
func (s *Store) ApplyBatch(ctx context.Context, chainID uint64, batch domain.AccountTokenBatch, cursor domain.Cursor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if batch.FromBlock > batch.ToBlock {
		return fmt.Errorf("apply batch: invalid range %d..%d", batch.FromBlock, batch.ToBlock)
	}

	b := s.db.NewBatch()
	defer b.Close()

	for _, p := range batch.Pairs {
		if !p.Standard.Valid() {
			return fmt.Errorf("apply batch: pair %s/%s has standard %q", p.Account, p.Token, p.Standard)
		}
		if err := b.Merge(pairKey(chainID, p), encodePairRange(p.FirstBlock, p.LastBlock), nil); err != nil {
			return fmt.Errorf("apply batch: merge pair: %w", err)
		}
		// Event-driven invalidation: the pair's last observed block is exactly
		// the staleness boundary for any cached balance of it.
		if err := b.Merge(balanceKey(chainID, p.Account, p.Token), encodeDirty(p.LastBlock), nil); err != nil {
			return fmt.Errorf("apply batch: stamp balance: %w", err)
		}
	}

	for _, d := range batch.Blocks {
		if err := b.Set(digestKey(chainID, d.Number), encodeDigest(d), nil); err != nil {
			return fmt.Errorf("apply batch: write digest: %w", err)
		}
	}

	// batch.NFTTransfers is deliberately not persisted here. An ownership view
	// is "last transfer wins" and needs a read side — owner-of-token and
	// tokens-of-account — whose keyspace is only decidable together with the
	// endpoint that reads it, so it lands with the ERC-721 endpoint in phase 9.
	// Nothing is lost: pairs already record the ERC-721 interaction, and a
	// re-backfill to populate the view is ~25 minutes (docs/measurements.md).

	if err := b.Set(cursorKey(chainID), encodeCursor(cursor), nil); err != nil {
		return fmt.Errorf("apply batch: write cursor: %w", err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("apply batch: commit: %w", err)
	}
	return nil
}

// Rewind drops per-block state above a common ancestor, flushes the whole
// balance cache and resets the cursor, in one batch.
//
// Account-token pairs are deliberately NOT deleted. The index asserts only
// "this account touched this token", which a reorg does not falsify in any way
// that matters: re-scanning re-adds the canonical pairs, and a pair that was
// orphaned leaves at worst an extra token in a list whose balance is then read
// live and found to be zero. Deleting them would instead mean reference-counting
// every pair per block — a large amount of machinery to remove a harmless row.
//
// The balance cache gets the opposite treatment: it is flushed wholesale,
// because a cached balance is a *claim about a block height* and a reorg
// invalidates every such claim at once. It is also pure cache, so the cost is
// one round of re-reads rather than lost data.
func (s *Store) Rewind(ctx context.Context, chainID uint64, toBlock uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cursor, err := s.cursor(chainID)
	if err != nil {
		return err
	}

	// The hash of the new head comes from the digest of the block we are
	// rewinding to, when that block had logs. When it does not, the hash is
	// cleared rather than guessed: the caller re-reads the header after a
	// rewind anyway, and a wrong hash would defeat the boot-time canonicality
	// check it feeds.
	headHash := domain.Hash{}
	if d, ok, err := s.blockDigest(chainID, toBlock); err != nil {
		return err
	} else if ok {
		headHash = d.Hash
	}

	b := s.db.NewBatch()
	defer b.Close()

	// Per-block state above the ancestor.
	digests := chainPrefix(prefixDigest, chainID)
	if err := b.DeleteRange(digestKey(chainID, toBlock+1), prefixEnd(digests), nil); err != nil {
		return fmt.Errorf("rewind: drop digests: %w", err)
	}
	// The whole balance cache for this chain.
	balances := chainPrefix(prefixBalance, chainID)
	if err := b.DeleteRange(balances, prefixEnd(balances), nil); err != nil {
		return fmt.Errorf("rewind: flush balance cache: %w", err)
	}

	// Rewind never moves the cursor forward: a reorg can only invalidate state,
	// and advancing here would claim blocks that were never indexed.
	if cursor.HeadBlock > toBlock {
		cursor.HeadBlock = toBlock
		cursor.HeadHash = headHash
	}
	if cursor.SafeBlock > toBlock {
		cursor.SafeBlock = toBlock
	}
	if cursor.FinalizedBlock > toBlock {
		cursor.FinalizedBlock = toBlock
	}
	if err := b.Set(cursorKey(chainID), encodeCursor(cursor), nil); err != nil {
		return fmt.Errorf("rewind: write cursor: %w", err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("rewind: commit: %w", err)
	}
	return nil
}

// blockDigest reads one stored per-block digest.
func (s *Store) blockDigest(chainID, block uint64) (domain.BlockDigest, bool, error) {
	value, closer, err := s.db.Get(digestKey(chainID, block))
	if errors.Is(err, pebble.ErrNotFound) {
		return domain.BlockDigest{}, false, nil
	}
	if err != nil {
		return domain.BlockDigest{}, false, fmt.Errorf("read block digest %d: %w", block, err)
	}
	defer closer.Close()
	d, err := decodeDigest(block, value)
	if err != nil {
		return domain.BlockDigest{}, false, err
	}
	return d, true, nil
}

// AccountTokens reads one page of the index.
//
// This is the whole read path for discovery: a bounded prefix scan over one
// account, with zero RPC calls. Pages are in token-address order, which is
// stable by construction — the key order is the page order — so pagination
// needs no snapshot pinning and a page cursor stays valid across writes.
func (s *Store) AccountTokens(ctx context.Context, q ports.AccountTokensQuery) (ports.AccountTokensPage, error) {
	if err := ctx.Err(); err != nil {
		return ports.AccountTokensPage{}, err
	}
	if q.Standard != domain.StandardUnknown && !q.Standard.Valid() {
		return ports.AccountTokensPage{}, fmt.Errorf("account tokens: unsupported standard %q", q.Standard)
	}
	limit := q.Limit
	switch {
	case limit <= 0:
		limit = DefaultPageLimit
	case limit > MaxPageLimit:
		limit = MaxPageLimit
	}

	prefix := accountPrefix(q.ChainID, q.Account)
	lower := prefix
	if q.After != nil {
		lower = afterTokenKey(q.ChainID, q.Account, *q.After)
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return ports.AccountTokensPage{}, fmt.Errorf("account tokens: open iterator: %w", err)
	}
	defer iter.Close()

	var page ports.AccountTokensPage
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		token, ok := pairToken(key)
		if !ok {
			return ports.AccountTokensPage{}, fmt.Errorf("account tokens: pair key is %d bytes, want %d", len(key), pairKeyLen)
		}
		standard, _ := pairStandard(key)

		// A page boundary is expressible only as a token address, so a token's
		// rows are never split: once the page is full, the scan stops at the
		// first row of a *different* token, which is also the cursor. A token
		// held under both standards therefore makes a page at most one row
		// longer than the limit.
		if len(page.Pairs) >= limit && token != page.Pairs[len(page.Pairs)-1].Token {
			last := page.Pairs[len(page.Pairs)-1].Token
			page.Next = &last
			break
		}
		if q.Standard != domain.StandardUnknown && standard != q.Standard {
			continue
		}

		first, last, err := decodePairRange(iter.Value())
		if err != nil {
			return ports.AccountTokensPage{}, err
		}
		page.Pairs = append(page.Pairs, domain.AccountToken{
			Account:    q.Account,
			Token:      token,
			Standard:   standard,
			FirstBlock: first,
			LastBlock:  last,
		})
	}
	if err := iter.Error(); err != nil {
		return ports.AccountTokensPage{}, fmt.Errorf("account tokens: scan: %w", err)
	}
	return page, nil
}

// CachedBalances returns what the cache holds for these tokens.
//
// Entries the indexer has marked dirty are returned too, with DirtyAtBlock set.
// That is deliberate: a degraded read path can serve a stale value with an
// honest flag, which beats failing the request. A clean entry is not "fresh for
// N seconds" — it is provably still correct, because the only thing that can
// change a balance is a transfer, and a transfer stamps the pair.
func (s *Store) CachedBalances(ctx context.Context, chainID uint64, account domain.Address, tokens []domain.Address) (map[domain.Address]ports.CachedBalance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make(map[domain.Address]ports.CachedBalance, len(tokens))
	for _, token := range tokens {
		value, closer, err := s.db.Get(balanceKey(chainID, account, token))
		if errors.Is(err, pebble.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read cached balance: %w", err)
		}
		rec, err := decodeBalance(value)
		closer.Close()
		if err != nil {
			return nil, err
		}
		// A bare invalidation stamp is a pair that was touched but never read.
		// It holds no value to serve, so it is a cache miss, not a stale hit.
		if !rec.value {
			continue
		}
		out[token] = ports.CachedBalance{
			Balance: domain.Balance{
				Token: token,
				Raw:   rec.raw,
				Block: rec.block,
			},
			DirtyAtBlock: rec.dirtyAt,
		}
	}
	return out, nil
}

// PutBalances stores freshly read balances.
//
// Failed reads are skipped rather than cached: a revert or a timeout says
// nothing about the balance, and caching it would need its own expiry policy to
// ever recover. The write is a merge, not a set, so a concurrent invalidation
// at a higher block survives it — see balanceMerger.
//
// The commit is not synced. The balance cache is derived state: losing it to a
// machine crash costs one round of re-reads, which is not worth an fsync on a
// path that runs per request.
func (s *Store) PutBalances(ctx context.Context, chainID uint64, account domain.Address, balances []domain.Balance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()

	n := 0
	for _, bal := range balances {
		if !bal.OK() {
			continue
		}
		value, err := encodeBalance(bal.Raw, bal.Block, 0)
		if err != nil {
			return err
		}
		if err := b.Merge(balanceKey(chainID, account, bal.Token), value, nil); err != nil {
			return fmt.Errorf("put balances: merge: %w", err)
		}
		n++
	}
	if n == 0 {
		return nil
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("put balances: commit: %w", err)
	}
	return nil
}

// TokenMeta reads cached metadata. Absent tokens are simply missing from the
// result: "not resolved yet" is a normal state, not an error.
func (s *Store) TokenMeta(ctx context.Context, chainID uint64, tokens []domain.Address) (map[domain.Address]domain.TokenMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make(map[domain.Address]domain.TokenMeta, len(tokens))
	for _, token := range tokens {
		m, ok, err := s.tokenMeta(chainID, token)
		if err != nil {
			return nil, err
		}
		if ok {
			out[token] = m
		}
	}
	return out, nil
}

func (s *Store) tokenMeta(chainID uint64, token domain.Address) (domain.TokenMeta, bool, error) {
	value, closer, err := s.db.Get(metaKey(chainID, token))
	if errors.Is(err, pebble.ErrNotFound) {
		return domain.TokenMeta{}, false, nil
	}
	if err != nil {
		return domain.TokenMeta{}, false, fmt.Errorf("read token metadata: %w", err)
	}
	defer closer.Close()
	m, err := decodeMeta(token, value)
	if err != nil {
		return domain.TokenMeta{}, false, err
	}
	return m, true, nil
}

// PutTokenMeta stores resolved metadata and maintains TokenMeta.Version.
//
// Version is owned here rather than taken from the caller so the invariant has
// exactly one implementation: it increments when the displayed fields actually
// change, and not otherwise. The API folds it into the ETag, so a symbol that
// resolves from null correctly invalidates a client's cached page while a
// re-resolution to the same value does not.
//
// Metadata is written once per token and read on every page, so a
// read-modify-write here is the right trade — unlike the pair and balance
// paths, which are merge-only for exactly the opposite reason.
func (s *Store) PutTokenMeta(ctx context.Context, chainID uint64, meta []domain.TokenMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()

	n := 0
	for _, m := range meta {
		existing, ok, err := s.tokenMeta(chainID, m.Token)
		if err != nil {
			return err
		}
		switch {
		case !ok:
			m.Version = 1
		case sameDisplay(existing, m):
			m.Version = existing.Version
			continue // nothing changed; leave the record and its version alone
		default:
			m.Version = existing.Version + 1
		}
		if err := b.Set(metaKey(chainID, m.Token), encodeMeta(m), nil); err != nil {
			return fmt.Errorf("put token metadata: %w", err)
		}
		n++
	}
	if n == 0 {
		return nil
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("put token metadata: commit: %w", err)
	}
	return nil
}

// sameDisplay compares the fields that are actually displayed, so Version
// tracks what a client would see rather than that a write happened.
func sameDisplay(a, b domain.TokenMeta) bool {
	return a.Standard == b.Standard &&
		a.Decimals == b.Decimals &&
		a.Resolved == b.Resolved &&
		a.Symbol == truncate(b.Symbol) &&
		a.Name == truncate(b.Name)
}

// Checkpoint writes a consistent snapshot of the store to dir.
//
// This is the scale-out story from docs/architecture.md made cheap: Pebble
// hardlinks the sstables, so snapshotting a multi-GB store costs no extra disk
// up front and completes in milliseconds. A new replica starts from a
// checkpoint instead of paying for its own backfill, which is the difference
// between adding an instance in seconds and adding one in half an hour of paid
// RPC calls.
//
// dir must not already exist. The snapshot is taken at the live committed
// state, so it is internally consistent — the cursor it contains describes
// exactly the pairs it contains, which is the same invariant ApplyBatch
// maintains.
func (s *Store) Checkpoint(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.Checkpoint(dir, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("checkpoint to %q: %w", dir, err)
	}
	return nil
}

// DiskUsage reports the store's on-disk size, for the operational logging that
// backs the "low single-digit GB" claim in docs/measurements.md.
func (s *Store) DiskUsage() uint64 { return s.db.Metrics().DiskSpaceUsage() }

// pebbleLogger adapts slog to Pebble's logger.
//
// Pebble's info output is compaction bookkeeping — useful when diagnosing write
// stalls, noise otherwise — so it is logged at debug level rather than info.
type pebbleLogger struct{ log *slog.Logger }

func (l pebbleLogger) Infof(format string, args ...any) {
	l.log.Debug("pebble: " + fmt.Sprintf(format, args...))
}

func (l pebbleLogger) Errorf(format string, args ...any) {
	l.log.Error("pebble: " + fmt.Sprintf(format, args...))
}

// Fatalf must not return; Pebble calls it only for conditions it considers
// unrecoverable, and a panic keeps the stack rather than exiting silently.
func (l pebbleLogger) Fatalf(format string, args ...any) {
	msg := "pebble: " + fmt.Sprintf(format, args...)
	l.log.Error(msg)
	panic(msg)
}
