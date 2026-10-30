// Package indexer keeps the store in step with the chain.
//
// It has two modes, and the split is the whole design:
//
//   - Backfill is a sequential sweep from the chain's start block to the tip.
//     Sequential rather than parallel because that was measured: throughput is
//     flat at ~200–280 blocks/s across a 16× range of concurrency, so the
//     ordered-commit machinery a parallel backfill needs would buy ~15% (see
//     docs/measurements.md). It runs once, costs ~1.8k requests, and is
//     resumable from the cursor.
//
//   - Follow tracks the tip block by block, comparing each new block's parent
//     hash against the one it already indexed. That comparison is in memory and
//     free, which is what makes reorg detection cheap enough to do on every
//     block rather than on a timer.
//
// Both modes write through ports.Store's atomic batch, so the cursor and the
// pairs it describes can never disagree — a crash costs the current range, not
// the index.
//
// Unreliable RPC is handled in layers rather than in one place: internal/rpc
// retries a call, this package retries a *range* with its own backoff, and a
// failure that outlives both does not kill the follower — it degrades
// readiness, so a load balancer stops sending this instance traffic while it
// keeps trying.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/obs"
	"tokendiscovery/internal/rpc"
)

// Chain is the slice of ports.ChainAdapter the indexer needs. Declaring it here
// rather than depending on the whole port means the indexer can be built and
// tested against a chain that cannot yet read balances — which is exactly the
// state the EVM adapter is in until phase 8.
type Chain interface {
	Chain() domain.ChainInfo
	Watermarks(ctx context.Context) (domain.Watermarks, error)
	Header(ctx context.Context, number uint64) (domain.BlockRef, error)
	ScanAccountTokens(ctx context.Context, from, to uint64) (domain.AccountTokenBatch, error)
}

// Store is the slice of ports.Store the indexer writes through.
type Store interface {
	Cursor(ctx context.Context, chainID uint64) (domain.Cursor, error)
	ApplyBatch(ctx context.Context, chainID uint64, batch domain.AccountTokenBatch, cursor domain.Cursor) error
	Rewind(ctx context.Context, chainID uint64, toBlock uint64) error
	BlockHash(ctx context.Context, chainID uint64, block uint64) (domain.Hash, bool, error)
}

var (
	// ErrNotSeeded means the store has never completed a backfill, so the
	// follower has no trustworthy position to continue from.
	ErrNotSeeded = errors.New("indexer: store has never been seeded; run backfill first")

	// ErrParserMismatch means the store was indexed by a different decoder.
	ErrParserMismatch = errors.New("indexer: store was indexed by a different parser version")

	// ErrDeepReorg means a reorg was detected that is deeper than the header
	// ring, so the common ancestor cannot be established.
	//
	// This is fatal on purpose. Ethereum finalises in roughly two epochs, so a
	// reorg deeper than the ring is not a reorg — it is a broken or lying
	// provider, and continuing would silently index a branch we never verified.
	ErrDeepReorg = errors.New("indexer: reorg is deeper than the header ring")
)

// Options configures the indexer.
type Options struct {
	// LogRangeBlocks is how many blocks one scan covers during backfill and
	// bulk catch-up. Measured default: 200 (~73 MB decoded, ~4 MB gzipped).
	LogRangeBlocks uint64

	// TipPollInterval is how often the head is polled while following.
	TipPollInterval time.Duration

	// HeaderRingSize bounds how deep a reorg can be resolved.
	HeaderRingSize int

	// BulkCatchUpBlocks is how far behind the follower must fall before it
	// switches from per-block tracking to ranged scans. Below the safe
	// watermark there is nothing to reorg, so the parent-hash walk is wasted
	// effort there.
	BulkCatchUpBlocks uint64

	// ParserVersion is the decoder's version, stored in the cursor and checked
	// against it. A mismatch refuses the store rather than mixing two
	// decoders' output.
	ParserVersion uint32

	// ReadyLagBlocks is how far behind the tip this instance may be and still
	// take traffic.
	ReadyLagBlocks uint64

	// RangeRetries bounds how many times one range is retried before the
	// backfill gives up. The backfill is resumable, so giving up is cheap.
	RangeRetries int

	// RetryBaseDelay and RetryMaxDelay bound the indexer's own backoff, which
	// sits on top of the RPC client's per-call retries.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	// ProgressInterval is how often backfill progress is logged.
	ProgressInterval time.Duration

	Logger  *slog.Logger
	Metrics *obs.Metrics

	// sleep and now are injectable so tests exercise backoff and staleness
	// without real time passing.
	sleep func(context.Context, time.Duration) error
	now   func() time.Time
}

func (o Options) withDefaults() Options {
	if o.LogRangeBlocks == 0 {
		o.LogRangeBlocks = 200
	}
	if o.TipPollInterval <= 0 {
		o.TipPollInterval = 4 * time.Second
	}
	if o.HeaderRingSize < 2 {
		o.HeaderRingSize = 128
	}
	if o.BulkCatchUpBlocks == 0 {
		o.BulkCatchUpBlocks = 64
	}
	if o.ReadyLagBlocks == 0 {
		o.ReadyLagBlocks = 8
	}
	if o.RangeRetries <= 0 {
		o.RangeRetries = 8
	}
	if o.RetryBaseDelay <= 0 {
		o.RetryBaseDelay = time.Second
	}
	if o.RetryMaxDelay <= 0 {
		o.RetryMaxDelay = 30 * time.Second
	}
	if o.ProgressInterval <= 0 {
		o.ProgressInterval = 15 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.sleep == nil {
		o.sleep = sleepCtx
	}
	if o.now == nil {
		o.now = time.Now
	}
	return o
}

// Indexer keeps one chain's index in step with the chain.
//
// One Indexer runs one mode at a time: Backfill then Follow, which is how the
// CLI drives it. They share the working scan range and the header window, so
// running both at once on the same value is a programming error rather than a
// supported way to go faster — and it would not go faster anyway, since
// concurrency buys nothing against a measured endpoint.
type Indexer struct {
	chain Chain
	store Store
	opts  Options
	log   *slog.Logger
	m     *obs.Metrics

	info domain.ChainInfo
	ring *headerRing

	// rangeBlocks is the working scan range. It only ever shrinks: see
	// shrinkRange.
	rangeBlocks uint64

	mu     sync.Mutex
	status Status
	cursor domain.Cursor
}

// New builds an indexer.
func New(chain Chain, store Store, opts Options) (*Indexer, error) {
	if chain == nil {
		return nil, errors.New("indexer: a Chain is required")
	}
	if store == nil {
		return nil, errors.New("indexer: a Store is required")
	}
	opts = opts.withDefaults()
	info := chain.Chain()
	if info.ChainID == 0 {
		return nil, errors.New("indexer: chain id must not be zero")
	}
	if info.StartBlock == 0 {
		return nil, errors.New("indexer: chain start block must not be zero")
	}
	return &Indexer{
		chain:       chain,
		store:       store,
		opts:        opts,
		log:         opts.Logger.With("chain", info.Slug, "chain_id", info.ChainID),
		m:           opts.Metrics,
		info:        info,
		ring:        newHeaderRing(opts.HeaderRingSize),
		rangeBlocks: opts.LogRangeBlocks,
	}, nil
}

// Status is a snapshot of where the indexer is, for readiness and for the
// /status endpoint the API grows in phase 7.
type Status struct {
	Seeded      bool
	Backfilling bool

	// Following reports that at least one tip poll has succeeded, so HeadBlock
	// and LatestBlock mean something.
	Following bool

	HeadBlock   uint64
	HeadTime    time.Time
	LatestBlock uint64
	SafeBlock   uint64

	LastTickAt  time.Time
	LastError   string
	LastErrorAt time.Time

	Reorgs uint64
}

// LagBlocks is how far the indexed head trails the chain tip.
func (s Status) LagBlocks() uint64 {
	if s.LatestBlock <= s.HeadBlock {
		return 0
	}
	return s.LatestBlock - s.HeadBlock
}

// Status returns a snapshot.
func (ix *Indexer) Status() Status {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.status
}

// Ready implements api.Readiness.
//
// Readiness is deliberately strict and separate from liveness: an instance that
// is up but behind would answer with a stale head, and a stale *empty* token
// list is the one failure mode that genuinely misleads a user. So it reports
// not-ready — and keeps working — rather than serving.
func (ix *Indexer) Ready() error {
	s := ix.Status()
	switch {
	case !s.Seeded:
		return errors.New("store has not been seeded; backfill has not completed")
	case s.Backfilling:
		return errors.New("backfill in progress")
	case !s.Following:
		return errors.New("waiting for the first successful tip poll")
	}
	// A dead provider is invisible in the lag figure — the head and the last
	// known tip both stop moving together — so staleness is judged on the age
	// of the last successful poll as well.
	if stale := ix.opts.TipPollInterval * 5; !s.LastTickAt.IsZero() && ix.opts.now().Sub(s.LastTickAt) > stale {
		return fmt.Errorf("no successful tip poll for %s (last error: %s)",
			ix.opts.now().Sub(s.LastTickAt).Round(time.Second), s.LastError)
	}
	if lag := s.LagBlocks(); lag > ix.opts.ReadyLagBlocks {
		return fmt.Errorf("indexed head %d is %d blocks behind the tip %d", s.HeadBlock, lag, s.LatestBlock)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Backfill
// ---------------------------------------------------------------------------

// Backfill sweeps from the chain's start block to the current tip.
//
// It resumes from the cursor, so an interrupted run costs only the range it was
// in: the add-only pair set and the min/max merge in the store make re-applying
// an overlapping range a no-op, which is why resume needs no reconciliation
// step.
//
// SeededFromBlock is written only once the sweep reaches the tip. Until then the
// store reads as unseeded, which is what stops `serve` from answering out of a
// half-indexed store.
func (ix *Indexer) Backfill(ctx context.Context) error {
	cursor, err := ix.store.Cursor(ctx, ix.info.ChainID)
	if err != nil {
		return err
	}
	if err := ix.checkParser(cursor); err != nil {
		return err
	}

	from := ix.info.StartBlock
	if cursor.HeadBlock >= from {
		from = cursor.HeadBlock + 1
		ix.log.Info("resuming backfill from the stored cursor",
			"head_block", cursor.HeadBlock, "from_block", from, "seeded", cursor.Seeded())
	}
	cursor.ParserVersion = ix.opts.ParserVersion

	wm, err := ix.chain.Watermarks(ctx)
	if err != nil {
		return fmt.Errorf("backfill: read watermarks: %w", err)
	}
	ix.setStatus(func(s *Status) {
		s.Backfilling = true
		s.Seeded = cursor.Seeded()
		s.LatestBlock = wm.Latest.Number
		s.SafeBlock = wm.Safe.Number
	})
	defer ix.setStatus(func(s *Status) { s.Backfilling = false })

	target := wm.Latest.Number
	ix.log.Info("backfill starting",
		"from_block", from, "target_block", target,
		"blocks", blocksBetween(from, target), "range_blocks", ix.rangeBlocks)

	start := ix.opts.now()
	lastLog := start
	var indexed, pairs, logs uint64
	retargets := 0

	for {
		for from <= target {
			if err := ctx.Err(); err != nil {
				return err
			}

			batch, end, err := ix.scan(ctx, from, target)
			if errors.Is(err, rpc.ErrBeyondHead) {
				// The node's head moved backwards, which happens behind a load
				// balancer whose members are not in lockstep. Re-read it and
				// retarget rather than failing a 25-minute sweep.
				retargets++
				if retargets > 10 {
					return fmt.Errorf("backfill: head kept moving backwards: %w", err)
				}
				wm, err = ix.chain.Watermarks(ctx)
				if err != nil {
					return fmt.Errorf("backfill: re-read watermarks: %w", err)
				}
				ix.log.Warn("provider head moved backwards; retargeting",
					"from_block", from, "new_target", wm.Latest.Number)
				target = wm.Latest.Number
				continue
			}
			if err != nil {
				return fmt.Errorf("backfill: scan %d..: %w", from, err)
			}

			cursor.HeadBlock = end
			cursor.HeadHash = batchHeadHash(batch, end)
			cursor.SafeBlock = wm.Safe.Number
			cursor.FinalizedBlock = wm.Finalized.Number
			if err := ix.store.ApplyBatch(ctx, ix.info.ChainID, batch, cursor); err != nil {
				return fmt.Errorf("backfill: apply %d..%d: %w", batch.FromBlock, batch.ToBlock, err)
			}

			indexed += end - from + 1
			pairs += uint64(len(batch.Pairs))
			logs += uint64(batch.LogCount)
			ix.recordBatch(batch, end-from+1)
			ix.setStatus(func(s *Status) { s.HeadBlock = end })
			from = end + 1

			if now := ix.opts.now(); now.Sub(lastLog) >= ix.opts.ProgressInterval {
				lastLog = now
				elapsed := now.Sub(start)
				ix.log.Info("backfill progress",
					"head_block", end,
					"target_block", target,
					"remaining_blocks", blocksBetween(from, target),
					"blocks_per_second", rate(indexed, elapsed),
					"pairs", pairs,
					"logs", logs,
					"eta", eta(blocksBetween(from, target), indexed, elapsed),
				)
			}
		}

		// The chain moved while we were sweeping. One more lap closes the gap;
		// since indexing runs ~20× faster than blocks are produced, this
		// converges immediately rather than chasing the tip forever.
		wm, err = ix.chain.Watermarks(ctx)
		if err != nil {
			return fmt.Errorf("backfill: re-read watermarks: %w", err)
		}
		if wm.Latest.Number <= target {
			break
		}
		ix.log.Info("chain advanced during backfill; continuing",
			"previous_target", target, "new_target", wm.Latest.Number)
		target = wm.Latest.Number
	}

	// Pin the head's hash with one header read so the follower can verify, at
	// boot, that the block the cursor names is still canonical. The digest hash
	// would do for blocks that had logs, but a single call removes the "unless
	// that block was empty" caveat entirely.
	head, err := ix.chain.Header(ctx, target)
	if err != nil {
		return fmt.Errorf("backfill: read head header %d: %w", target, err)
	}
	cursor.HeadBlock = head.Number
	cursor.HeadHash = head.Hash
	if cursor.SeededFromBlock == 0 {
		// Only ever written once. Overwriting it on a later run would claim
		// coverage from a block that run never swept — a re-run with a
		// different start block resumes from the cursor, it does not extend
		// the index backwards.
		cursor.SeededFromBlock = ix.info.StartBlock
	}
	empty := domain.AccountTokenBatch{FromBlock: head.Number, ToBlock: head.Number}
	if err := ix.store.ApplyBatch(ctx, ix.info.ChainID, empty, cursor); err != nil {
		return fmt.Errorf("backfill: mark seeded: %w", err)
	}

	ix.mu.Lock()
	ix.cursor = cursor
	ix.mu.Unlock()
	ix.ring.reset(head)
	ix.setStatus(func(s *Status) {
		s.Seeded = true
		s.HeadBlock = head.Number
		s.HeadTime = head.Time
		s.LatestBlock = head.Number
	})

	elapsed := ix.opts.now().Sub(start)
	ix.log.Info("backfill complete",
		"head_block", head.Number,
		"blocks_indexed", indexed,
		"logs", logs,
		"pairs", pairs,
		"took", elapsed.Round(time.Second),
		"blocks_per_second", rate(indexed, elapsed))
	return nil
}

// scan fetches one range, adapting to provider pushback.
//
// maxTo bounds the range; the returned block is where the scan actually
// stopped, which is lower when the range had to be made smaller.
func (ix *Indexer) scan(ctx context.Context, from, maxTo uint64) (domain.AccountTokenBatch, uint64, error) {
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return domain.AccountTokenBatch{}, 0, err
		}
		end := from + ix.rangeBlocks - 1
		if end > maxTo || end < from { // end < from catches the overflow
			end = maxTo
		}

		batch, err := ix.chain.ScanAccountTokens(ctx, from, end)
		if err == nil {
			return batch, end, nil
		}
		switch {
		case ctx.Err() != nil:
			return domain.AccountTokenBatch{}, 0, errors.Join(err, ctx.Err())

		case rpc.SplitRange(err):
			// Making the request smaller is progress, not a retry, so it does
			// not consume an attempt.
			if end == from {
				return domain.AccountTokenBatch{}, 0, fmt.Errorf("block %d alone exceeds the provider's limit: %w", from, err)
			}
			ix.shrinkRange(end - from + 1)

		case errors.Is(err, rpc.ErrBeyondHead):
			// The caller retargets; it knows what it was sweeping towards.
			return domain.AccountTokenBatch{}, 0, err

		case rpc.Retryable(err):
			attempt++
			if attempt >= ix.opts.RangeRetries {
				return domain.AccountTokenBatch{}, 0, fmt.Errorf("gave up after %d attempts: %w", attempt, err)
			}
			delay := backoff(ix.opts.RetryBaseDelay, ix.opts.RetryMaxDelay, attempt-1)
			ix.log.Warn("scan failed; retrying",
				"from", from, "to", end, "attempt", attempt, "delay", delay, "cause", err)
			if err := ix.opts.sleep(ctx, delay); err != nil {
				return domain.AccountTokenBatch{}, 0, err
			}

		default:
			return domain.AccountTokenBatch{}, 0, err
		}
	}
}

// shrinkRange halves the working scan range after the provider rejected one as
// too large.
//
// The reduction is sticky for the life of the process. The configured range is
// a measured default; a provider that rejects it is telling us the measurement
// does not hold here, so creeping back up would just rediscover the same limit
// every few ranges and waste a paid request each time.
func (ix *Indexer) shrinkRange(attempted uint64) {
	next := attempted / 2
	if next < 1 {
		next = 1
	}
	if next >= ix.rangeBlocks {
		next = ix.rangeBlocks / 2
		if next < 1 {
			next = 1
		}
	}
	ix.log.Warn("provider rejected the scan range; halving for the rest of this run",
		"attempted_blocks", attempted, "range_blocks", next)
	ix.rangeBlocks = next
}

// ---------------------------------------------------------------------------
// Follow
// ---------------------------------------------------------------------------

// Follow tracks the chain tip until the context is cancelled.
//
// Transient failures do not stop it: they are recorded and retried on the next
// tick, and readiness degrades in the meantime so that a load balancer takes
// this instance out of rotation rather than being served stale answers. The one
// fatal case is a reorg deeper than the header ring, where continuing would mean
// indexing a branch that was never verified.
func (ix *Indexer) Follow(ctx context.Context) error {
	cursor, err := ix.store.Cursor(ctx, ix.info.ChainID)
	if err != nil {
		return err
	}
	if !cursor.Seeded() {
		return ErrNotSeeded
	}
	if err := ix.checkParser(cursor); err != nil {
		return err
	}

	head, err := ix.verifyHead(ctx, cursor)
	if err != nil {
		return err
	}
	cursor.HeadBlock, cursor.HeadHash = head.Number, head.Hash
	// verifyHead may have rewound, in which case the stored watermarks were
	// clamped too; keep the in-memory copy in step rather than relying on the
	// next tick to overwrite them.
	if cursor.SafeBlock > head.Number {
		cursor.SafeBlock = head.Number
	}
	if cursor.FinalizedBlock > head.Number {
		cursor.FinalizedBlock = head.Number
	}

	ix.mu.Lock()
	ix.cursor = cursor
	ix.mu.Unlock()
	ix.ring.reset(head)
	ix.setStatus(func(s *Status) {
		s.Seeded = true
		s.HeadBlock = head.Number
		s.HeadTime = head.Time
	})
	ix.log.Info("following the chain tip",
		"head_block", head.Number, "poll_interval", ix.opts.TipPollInterval)

	ticker := time.NewTicker(ix.opts.TipPollInterval)
	defer ticker.Stop()

	for {
		if err := ix.tick(ctx); err != nil {
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				return nil
			case errors.Is(err, ErrDeepReorg):
				return err
			}
			ix.noteError(err)
			ix.log.Warn("tip poll failed; retrying on the next tick", "cause", err)
		}
		select {
		case <-ctx.Done():
			ix.log.Info("stopping the tip follower", "head_block", ix.Status().HeadBlock)
			return nil
		case <-ticker.C:
		}
	}
}

// tick advances the index as far as the current tip allows.
func (ix *Indexer) tick(ctx context.Context) error {
	wm, err := ix.chain.Watermarks(ctx)
	if err != nil {
		return fmt.Errorf("read watermarks: %w", err)
	}
	ix.recordWatermarks(wm)

	head, ok := ix.ring.tip()
	if !ok {
		// Follow seeds the ring before ticking, so an empty one means a bug.
		// Indexing from an unknown head would sweep from block 1.
		return errors.New("header ring is empty; refusing to index from an unknown head")
	}
	ix.setStatus(func(s *Status) {
		s.Following = true
		s.LastTickAt = ix.opts.now()
		s.LatestBlock = wm.Latest.Number
		s.SafeBlock = wm.Safe.Number
	})

	// A reorg at the head itself leaves the height unchanged, so the hash has
	// to be compared even when nothing appears to have moved.
	if wm.Latest.Number == head.Number && wm.Latest.Hash != head.Hash && !head.Hash.IsZero() {
		return ix.handleReorg(ctx, wm.Latest)
	}
	if wm.Latest.Number <= head.Number {
		return nil
	}

	// Bulk catch-up. Everything at or below the safe watermark cannot be
	// reorged, so there is nothing for the per-block parent-hash walk to
	// protect and ranged scans are ~200× fewer requests.
	for wm.Safe.Number > head.Number+ix.opts.BulkCatchUpBlocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ix.catchUp(ctx, head.Number+1, wm.Safe.Number, wm); err != nil {
			return err
		}
		head, _ = ix.ring.tip()
	}

	for next := head.Number + 1; next <= wm.Latest.Number; next++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// The watermark poll already carries the head's header, so the common
		// case — one new block per tick — costs no extra request.
		hdr := wm.Latest
		if next != wm.Latest.Number {
			hdr, err = ix.chain.Header(ctx, next)
			if err != nil {
				return fmt.Errorf("read header %d: %w", next, err)
			}
		}
		if tip, ok := ix.ring.tip(); ok && !tip.Hash.IsZero() && hdr.ParentHash != tip.Hash {
			return ix.handleReorg(ctx, hdr)
		}
		if err := ix.indexBlock(ctx, hdr, wm); err != nil {
			return err
		}
	}
	return nil
}

// catchUp indexes a range in bulk, without the per-block header walk.
func (ix *Indexer) catchUp(ctx context.Context, from, to uint64, wm domain.Watermarks) error {
	batch, end, err := ix.scan(ctx, from, to)
	if err != nil {
		return fmt.Errorf("catch-up scan from %d: %w", from, err)
	}
	cursor := ix.nextCursor(end, batchHeadHash(batch, end), wm)
	if err := ix.store.ApplyBatch(ctx, ix.info.ChainID, batch, cursor); err != nil {
		return fmt.Errorf("catch-up apply %d..%d: %w", batch.FromBlock, batch.ToBlock, err)
	}
	ix.commit(cursor, batch, domain.BlockRef{Number: end, Hash: cursor.HeadHash}, end-from+1)
	ix.log.Info("bulk catch-up", "head_block", end, "safe_block", wm.Safe.Number,
		"behind_blocks", blocksBetween(end+1, wm.Latest.Number))
	return nil
}

// indexBlock scans and applies exactly one block.
func (ix *Indexer) indexBlock(ctx context.Context, hdr domain.BlockRef, wm domain.Watermarks) error {
	batch, _, err := ix.scan(ctx, hdr.Number, hdr.Number)
	if err != nil {
		return fmt.Errorf("scan block %d: %w", hdr.Number, err)
	}

	// eth_getLogs carries no `removed` flag, so a provider can hand back logs
	// from a branch that is being replaced. The block hash in the logs is
	// checked against the header we walked, and a mismatch aborts the tick
	// rather than being indexed: the next tick sees the new header and resolves
	// it as the reorg it is.
	if bad := nonCanonicalLogs(batch, hdr.Hash); bad > 0 {
		if ix.m != nil {
			ix.m.NonCanonicalLogs.Add(float64(bad))
		}
		return fmt.Errorf("block %d: %d logs carry a non-canonical block hash", hdr.Number, bad)
	}

	cursor := ix.nextCursor(hdr.Number, hdr.Hash, wm)
	if err := ix.store.ApplyBatch(ctx, ix.info.ChainID, batch, cursor); err != nil {
		return fmt.Errorf("apply block %d: %w", hdr.Number, err)
	}
	ix.commit(cursor, batch, hdr, 1)
	ix.log.Debug("indexed block",
		"block", hdr.Number, "logs", batch.LogCount, "pairs", len(batch.Pairs))
	return nil
}

// handleReorg walks back to the common ancestor, rewinds the store to it, and
// leaves the next tick to re-index the canonical branch.
func (ix *Indexer) handleReorg(ctx context.Context, hdr domain.BlockRef) error {
	ancestor, err := ix.findAncestor(ctx, hdr)
	if err != nil {
		return err
	}
	tip, _ := ix.ring.tip()
	depth := blocksBetween(ancestor.Number+1, tip.Number)

	ix.log.Warn("reorg detected",
		"at_block", hdr.Number, "indexed_head", tip.Number,
		"common_ancestor", ancestor.Number, "depth_blocks", depth)

	if err := ix.store.Rewind(ctx, ix.info.ChainID, ancestor.Number); err != nil {
		return fmt.Errorf("rewind to %d: %w", ancestor.Number, err)
	}
	ix.ring.reset(ancestor)

	ix.mu.Lock()
	ix.cursor.HeadBlock, ix.cursor.HeadHash = ancestor.Number, ancestor.Hash
	if ix.cursor.SafeBlock > ancestor.Number {
		ix.cursor.SafeBlock = ancestor.Number
	}
	if ix.cursor.FinalizedBlock > ancestor.Number {
		ix.cursor.FinalizedBlock = ancestor.Number
	}
	ix.mu.Unlock()

	ix.setStatus(func(s *Status) {
		s.HeadBlock = ancestor.Number
		s.HeadTime = ancestor.Time
		s.Reorgs++
	})
	if ix.m != nil {
		ix.m.Reorgs.Inc()
		ix.m.ReorgDepth.Observe(float64(depth))
		ix.m.BalanceCacheFlushes.Inc() // Rewind flushes the cache; count it where it happens
	}
	return nil
}

// findAncestor walks back from a diverged block until the canonical chain and
// the hashes we recorded agree.
//
// The oracle is two-tiered: the in-memory header ring answers for free, and the
// store's per-block digests answer for everything older. That second tier is
// what makes the walk work right after a restart, when the ring is empty but a
// reorg may have happened while the service was down.
//
// Each step costs one header read, so the common one-block reorg resolves in
// one request. The walk is bounded, and a divergence older than the bound is
// reported rather than guessed at: Ethereum finalises in roughly two epochs, so
// a reorg that deep is not a reorg but a broken or lying provider, and indexing
// a branch we never verified would be worse than stopping.
func (ix *Indexer) findAncestor(ctx context.Context, diverged domain.BlockRef) (domain.BlockRef, error) {
	// The canonical parent of the diverged block is the first candidate, so a
	// one-block reorg needs no extra request at all.
	parentHash := diverged.ParentHash

	for depth := uint64(0); depth < uint64(ix.opts.HeaderRingSize); depth++ {
		if err := ctx.Err(); err != nil {
			return domain.BlockRef{}, err
		}
		if diverged.Number < depth+1 {
			break
		}
		candidate := diverged.Number - 1 - depth
		if candidate < ix.info.StartBlock {
			break
		}

		known, ok, err := ix.knownHash(ctx, candidate)
		if err != nil {
			return domain.BlockRef{}, err
		}
		if ok && known == parentHash {
			return ix.ancestorRef(candidate, known), nil
		}

		canonical, err := ix.chain.Header(ctx, candidate)
		if err != nil {
			return domain.BlockRef{}, fmt.Errorf("walk back to %d: %w", candidate, err)
		}
		if ok && known == canonical.Hash {
			return canonical, nil
		}
		// No record for this block (it carried no logs, or predates the ring
		// and the digests): keep walking rather than treating a gap as a
		// divergence.
		parentHash = canonical.ParentHash
	}

	return domain.BlockRef{}, fmt.Errorf("%w: diverged at %d, walked back %d blocks without agreement",
		ErrDeepReorg, diverged.Number, ix.opts.HeaderRingSize)
}

// knownHash returns the hash this service recorded for a block: the in-memory
// ring first, because it is free, then the store's digest.
func (ix *Indexer) knownHash(ctx context.Context, number uint64) (domain.Hash, bool, error) {
	if ref, ok := ix.ring.at(number); ok && !ref.Hash.IsZero() {
		return ref.Hash, true, nil
	}
	return ix.store.BlockHash(ctx, ix.info.ChainID, number)
}

// ancestorRef prefers the remembered header, which carries a timestamp, over a
// bare number and hash from the store.
func (ix *Indexer) ancestorRef(number uint64, hash domain.Hash) domain.BlockRef {
	if ref, ok := ix.ring.at(number); ok && ref.Hash == hash {
		return ref
	}
	return domain.BlockRef{Number: number, Hash: hash}
}

// verifyHead checks at boot that the block the cursor names is still canonical,
// which catches a reorg that happened while the service was down.
func (ix *Indexer) verifyHead(ctx context.Context, cursor domain.Cursor) (domain.BlockRef, error) {
	head, err := ix.chain.Header(ctx, cursor.HeadBlock)
	if err != nil {
		return domain.BlockRef{}, fmt.Errorf("verify stored head %d: %w", cursor.HeadBlock, err)
	}
	if cursor.HeadHash.IsZero() {
		// A backfill that stopped on a block with no logs leaves no hash to
		// check. The pair set is add-only, so trusting the height and letting
		// the follower's parent-hash walk take over from here is safe.
		ix.log.Info("stored head carries no hash; continuing from its height",
			"head_block", cursor.HeadBlock)
		return head, nil
	}
	if head.Hash == cursor.HeadHash {
		return head, nil
	}

	// The stored head is not on the canonical chain any more. The store's
	// per-block digests make the exact ancestor findable even though nothing
	// is in memory yet, so this recovers precisely rather than rewinding a
	// guessed distance.
	ix.log.Warn("stored head is no longer canonical; a reorg straddled the restart",
		"head_block", cursor.HeadBlock, "stored_hash", cursor.HeadHash, "canonical_hash", head.Hash)

	ancestor, err := ix.findAncestor(ctx, head)
	if err != nil {
		return domain.BlockRef{}, err
	}
	if err := ix.store.Rewind(ctx, ix.info.ChainID, ancestor.Number); err != nil {
		return domain.BlockRef{}, fmt.Errorf("rewind to %d: %w", ancestor.Number, err)
	}
	if ix.m != nil {
		ix.m.Reorgs.Inc()
		ix.m.ReorgDepth.Observe(float64(blocksBetween(ancestor.Number+1, cursor.HeadBlock)))
		ix.m.BalanceCacheFlushes.Inc()
	}
	ix.setStatus(func(s *Status) { s.Reorgs++ })
	ix.log.Warn("rewound to a canonical ancestor",
		"block", ancestor.Number, "depth_blocks", blocksBetween(ancestor.Number+1, cursor.HeadBlock))
	return ancestor, nil
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// nextCursor builds the cursor to commit alongside a batch.
func (ix *Indexer) nextCursor(head uint64, hash domain.Hash, wm domain.Watermarks) domain.Cursor {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	c := ix.cursor
	c.HeadBlock = head
	c.HeadHash = hash
	c.SafeBlock = wm.Safe.Number
	c.FinalizedBlock = wm.Finalized.Number
	c.ParserVersion = ix.opts.ParserVersion
	return c
}

// commit records a successful write: the cursor, the header window and the
// metrics all move together, so a half-applied advance cannot be observed.
func (ix *Indexer) commit(cursor domain.Cursor, batch domain.AccountTokenBatch, head domain.BlockRef, blocks uint64) {
	ix.mu.Lock()
	ix.cursor = cursor
	ix.mu.Unlock()

	ix.ring.push(head)
	ix.recordBatch(batch, blocks)
	ix.setStatus(func(s *Status) {
		s.HeadBlock = head.Number
		if !head.Time.IsZero() {
			s.HeadTime = head.Time
		}
		s.LastError = ""
	})
}

func (ix *Indexer) checkParser(cursor domain.Cursor) error {
	if cursor.ParserVersion == 0 || cursor.ParserVersion == ix.opts.ParserVersion {
		return nil
	}
	return fmt.Errorf("%w: store is v%d, this build is v%d",
		ErrParserMismatch, cursor.ParserVersion, ix.opts.ParserVersion)
}

func (ix *Indexer) setStatus(f func(*Status)) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	f(&ix.status)
}

func (ix *Indexer) noteError(err error) {
	ix.setStatus(func(s *Status) {
		s.LastError = err.Error()
		s.LastErrorAt = ix.opts.now()
	})
}

func (ix *Indexer) recordBatch(batch domain.AccountTokenBatch, blocks uint64) {
	if ix.m == nil {
		return
	}
	ix.m.BlocksIndexed.Add(float64(blocks))
	ix.m.PairsWritten.Add(float64(len(batch.Pairs)))
	ix.m.HeadBlock.Set(float64(batch.ToBlock))
}

func (ix *Indexer) recordWatermarks(wm domain.Watermarks) {
	if ix.m == nil {
		return
	}
	ix.m.SafeBlock.Set(float64(wm.Safe.Number))
	ix.m.FinalizedBlock.Set(float64(wm.Finalized.Number))
	ix.m.FinalityLagBlocks.Set(float64(wm.FinalityLag()))
	if head, ok := ix.ring.tip(); ok && !head.Time.IsZero() {
		ix.m.LagSeconds.Set(ix.opts.now().Sub(head.Time).Seconds())
	}
}

// nonCanonicalLogs counts the logs in a batch that claim a different block hash
// than the header says, which is how a branch being replaced shows up in an
// eth_getLogs response that carries no `removed` flag.
func nonCanonicalLogs(batch domain.AccountTokenBatch, want domain.Hash) int {
	bad := 0
	for _, b := range batch.Blocks {
		if b.Hash != want {
			bad += b.LogCount
		}
	}
	return bad
}

// batchHeadHash is the hash the batch reports for its last block, or zero when
// that block carried no logs and so has no digest.
func batchHeadHash(batch domain.AccountTokenBatch, end uint64) domain.Hash {
	for _, b := range batch.Blocks {
		if b.Number == end {
			return b.Hash
		}
	}
	return domain.Hash{}
}

func blocksBetween(from, to uint64) uint64 {
	if to < from {
		return 0
	}
	return to - from + 1
}

func backoff(base, max time.Duration, attempt int) time.Duration {
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

func rate(blocks uint64, elapsed time.Duration) string {
	if elapsed <= 0 || blocks == 0 {
		return "0"
	}
	return fmt.Sprintf("%.0f", float64(blocks)/elapsed.Seconds())
}

// eta projects the remaining time from the rate achieved so far.
func eta(remaining, done uint64, elapsed time.Duration) string {
	if done == 0 || elapsed <= 0 || remaining == 0 {
		return "unknown"
	}
	perBlock := elapsed.Seconds() / float64(done)
	return (time.Duration(perBlock * float64(remaining) * float64(time.Second))).Round(time.Second).String()
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// LagBlocks is how far the indexed head trails the last tip this instance saw.
// It is part of api.Indexing: every discovery response carries it, so a client
// can tell "this account holds no tokens" from "this instance is behind".
func (ix *Indexer) LagBlocks() uint64 { return ix.Status().LagBlocks() }

// LatestSeenBlock is the highest chain tip this instance has observed.
func (ix *Indexer) LatestSeenBlock() uint64 { return ix.Status().LatestBlock }
