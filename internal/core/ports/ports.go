// Package ports declares the interfaces the service is built against. Nothing
// here imports a driver, a database or an HTTP client — the concrete chain
// adapters (internal/chains), store (internal/store) and RPC client
// (internal/rpc) depend on these, not the other way round.
//
// These are the seam the assignment asks for: adding a network means supplying
// a ChainAdapter, and nothing in the service or storage layers changes.
package ports

import (
	"context"

	"tokendiscovery/internal/core/domain"
)

// BalanceReader reads live token balances. Split out from ChainAdapter so the
// balance service can depend on just this.
type BalanceReader interface {
	// Balances reads balanceOf for each token at a specific block.
	//
	// atBlock is pinned by explicit number rather than a "latest" label so that
	// two calls in one response cannot silently execute at different heights.
	// Implementations return one Balance per requested token, in order, with a
	// per-token error rather than failing the whole call when one contract
	// reverts. A non-nil error means some tokens could not be read at all; the
	// slice is still complete, with those entries carrying Err and no value.
	//
	// The execution height is reported per Balance and can differ from atBlock
	// when the node no longer holds that state. Callers must trust Block rather
	// than assume the pin held.
	Balances(ctx context.Context, account domain.Address, tokens []domain.TokenRef, atBlock uint64) ([]domain.Balance, error)
}

// MetadataReader reads token display metadata. Results are cached for the
// lifetime of the store, so implementations may be slow and are called rarely.
type MetadataReader interface {
	// Metadata returns an entry for each token the contract was actually asked
	// about, with Resolved false if it had no usable answer. A token missing
	// from the result was not asked about, which the non-nil error explains,
	// and must not be recorded as having no metadata.
	Metadata(ctx context.Context, tokens []domain.TokenRef) ([]domain.TokenMeta, error)
}

// ChainAdapter is everything the service needs from one network.
//
// A second EVM network is a ChainInfo plus config. A non-EVM network is a new
// implementation of this interface; the parts that would not survive such a
// move are documented in docs/limits.md.
type ChainAdapter interface {
	BalanceReader
	MetadataReader

	// Chain describes the network this adapter serves.
	Chain() domain.ChainInfo

	// Watermarks reads the latest, safe and finalized heights. Implementations
	// are expected to poll safe/finalized on an epoch cadence rather than per
	// block, since those only move on epoch boundaries.
	Watermarks(ctx context.Context) (domain.Watermarks, error)

	// Header reads one block header. Used by the reorg detector to walk back to
	// a common ancestor, and at boot to verify the stored head is canonical.
	Header(ctx context.Context, number uint64) (domain.BlockRef, error)

	// ScanAccountTokens fetches the transfer logs for an inclusive block range
	// and decodes them into account-token pairs.
	//
	// Implementations must validate that returned logs belong to the canonical
	// chain: eth_getLogs carries no `removed` flag, so a provider can return
	// logs from a branch being replaced.
	ScanAccountTokens(ctx context.Context, from, to uint64) (domain.AccountTokenBatch, error)
}

// Registry resolves a chain slug to its adapter, so the API can be written
// against "whatever chains are configured" rather than a hardcoded set.
type Registry interface {
	Adapter(slug string) (ChainAdapter, bool)
	Chains() []domain.ChainInfo
}

// AccountTokensQuery is one page request against the index.
type AccountTokensQuery struct {
	ChainID  uint64
	Account  domain.Address
	Standard domain.Standard

	// After resumes a scan from the previous page's last token, exclusive.
	// Pagination is in token-address (key) order, which is stable by
	// construction and needs no snapshot pinning.
	After *domain.Address

	Limit int
}

// AccountTokensPage is one page of the index, with the cursor to continue from.
type AccountTokensPage struct {
	Pairs []domain.AccountToken

	// Next is nil when the page is the last one.
	Next *domain.Address
}

// Store is the durable index. The implementation is embedded (Pebble), so these
// calls are local reads rather than network round trips, which is what lets the
// read path answer without touching RPC.
type Store interface {
	// Cursor reads the indexer's durable position.
	Cursor(ctx context.Context, chainID uint64) (domain.Cursor, error)

	// ApplyBatch writes a scanned batch, the balance invalidations it implies,
	// and the advanced cursor in ONE atomic batch. Doing these together is what
	// makes a crash mid-write recoverable: on restart the cursor and the pairs
	// it describes can never disagree.
	ApplyBatch(ctx context.Context, chainID uint64, batch domain.AccountTokenBatch, cursor domain.Cursor) error

	// Rewind drops per-block state above a common ancestor, flushes the whole
	// balance cache, and resets the cursor — all in one batch. Account-token
	// pairs are deliberately NOT deleted: re-scanning re-adds the canonical
	// ones and the add-only set absorbs the rest.
	Rewind(ctx context.Context, chainID uint64, toBlock uint64) error

	// BlockHash reads the hash recorded for an indexed block.
	//
	// This is the reorg detector's ancestor oracle, and it is persistent on
	// purpose: an in-memory window of recent headers is empty right after a
	// restart, which is exactly when a reorg that happened while the service
	// was down has to be resolved. ok is false when the block was never
	// indexed, or when it carried no logs and so has no digest.
	BlockHash(ctx context.Context, chainID uint64, block uint64) (domain.Hash, bool, error)

	// AccountTokens reads one page of the index.
	AccountTokens(ctx context.Context, q AccountTokensQuery) (AccountTokensPage, error)

	// CachedBalances returns what the cache holds for these tokens. Entries
	// marked dirty by the indexer are returned too, so a degraded read path can
	// serve them with an honest staleness flag rather than failing.
	CachedBalances(ctx context.Context, chainID uint64, account domain.Address, tokens []domain.Address) (map[domain.Address]CachedBalance, error)

	// PutBalances stores freshly read balances.
	PutBalances(ctx context.Context, chainID uint64, account domain.Address, balances []domain.Balance) error

	// TokenMeta reads cached metadata; absent tokens are simply missing from the
	// result rather than erroring.
	TokenMeta(ctx context.Context, chainID uint64, tokens []domain.Address) (map[domain.Address]domain.TokenMeta, error)

	// PutTokenMeta stores resolved metadata.
	PutTokenMeta(ctx context.Context, chainID uint64, meta []domain.TokenMeta) error

	// Checkpoint writes a consistent snapshot to dir. Backed by Pebble's
	// hardlinked checkpoint, so it is near-instant and cheap.
	Checkpoint(ctx context.Context, dir string) error

	Close() error
}

// CachedBalance is a stored balance plus the staleness bookkeeping that makes
// event-driven invalidation possible.
type CachedBalance struct {
	Balance domain.Balance

	// DirtyAtBlock is non-zero when the indexer has seen a transfer touching
	// this pair since the value was read. A clean entry is not "fresh for 30
	// seconds" — it is provably still correct, so it can be served forever
	// without an RPC call.
	DirtyAtBlock uint64
}

// Dirty reports whether the cached value is known to be out of date.
func (c CachedBalance) Dirty() bool { return c.DirtyAtBlock > 0 }
