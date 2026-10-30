package evm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
	"tokendiscovery/internal/obs"
	"tokendiscovery/internal/rpc"
)

// Caller is the JSON-RPC transport the adapter needs. *rpc.Client implements
// it; tests supply a fake, which is why the adapter takes an interface rather
// than the concrete client.
type Caller interface {
	Call(ctx context.Context, out any, method string, params ...any) error
}

// Block tags. "latest" is the head, "safe" is justified and "finalized" is
// final — the three heights domain.Watermarks tracks.
const (
	tagLatest    = "latest"
	tagSafe      = "safe"
	tagFinalized = "finalized"
)

// Adapter is the EVM implementation of the chain-facing ports.
//
// It is parameterised entirely by Config, so every EVM network is served by
// this one type. The indexing half of ports.ChainAdapter — Chain, Watermarks,
// Header and ScanAccountTokens — is live; the balance and metadata halves are
// Multicall3 work and land in phase 8, where the stubs at the bottom of this
// file are replaced.
type Adapter struct {
	rpc Caller
	dec *Decoder
	cfg Config
	log *slog.Logger
	m   *obs.Metrics

	// now is injectable so the watermark cache is testable without sleeping.
	now func() time.Time

	// Safe and finalized only move on epoch boundaries, so they are cached and
	// refreshed on their own cadence rather than on every tip poll. Caching
	// here rather than in the indexer is what keeps the port's contract honest:
	// a caller may ask for watermarks as often as it likes.
	mu            sync.Mutex
	cachedSafe    domain.BlockRef
	cachedFinal   domain.BlockRef
	cachedAt      time.Time
	stallReported bool
}

// Config is the per-network configuration the adapter needs. It mirrors the
// relevant part of config.ChainConfig, so the adapter does not depend on the
// config package.
type Config struct {
	Chain domain.ChainInfo

	// WatermarkPollInterval is how long safe and finalized are cached.
	WatermarkPollInterval time.Duration

	// FinalityStallBlocks is the latest-minus-finalized gap past which the
	// chain is treated as not finalising and the block-count fallback engages.
	FinalityStallBlocks uint64

	// ConfirmationDepth is the fallback safety depth, used by the block-count
	// strategy and when finality has stalled.
	ConfirmationDepth uint64

	Logger  *slog.Logger
	Metrics *obs.Metrics
}

// NewAdapter builds an adapter for one network.
func NewAdapter(client Caller, cfg Config) (*Adapter, error) {
	if client == nil {
		return nil, errors.New("evm: a Caller is required")
	}
	if cfg.Chain.ChainID == 0 {
		return nil, errors.New("evm: chain id must not be zero")
	}
	if !cfg.Chain.Watermarks.Valid() {
		return nil, fmt.Errorf("evm: unknown watermark strategy %q", cfg.Chain.Watermarks)
	}
	if len(cfg.Chain.Standards) == 0 {
		return nil, errors.New("evm: at least one token standard must be enabled")
	}
	if cfg.ConfirmationDepth == 0 {
		cfg.ConfirmationDepth = 12
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Adapter{
		rpc: client,
		dec: NewDecoder(cfg.Chain.Standards),
		cfg: cfg,
		log: log,
		m:   cfg.Metrics,
		now: time.Now,
	}, nil
}

// Chain describes the network this adapter serves.
func (a *Adapter) Chain() domain.ChainInfo { return a.cfg.Chain }

// header is the subset of a block header the service needs. Decoded from
// strings for the same reason Log is: one unexpected field must not cost the
// whole response.
type header struct {
	Number     string `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Timestamp  string `json:"timestamp"`
}

// toRef converts a decoded header, rejecting the shapes that would otherwise
// become a zero-valued BlockRef and be mistaken for block 0.
func (h *header) toRef() (domain.BlockRef, error) {
	var ref domain.BlockRef
	if h == nil || h.Hash == "" {
		// A node that does not have the block returns result: null. That is
		// "not yet" rather than "never", so it is classified as beyond-head
		// and the caller clamps instead of failing.
		return ref, fmt.Errorf("block header is null: %w", rpc.ErrBeyondHead)
	}
	n, err := parseHexUint64(h.Number)
	if err != nil {
		return ref, fmt.Errorf("block number %q: %w", h.Number, err)
	}
	hash, err := domain.ParseHash(h.Hash)
	if err != nil {
		return ref, fmt.Errorf("block hash %q: %w", h.Hash, err)
	}
	parent, err := domain.ParseHash(h.ParentHash)
	if err != nil {
		return ref, fmt.Errorf("parent hash %q: %w", h.ParentHash, err)
	}
	ts, err := parseHexUint64(h.Timestamp)
	if err != nil {
		return ref, fmt.Errorf("timestamp %q: %w", h.Timestamp, err)
	}
	return domain.BlockRef{
		Number:     n,
		Hash:       hash,
		ParentHash: parent,
		Time:       time.Unix(int64(ts), 0).UTC(),
	}, nil
}

// Header reads one block header by number.
//
// Used by the reorg detector to walk back to a common ancestor, and at boot to
// verify the stored head is still canonical.
func (a *Adapter) Header(ctx context.Context, number uint64) (domain.BlockRef, error) {
	return a.headerAt(ctx, hexUint64(number))
}

// headerAt reads a header by number or tag.
//
// The second parameter is false: full transaction bodies would multiply the
// response size for data the service never looks at.
func (a *Adapter) headerAt(ctx context.Context, blockRef string) (domain.BlockRef, error) {
	var h *header
	if err := a.rpc.Call(ctx, &h, "eth_getBlockByNumber", blockRef, false); err != nil {
		return domain.BlockRef{}, fmt.Errorf("header %s: %w", blockRef, err)
	}
	ref, err := h.toRef()
	if err != nil {
		return domain.BlockRef{}, fmt.Errorf("header %s: %w", blockRef, err)
	}
	return ref, nil
}

// Watermarks reads the latest, safe and finalized heights.
//
// latest is always fetched: it is the head, it moves every block, and its hash
// and parent hash are what the reorg detector compares against. safe and
// finalized are cached for WatermarkPollInterval, because they only move on
// epoch boundaries — refetching them on every tip poll would triple the
// steady-state call count to learn nothing.
func (a *Adapter) Watermarks(ctx context.Context) (domain.Watermarks, error) {
	latest, err := a.headerAt(ctx, tagLatest)
	if err != nil {
		return domain.Watermarks{}, err
	}

	switch a.cfg.Chain.Watermarks {
	case domain.WatermarkImmediateFinality:
		// Every accepted block is already final, so there is nothing else to
		// ask for and no extra call to make.
		return domain.Watermarks{Latest: latest, Safe: latest, Finalized: latest}, nil

	case domain.WatermarkBlockCount:
		return domain.Watermarks{
			Latest:    latest,
			Safe:      a.depthRef(latest, a.cfg.ConfirmationDepth),
			Finalized: a.depthRef(latest, a.cfg.ConfirmationDepth),
		}, nil
	}

	safe, finalized, err := a.consensusTags(ctx, latest)
	if err != nil {
		return domain.Watermarks{}, err
	}
	wm := domain.Watermarks{Latest: latest, Safe: safe, Finalized: finalized}

	// Finality stall fallback. If the chain stops finalising, digests never
	// freeze and checkpoints never advance, so the service switches to a fixed
	// confirmation depth rather than waiting indefinitely. The gap is reported
	// once per stall rather than every poll.
	if lag := wm.FinalityLag(); lag > a.cfg.FinalityStallBlocks {
		a.noteStall(true, lag)
		fallback := a.depthRef(latest, a.cfg.ConfirmationDepth)
		if fallback.Number > wm.Safe.Number {
			wm.Safe = fallback
		}
		if fallback.Number > wm.Finalized.Number {
			wm.Finalized = fallback
		}
	} else {
		a.noteStall(false, lag)
	}
	return wm, nil
}

// noteStall logs a finality stall and its recovery once each, rather than on
// every poll.
func (a *Adapter) noteStall(stalled bool, lag uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case stalled && !a.stallReported:
		a.log.Warn("finality has stalled; falling back to a fixed confirmation depth",
			"lag_blocks", lag, "threshold", a.cfg.FinalityStallBlocks,
			"confirmation_depth", a.cfg.ConfirmationDepth)
		a.stallReported = true
	case !stalled && a.stallReported:
		a.log.Info("finality has recovered", "lag_blocks", lag)
		a.stallReported = false
	}
}

// consensusTags returns the cached safe and finalized refs, refreshing them
// when the cache has expired.
func (a *Adapter) consensusTags(ctx context.Context, latest domain.BlockRef) (safe, finalized domain.BlockRef, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	fresh := !a.cachedAt.IsZero() && a.now().Sub(a.cachedAt) < a.cfg.WatermarkPollInterval
	if fresh {
		return a.cachedSafe, a.cachedFinal, nil
	}

	safe, err = a.headerAt(ctx, tagSafe)
	if err != nil {
		// Pre-Merge nodes and some L2s do not implement these tags. Falling
		// back to a confirmation depth keeps the service working on a chain
		// that cannot answer, which is the whole point of the strategy enum.
		if a.unsupportedTag(err) {
			a.log.Warn("node does not support consensus block tags; using a confirmation depth",
				"depth", a.cfg.ConfirmationDepth, "cause", err)
			ref := a.depthRef(latest, a.cfg.ConfirmationDepth)
			a.cachedSafe, a.cachedFinal, a.cachedAt = ref, ref, a.now()
			return ref, ref, nil
		}
		return domain.BlockRef{}, domain.BlockRef{}, err
	}
	finalized, err = a.headerAt(ctx, tagFinalized)
	if err != nil {
		return domain.BlockRef{}, domain.BlockRef{}, err
	}

	a.cachedSafe, a.cachedFinal, a.cachedAt = safe, finalized, a.now()
	return safe, finalized, nil
}

// unsupportedTag reports whether an error means the node does not know the tag,
// as opposed to a transient failure. Retrying the latter is the right move, so
// they must not be conflated.
func (a *Adapter) unsupportedTag(err error) bool {
	if errors.Is(err, rpc.ErrPermanent) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown block") || strings.Contains(msg, "unsupported block")
}

// depthRef derives a watermark that is N blocks behind the head.
//
// It carries only a number: a derived height has no hash of its own without
// another request, and nothing consumes the hash of safe or finalized. The
// reorg detector only ever compares hashes at the head.
func (a *Adapter) depthRef(latest domain.BlockRef, depth uint64) domain.BlockRef {
	n := uint64(0)
	if latest.Number > depth {
		n = latest.Number - depth
	}
	if n < a.cfg.Chain.StartBlock {
		n = a.cfg.Chain.StartBlock
	}
	return domain.BlockRef{Number: n}
}

// ScanAccountTokens fetches the Transfer logs for an inclusive block range and
// decodes them into account-token pairs.
//
// One request, filtered on topic0 only, serves every account at once — that is
// the design's central claim. Nothing is filtered by address, so the cost of
// the scan is independent of how many users the service has.
//
// Range sizing and the response to an over-large range belong to the caller:
// this returns the provider's error unchanged so the indexer can halve and
// retry, which it can only do if the error class survives.
func (a *Adapter) ScanAccountTokens(ctx context.Context, from, to uint64) (domain.AccountTokenBatch, error) {
	if from > to {
		return domain.AccountTokenBatch{}, fmt.Errorf("scan: invalid range %d..%d", from, to)
	}

	var logs []Log
	err := a.rpc.Call(ctx, &logs, "eth_getLogs", map[string]any{
		"fromBlock": hexUint64(from),
		"toBlock":   hexUint64(to),
		"topics":    []string{TransferTopic0.Hex()},
	})
	if err != nil {
		return domain.AccountTokenBatch{}, err
	}

	batch, err := a.dec.Decode(from, to, logs)
	if err != nil {
		return domain.AccountTokenBatch{}, fmt.Errorf("scan %d..%d: %w", from, to, err)
	}
	if a.m != nil {
		a.m.LogsDecoded.Add(float64(batch.LogCount))
	}
	if batch.Skipped.Malformed > 0 {
		// Malformed logs mean the provider or the decoder is wrong, and the
		// resulting harm is a *missing* token — the one failure mode that
		// actually misleads a user. So it is logged, not just counted.
		a.log.Warn("logs could not be decoded",
			"from", from, "to", to, "malformed", batch.Skipped.Malformed, "logs", batch.LogCount)
	}
	return batch, nil
}

// hexUint64 formats a block number as a JSON-RPC quantity.
func hexUint64(n uint64) string { return "0x" + strconv.FormatUint(n, 16) }

// errBalancesUnimplemented is returned by the two reader halves of
// ports.ChainAdapter until the Multicall3 work lands in phase 8.
//
// They exist now, failing loudly, rather than being absent: ports.ChainAdapter
// is the seam the whole architecture is built on, and a registry that cannot
// hold the one adapter there is would be a worse lie than a method that says
// exactly what it does not do yet. Nothing calls them in phase 7 — the API's
// balance resolver reads the store's cache and reports a miss as
// "unavailable" — so the failure path is reachable only by a caller that has
// got ahead of itself.
var errBalancesUnimplemented = errors.New("evm: live balance reads land in phase 8 (Multicall3)")

// Balances implements ports.BalanceReader.
func (a *Adapter) Balances(ctx context.Context, account domain.Address, tokens []domain.TokenRef, atBlock uint64) ([]domain.Balance, error) {
	return nil, errBalancesUnimplemented
}

// Metadata implements ports.MetadataReader.
func (a *Adapter) Metadata(ctx context.Context, tokens []domain.TokenRef) ([]domain.TokenMeta, error) {
	return nil, errBalancesUnimplemented
}

var _ ports.ChainAdapter = (*Adapter)(nil)
