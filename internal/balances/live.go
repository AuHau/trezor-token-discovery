package balances

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
	"tokendiscovery/internal/obs"
)

const (
	// defaultBudget bounds the live reads behind one request. Discovery itself
	// is a local scan, so without this a slow provider would make a fast
	// endpoint slow for no benefit: past the budget a stale value is the better
	// answer.
	defaultBudget = 800 * time.Millisecond

	// revertedRetry is how long a contract that refused balanceOf is left alone.
	// Nothing stamps the pair when such a contract starts working again, so it
	// needs a clock, unlike a value — but without this, an account holding one
	// broken spam token would cost an eth_call on every poll, indefinitely.
	revertedRetry = 10 * time.Minute

	// defaultMaxAgeBlocks is how old a clean entry may get before it is read
	// again, about a day on Ethereum. Rebasing tokens such as stETH change every
	// holder's balance with no Transfer, so no stamp ever marks them dirty. This
	// bounds that error for one extra read per active account per day, instead
	// of keeping a list of which tokens rebase.
	defaultMaxAgeBlocks = 7200

	// maxRemembered bounds the memory a flood of broken contracts can claim.
	maxRemembered = 1 << 16
)

// LiveStore is the slice of ports.Store the live resolver needs.
type LiveStore interface {
	Store
	PutTokenMeta(ctx context.Context, chainID uint64, meta []domain.TokenMeta) error
}

// Chains resolves a chain ID to whatever reads from that chain.
type Chains interface {
	ByID(chainID uint64) (ports.ChainAdapter, bool)
}

// LiveOptions configures a Live resolver.
type LiveOptions struct {
	// Budget bounds chain reads per request. Zero selects the default.
	Budget time.Duration
	// MaxAgeBlocks is how old a clean entry may get. Zero selects the default.
	MaxAgeBlocks uint64
	Logger       *slog.Logger
	Metrics      *obs.Metrics
}

// Live resolves balances from the cache and fills what the cache cannot prove
// current with a chain read, which it writes back.
//
// A failed read is never an error for the caller: the token keeps whatever the
// cache had, flagged accordingly, because discovery stays correct without
// balances and must not fail because of them.
type Live struct {
	cached *Cached
	store  LiveStore
	chains Chains
	budget time.Duration
	maxAge uint64
	log    *slog.Logger
	m      *obs.Metrics

	// flight makes a hot account under load one read rather than one per
	// request. Keyed by the exact set of tokens, so it collapses repeated polls
	// of a page; overlapping but different pages still read separately.
	flight singleflight.Group

	// now is injectable so the reverted-contract window is testable.
	now func() time.Time

	mu       sync.Mutex
	reverted map[revertKey]revertedRead
}

type revertKey struct {
	chainID uint64
	account domain.Address
	token   domain.Address
}

type revertedRead struct {
	reason string
	until  time.Time
}

// NewLive builds a resolver that reads through to the chain.
func NewLive(store LiveStore, chains Chains, o LiveOptions) (*Live, error) {
	if chains == nil {
		return nil, errors.New("balances: a chain source is required")
	}
	cached, err := NewCached(store, o.Metrics)
	if err != nil {
		return nil, err
	}
	if o.Budget <= 0 {
		o.Budget = defaultBudget
	}
	if o.MaxAgeBlocks == 0 {
		o.MaxAgeBlocks = defaultMaxAgeBlocks
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return &Live{
		cached:   cached,
		store:    store,
		chains:   chains,
		budget:   o.Budget,
		maxAge:   o.MaxAgeBlocks,
		log:      o.Logger,
		m:        o.Metrics,
		now:      time.Now,
		reverted: make(map[revertKey]revertedRead),
	}, nil
}

// Resolve implements Reader.
func (l *Live) Resolve(ctx context.Context, chainID uint64, account domain.Address, tokens []domain.Address, atBlock uint64) (map[domain.Address]Resolved, error) {
	out, err := l.cached.Resolve(ctx, chainID, account, tokens, atBlock)
	if err != nil {
		return nil, err
	}

	var refs []domain.TokenRef
	for _, token := range tokens {
		if entry := out[token]; entry.Status == StatusOK {
			if atBlock < entry.Block+l.maxAge {
				continue
			}
			// Too old to vouch for. If the re-read fails, this is what it is.
			entry.Status = StatusStale
			out[token] = entry
		}
		if reason, ok := l.recentlyReverted(revertKey{chainID, account, token}); ok {
			entry := out[token]
			entry.Err = reason
			out[token] = entry
			continue
		}
		refs = append(refs, domain.TokenRef{ChainID: chainID, Address: token})
	}
	if len(refs) == 0 {
		return out, nil
	}

	reader, ok := l.chains.ByID(chainID)
	if !ok {
		return out, nil
	}

	// The read is shared, so it must not die with whichever request happened to
	// start it. Each caller still stops waiting when its own context ends.
	ch := l.flight.DoChan(flightKey("b", chainID, account, atBlock, refs), func() (any, error) {
		return l.read(context.WithoutCancel(ctx), reader, chainID, account, refs, atBlock), nil
	})
	select {
	case <-ctx.Done():
		return out, nil
	case res := <-ch:
		if res.Shared && l.m != nil {
			l.m.BalanceCoalesced.Inc()
		}
		for _, b := range res.Val.([]domain.Balance) {
			if !b.OK() {
				entry := out[b.Token]
				entry.Err = b.Err
				out[b.Token] = entry
				continue
			}
			// Behind the pin means a transfer may sit between the read and the
			// height this response claims, so it is not provably current.
			status := StatusOK
			if b.Block < atBlock {
				status = StatusStale
			}
			out[b.Token] = Resolved{Status: status, Balance: b}
		}
	}
	return out, nil
}

// read performs one budgeted chain read and writes the results back.
func (l *Live) read(ctx context.Context, reader ports.BalanceReader, chainID uint64, account domain.Address, refs []domain.TokenRef, atBlock uint64) []domain.Balance {
	readCtx, cancel := context.WithTimeout(ctx, l.budget)
	defer cancel()

	fresh, err := reader.Balances(readCtx, account, refs, atBlock)
	if err != nil {
		l.log.Warn("balance read failed", "account", account.Checksum(), "tokens", len(refs), "error", err)
	}

	good := fresh[:0:0]
	degraded := 0
	for _, b := range fresh {
		switch {
		case b.OK():
			good = append(good, b)
		case b.Reverted:
			degraded++
			l.rememberReverted(revertKey{chainID, account, b.Token}, b.Err)
		default:
			degraded++
		}
	}
	if degraded > 0 && l.m != nil {
		l.m.BalanceDegraded.Add(float64(degraded))
	}
	// Written with the unbounded context: a read that came back just inside the
	// budget has already been paid for, so it should not be thrown away.
	if err := l.store.PutBalances(ctx, chainID, account, good); err != nil {
		l.log.Warn("balance write-back failed", "account", account.Checksum(), "error", err)
	}
	return fresh
}

// Fill completes known with metadata read from the chain for the tokens it does
// not cover, and returns the result. Failures leave a token out, to be asked
// about again by a later request; only a contract's actual answer is stored,
// and that is never asked for again.
func (l *Live) Fill(ctx context.Context, chainID uint64, standard domain.Standard, tokens []domain.Address, known map[domain.Address]domain.TokenMeta) map[domain.Address]domain.TokenMeta {
	var refs []domain.TokenRef
	for _, token := range tokens {
		if _, ok := known[token]; !ok {
			refs = append(refs, domain.TokenRef{ChainID: chainID, Address: token, Standard: standard})
		}
	}
	if len(refs) == 0 {
		return known
	}
	reader, ok := l.chains.ByID(chainID)
	if !ok {
		return known
	}

	detached := context.WithoutCancel(ctx)
	ch := l.flight.DoChan(flightKey("m", chainID, domain.Address{}, uint64(standard), refs), func() (any, error) {
		readCtx, cancel := context.WithTimeout(detached, l.budget)
		defer cancel()
		meta, err := reader.Metadata(readCtx, refs)
		if err != nil {
			l.log.Warn("token metadata read failed", "tokens", len(refs), "error", err)
		}
		if err := l.store.PutTokenMeta(detached, chainID, meta); err != nil {
			l.log.Warn("token metadata write failed", "error", err)
		}
		return meta, nil
	})

	select {
	case <-ctx.Done():
	case res := <-ch:
		merged := make(map[domain.Address]domain.TokenMeta, len(tokens))
		for token, m := range known {
			merged[token] = m
		}
		for _, m := range res.Val.([]domain.TokenMeta) {
			merged[m.Token] = m
		}
		return merged
	}
	return known
}

func (l *Live) recentlyReverted(key revertKey) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.reverted[key]
	if !ok {
		return "", false
	}
	if !l.now().Before(r.until) {
		delete(l.reverted, key)
		return "", false
	}
	return r.reason, true
}

func (l *Live) rememberReverted(key revertKey, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if len(l.reverted) >= maxRemembered {
		for k, r := range l.reverted {
			if !now.Before(r.until) {
				delete(l.reverted, k)
			}
		}
		// Everything is still live, so this is a flood. Forgetting costs a
		// re-read; growing without bound costs the process.
		if len(l.reverted) >= maxRemembered {
			clear(l.reverted)
		}
	}
	l.reverted[key] = revertedRead{reason: reason, until: now.Add(revertedRetry)}
}

func flightKey(kind string, chainID uint64, account domain.Address, n uint64, refs []domain.TokenRef) string {
	var b strings.Builder
	b.Grow(len(kind) + 48 + len(refs)*domain.AddressLen)
	fmt.Fprintf(&b, "%s/%d/%x/%d/", kind, chainID, account[:], n)
	for _, r := range refs {
		b.Write(r.Address[:])
	}
	return b.String()
}

var _ Reader = (*Live)(nil)
