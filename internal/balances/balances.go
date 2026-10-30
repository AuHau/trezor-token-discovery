// Package balances resolves token balances for a page of discovery results.
//
// It exists as its own layer because "the balance of this pair" has three
// possible answers, and the API has to tell them apart: a value that is
// provably current, a value that is known to be out of date, and no value at
// all. Collapsing those into "balance or zero" is what would let the service
// report a confident zero for an account that simply has not been read yet.
//
// The cache is authoritative when it is clean. That is not a staleness window
// but a proof: the only thing that can change an ERC-20 balance is a transfer,
// every transfer stamps the pair in the store, and a stamp marks the entry
// dirty. So a clean entry can be served forever with no RPC call — which is
// what keeps steady-state RPC tracking on-chain activity rather than request
// rate.
package balances

import (
	"context"
	"fmt"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
	"tokendiscovery/internal/obs"
)

// Status says how much trust a returned balance deserves.
type Status string

const (
	// StatusOK is a value that is provably current: it was read at or after
	// the last transfer touching the pair.
	StatusOK Status = "ok"

	// StatusStale is a real value that a later transfer has invalidated. It is
	// returned rather than withheld so a degraded read path has something
	// honest to serve, flagged as what it is.
	StatusStale Status = "stale"

	// StatusUnavailable means no value could be produced. The token is still
	// reported — discovery does not depend on balances — but the number is
	// absent rather than guessed at.
	StatusUnavailable Status = "unavailable"
)

// Resolved is one token's balance plus how much it can be trusted.
type Resolved struct {
	Status Status
	domain.Balance
}

// Reader resolves balances for a set of tokens held by one account.
//
// atBlock is passed explicitly so every balance in a response is pinned to the
// same height as the discovery data it accompanies.
type Reader interface {
	Resolve(ctx context.Context, chainID uint64, account domain.Address, tokens []domain.Address, atBlock uint64) (map[domain.Address]Resolved, error)
}

// Store is the slice of ports.Store this package reads and writes.
type Store interface {
	CachedBalances(ctx context.Context, chainID uint64, account domain.Address, tokens []domain.Address) (map[domain.Address]ports.CachedBalance, error)
	PutBalances(ctx context.Context, chainID uint64, account domain.Address, balances []domain.Balance) error
}

// Cached resolves balances from the store alone, with no chain access.
//
// This is the whole read path when the cache is warm. Live wraps it with the
// chain reads that fill what it cannot answer.
type Cached struct {
	store Store
	m     *obs.Metrics
}

// NewCached builds a cache-only resolver.
func NewCached(store Store, m *obs.Metrics) (*Cached, error) {
	if store == nil {
		return nil, fmt.Errorf("balances: a Store is required")
	}
	return &Cached{store: store, m: m}, nil
}

// Resolve implements Reader.
func (c *Cached) Resolve(ctx context.Context, chainID uint64, account domain.Address, tokens []domain.Address, atBlock uint64) (map[domain.Address]Resolved, error) {
	cached, err := c.store.CachedBalances(ctx, chainID, account, tokens)
	if err != nil {
		return nil, err
	}

	out := make(map[domain.Address]Resolved, len(tokens))
	var hits, dirty, misses int
	for _, token := range tokens {
		entry, ok := cached[token]
		switch {
		case !ok:
			misses++
			out[token] = Resolved{
				Status:  StatusUnavailable,
				Balance: domain.Balance{Token: token, Err: "not cached"},
			}
		case entry.Dirty():
			dirty++
			out[token] = Resolved{Status: StatusStale, Balance: entry.Balance}
		default:
			hits++
			out[token] = Resolved{Status: StatusOK, Balance: entry.Balance}
		}
	}

	// These three counters are what back the claim that RPC use tracks
	// on-chain activity rather than request rate: in steady state hits should
	// dominate, and dirty should rise only with transfers.
	if c.m != nil {
		c.m.BalanceCacheHits.Add(float64(hits))
		c.m.BalanceCacheDirty.Add(float64(dirty))
		c.m.BalanceCacheMisses.Add(float64(misses))
	}
	return out, nil
}

var _ Reader = (*Cached)(nil)
