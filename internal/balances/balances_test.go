package balances

import (
	"context"
	"math/big"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
	"tokendiscovery/internal/obs"
)

type fakeStore struct {
	cached map[domain.Address]ports.CachedBalance
	err    error
}

func (f *fakeStore) CachedBalances(_ context.Context, _ uint64, _ domain.Address, tokens []domain.Address) (map[domain.Address]ports.CachedBalance, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := map[domain.Address]ports.CachedBalance{}
	for _, t := range tokens {
		if entry, ok := f.cached[t]; ok {
			out[t] = entry
		}
	}
	return out, nil
}

func (f *fakeStore) PutBalances(context.Context, uint64, domain.Address, []domain.Balance) error {
	return nil
}

func addr(n byte) domain.Address {
	var a domain.Address
	a[domain.AddressLen-1] = n
	return a
}

func TestCachedResolveDistinguishesThreeAnswers(t *testing.T) {
	// The whole point of this layer: "provably current", "known out of date"
	// and "no value at all" are three different answers, and collapsing them
	// would let the service report a confident zero for an account it has
	// never read.
	clean, stale, missing := addr(1), addr(2), addr(3)
	store := &fakeStore{cached: map[domain.Address]ports.CachedBalance{
		clean: {Balance: domain.Balance{Token: clean, Raw: big.NewInt(100), Block: 500}},
		stale: {
			Balance:      domain.Balance{Token: stale, Raw: big.NewInt(7), Block: 400},
			DirtyAtBlock: 450,
		},
	}}

	m := obs.NewMetrics()
	c, err := NewCached(store, m)
	if err != nil {
		t.Fatalf("NewCached: %v", err)
	}

	got, err := c.Resolve(context.Background(), 1, addr(9), []domain.Address{clean, stale, missing}, 500)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("resolved %d tokens, want 3", len(got))
	}
	if r := got[clean]; r.Status != StatusOK || r.Raw.Int64() != 100 || r.Block != 500 {
		t.Errorf("clean entry = %+v, want ok/100/500", r)
	}
	if r := got[stale]; r.Status != StatusStale || r.Raw.Int64() != 7 {
		t.Errorf("dirty entry = %+v, want the old value marked stale", r)
	}
	if r := got[missing]; r.Status != StatusUnavailable || r.OK() {
		t.Errorf("missing entry = %+v, want unavailable with no value", r)
	}

	// These counters are the evidence for "RPC use tracks on-chain activity,
	// not request rate", so they have to be recorded where the decision is made.
	if got := testutil.ToFloat64(m.BalanceCacheHits); got != 1 {
		t.Errorf("hits = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.BalanceCacheDirty); got != 1 {
		t.Errorf("dirty = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.BalanceCacheMisses); got != 1 {
		t.Errorf("misses = %v, want 1", got)
	}
}

func TestCachedResolveNoTokens(t *testing.T) {
	c, err := NewCached(&fakeStore{}, nil)
	if err != nil {
		t.Fatalf("NewCached: %v", err)
	}
	got, err := c.Resolve(context.Background(), 1, addr(9), nil, 500)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("resolved %d tokens for an empty page", len(got))
	}
}

func TestNewCachedRequiresAStore(t *testing.T) {
	if _, err := NewCached(nil, nil); err == nil {
		t.Error("a nil store was accepted")
	}
}
