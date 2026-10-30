package balances

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
	"tokendiscovery/internal/obs"
)

const (
	liveChain = uint64(1)
	liveHead  = uint64(1000)
)

// fakeChain answers balance and metadata reads and counts them. Only the two
// reader methods are ever called, so the embedded nil interface is safe.
type fakeChain struct {
	ports.ChainAdapter

	reads     atomic.Int32
	metaReads atomic.Int32

	read func(ctx context.Context, tokens []domain.TokenRef) ([]domain.Balance, error)
	meta func(ctx context.Context, tokens []domain.TokenRef) ([]domain.TokenMeta, error)
}

func (f *fakeChain) ByID(id uint64) (ports.ChainAdapter, bool) {
	return f, id == liveChain
}

func (f *fakeChain) Balances(ctx context.Context, _ domain.Address, tokens []domain.TokenRef, atBlock uint64) ([]domain.Balance, error) {
	f.reads.Add(1)
	if f.read != nil {
		return f.read(ctx, tokens)
	}
	out := make([]domain.Balance, len(tokens))
	for i, t := range tokens {
		out[i] = domain.Balance{Token: t.Address, Raw: big.NewInt(42), Block: atBlock}
	}
	return out, nil
}

func (f *fakeChain) Metadata(ctx context.Context, tokens []domain.TokenRef) ([]domain.TokenMeta, error) {
	f.metaReads.Add(1)
	return f.meta(ctx, tokens)
}

// recordingStore keeps what is written so a test can assert the write-back.
type recordingStore struct {
	fakeStore
	generation atomic.Uint64

	mu      sync.Mutex
	puts    []domain.Balance
	putGens []uint64
	meta    []domain.TokenMeta
}

func (r *recordingStore) BalanceGeneration() uint64 { return r.generation.Load() }

func (r *recordingStore) PutBalances(_ context.Context, _ uint64, _ domain.Address, generation uint64, b []domain.Balance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.puts = append(r.puts, b...)
	r.putGens = append(r.putGens, generation)
	return nil
}

func (r *recordingStore) PutTokenMeta(_ context.Context, _ uint64, m []domain.TokenMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.meta = append(r.meta, m...)
	return nil
}

func newLive(t *testing.T, store *recordingStore, chain *fakeChain, budget time.Duration) (*Live, *obs.Metrics) {
	t.Helper()
	m := obs.NewMetrics()
	l, err := NewLive(store, chain, LiveOptions{Budget: budget, Metrics: m})
	if err != nil {
		t.Fatalf("NewLive: %v", err)
	}
	t.Cleanup(l.Close)
	return l, m
}

func TestLiveCleanCacheCostsNothing(t *testing.T) {
	// The design's central claim: a clean entry is a proof, not a guess, so a
	// fully cached page reaches no provider however often it is polled.
	token := addr(1)
	store := &recordingStore{fakeStore: fakeStore{cached: map[domain.Address]ports.CachedBalance{
		token: {Balance: domain.Balance{Token: token, Raw: big.NewInt(7), Block: 400}},
	}}}
	chain := &fakeChain{}
	l, _ := newLive(t, store, chain, time.Second)

	got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if chain.reads.Load() != 0 {
		t.Errorf("%d chain reads for a clean entry, want 0", chain.reads.Load())
	}
	if got[token].Status != StatusOK || got[token].Raw.Int64() != 7 {
		t.Errorf("got %+v", got[token])
	}
}

func TestLiveFillsMissesAndDirtyEntries(t *testing.T) {
	clean, dirty, missing := addr(1), addr(2), addr(3)
	store := &recordingStore{fakeStore: fakeStore{cached: map[domain.Address]ports.CachedBalance{
		clean: {Balance: domain.Balance{Token: clean, Raw: big.NewInt(7), Block: 400}},
		dirty: {Balance: domain.Balance{Token: dirty, Raw: big.NewInt(8), Block: 400}, DirtyAtBlock: 500},
	}}}
	var asked []domain.TokenRef
	chain := &fakeChain{}
	chain.read = func(_ context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		asked = tokens
		out := make([]domain.Balance, len(tokens))
		for i, t := range tokens {
			out[i] = domain.Balance{Token: t.Address, Raw: big.NewInt(42), Block: liveHead}
		}
		return out, nil
	}
	l, _ := newLive(t, store, chain, time.Second)

	got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{clean, dirty, missing}, liveHead)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(asked) != 2 {
		t.Errorf("asked about %d tokens, want only the two the cache cannot vouch for", len(asked))
	}
	if b := got[clean]; b.Status != StatusOK || b.Raw.Int64() != 7 {
		t.Errorf("clean = %+v", b)
	}
	for _, token := range []domain.Address{dirty, missing} {
		if b := got[token]; b.Status != StatusOK || b.Raw.Int64() != 42 || b.Block != liveHead {
			t.Errorf("refreshed %x = %+v", token, b)
		}
	}
	if len(store.puts) != 2 {
		t.Errorf("wrote back %d balances, want 2", len(store.puts))
	}
}

func TestLiveReadBehindThePinIsStale(t *testing.T) {
	// A read below the height the response claims could have missed a transfer.
	token := addr(1)
	chain := &fakeChain{}
	chain.read = func(_ context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		return []domain.Balance{{Token: token, Raw: big.NewInt(1), Block: liveHead - 3}}, nil
	}
	l, _ := newLive(t, &recordingStore{}, chain, time.Second)

	got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got[token].Status != StatusStale || got[token].Block != liveHead-3 {
		t.Errorf("got %+v, want stale at the block it was really read at", got[token])
	}
}

func TestLiveDegradesInsteadOfFailing(t *testing.T) {
	// Discovery is correct without balances, so an RPC outage must cost the
	// balance and never the response.
	dirty, missing := addr(1), addr(2)
	store := &recordingStore{fakeStore: fakeStore{cached: map[domain.Address]ports.CachedBalance{
		dirty: {Balance: domain.Balance{Token: dirty, Raw: big.NewInt(8), Block: 400}, DirtyAtBlock: 500},
	}}}
	chain := &fakeChain{}
	chain.read = func(_ context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		out := make([]domain.Balance, len(tokens))
		for i, t := range tokens {
			out[i] = domain.Balance{Token: t.Address, Err: "balance could not be read"}
		}
		return out, errors.New("provider down")
	}
	l, m := newLive(t, store, chain, time.Second)

	got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{dirty, missing}, liveHead)
	if err != nil {
		t.Fatalf("an outage must not fail the request: %v", err)
	}
	if b := got[dirty]; b.Status != StatusStale || b.Raw.Int64() != 8 || b.Err == "" {
		t.Errorf("dirty = %+v, want the old value, flagged, with the reason", b)
	}
	if b := got[missing]; b.Status != StatusUnavailable || b.OK() {
		t.Errorf("missing = %+v, want unavailable with no value", b)
	}
	if n := testutil.ToFloat64(m.BalanceDegraded); n != 2 {
		t.Errorf("degraded = %v, want 2", n)
	}

	// An outage is not a verdict on the contract, so the next request retries.
	if _, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{missing}, liveHead); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if chain.reads.Load() != 2 {
		t.Errorf("%d reads, want a retry after a transport failure", chain.reads.Load())
	}
}

func TestLiveEnforcesTheBudget(t *testing.T) {
	token := addr(1)
	chain := &fakeChain{}
	chain.read = func(ctx context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		<-ctx.Done() // a provider that never answers
		return []domain.Balance{{Token: token, Err: "balance could not be read"}}, ctx.Err()
	}
	l, _ := newLive(t, &recordingStore{}, chain, 20*time.Millisecond)

	done := make(chan map[domain.Address]Resolved, 1)
	go func() {
		got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead)
		if err != nil {
			t.Errorf("Resolve: %v", err)
		}
		done <- got
	}()
	select {
	case got := <-done:
		if got[token].Status != StatusUnavailable {
			t.Errorf("got %+v", got[token])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Resolve did not stop at its budget")
	}
}

func TestLiveLeavesARevertingContractAlone(t *testing.T) {
	// Nothing stamps the pair when a broken contract recovers, so it needs a
	// clock. Without one, an account holding one broken spam token would cost a
	// call on every poll, forever.
	token := addr(1)
	chain := &fakeChain{}
	chain.read = func(_ context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		return []domain.Balance{{Token: token, Reverted: true, Err: "balanceOf reverted"}}, nil
	}
	l, _ := newLive(t, &recordingStore{}, chain, time.Second)
	now := time.Unix(1000, 0)
	l.now = func() time.Time { return now }

	resolve := func() Resolved {
		t.Helper()
		got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		return got[token]
	}

	if b := resolve(); b.Status != StatusUnavailable || b.Err != "balanceOf reverted" {
		t.Errorf("first = %+v", b)
	}
	if b := resolve(); b.Err != "balanceOf reverted" {
		t.Errorf("remembered = %+v, want the reason kept", b)
	}
	if n := chain.reads.Load(); n != 1 {
		t.Errorf("%d reads inside the window, want 1", n)
	}

	now = now.Add(revertedRetry)
	resolve()
	if n := chain.reads.Load(); n != 2 {
		t.Errorf("%d reads after the window, want a retry", n)
	}
}

func TestLiveRememberedRevertsAreBounded(t *testing.T) {
	l, _ := newLive(t, &recordingStore{}, &fakeChain{}, time.Second)
	for i := range maxRemembered + 10 {
		var a domain.Address
		a[0], a[1], a[2] = byte(i), byte(i>>8), byte(i>>16)
		l.rememberReverted(revertKey{liveChain, a, a}, "reverted")
	}
	if n := len(l.reverted); n > maxRemembered {
		t.Errorf("remembered %d entries, want at most %d", n, maxRemembered)
	}
}

func TestLiveTakesTheGenerationBeforeReading(t *testing.T) {
	// A reorg during the read has to keep its result out of the cache, which
	// only works if the write-back carries the generation from before it.
	token := addr(1)
	store := &recordingStore{}
	chain := &fakeChain{}
	chain.read = func(_ context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		store.generation.Add(1)
		return []domain.Balance{{Token: tokens[0].Address, Raw: big.NewInt(99), Block: liveHead}}, nil
	}
	l, _ := newLive(t, store, chain, time.Second)

	if _, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.putGens) != 1 || store.putGens[0] != 0 {
		t.Errorf("write-back generations = %v, want the one from before the read", store.putGens)
	}
}

func TestLiveDoesNotShareAReadAcrossARewind(t *testing.T) {
	// A request arriving after a reorg must not be answered by a read from
	// before it, even though it asks for the same tokens at the same height.
	token := addr(1)
	store := &recordingStore{}
	started, release := make(chan struct{}), make(chan struct{})
	chain := &fakeChain{}
	chain.read = func(_ context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		value := int64(7)
		if chain.reads.Load() == 1 {
			close(started)
			<-release
			value = 99
		}
		return []domain.Balance{{Token: tokens[0].Address, Raw: big.NewInt(value), Block: liveHead}}, nil
	}
	l, _ := newLive(t, store, chain, 5*time.Second)
	tokens := []domain.Address{token}

	before := make(chan struct{})
	go func() {
		defer close(before)
		if _, err := l.Resolve(context.Background(), liveChain, addr(9), tokens, liveHead); err != nil {
			t.Errorf("Resolve: %v", err)
		}
	}()
	<-started

	store.generation.Add(1)

	got, err := l.Resolve(context.Background(), liveChain, addr(9), tokens, liveHead)
	close(release)
	<-before
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if chain.reads.Load() != 2 || got[token].Raw == nil || got[token].Raw.Int64() != 7 {
		t.Errorf("%d reads, got %+v; want a second read answering 7", chain.reads.Load(), got[token])
	}
}

func TestLiveCoalescesConcurrentRequests(t *testing.T) {
	token := addr(1)
	release := make(chan struct{})
	started := make(chan struct{}, 16)
	chain := &fakeChain{}
	chain.read = func(ctx context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		started <- struct{}{}
		<-release
		return []domain.Balance{{Token: token, Raw: big.NewInt(1), Block: liveHead}}, nil
	}
	l, m := newLive(t, &recordingStore{}, chain, 5*time.Second)

	const callers = 8
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead)
			if err != nil || got[token].Status != StatusOK {
				t.Errorf("got %+v, err %v", got[token], err)
			}
		})
	}
	<-started
	// Give the rest time to join; a late one only makes the count conservative.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if n := chain.reads.Load(); n >= callers {
		t.Errorf("%d reads for %d concurrent callers, want them shared", n, callers)
	}
	if testutil.ToFloat64(m.BalanceCoalesced) == 0 {
		t.Error("no coalescing was recorded")
	}
}

func TestLiveReadSurvivesTheCallerLeaving(t *testing.T) {
	// The read is shared and already paid for. A client hanging up must neither
	// abort it for the others nor throw its result away.
	token := addr(1)
	release := make(chan struct{})
	chain := &fakeChain{}
	var readCtxErr atomic.Value
	chain.read = func(ctx context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		<-release
		if err := ctx.Err(); err != nil {
			readCtxErr.Store(err)
		}
		return []domain.Balance{{Token: token, Raw: big.NewInt(1), Block: liveHead}}, nil
	}
	store := &recordingStore{}
	l, _ := newLive(t, store, chain, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := l.Resolve(ctx, liveChain, addr(9), []domain.Address{token}, liveHead)
	if err == nil && got == nil {
		t.Fatal("no result")
	}
	close(release)

	deadline := time.Now().Add(5 * time.Second)
	for {
		store.mu.Lock()
		n := len(store.puts)
		store.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the read was not written back after its caller left")
		}
		time.Sleep(time.Millisecond)
	}
	if err := readCtxErr.Load(); err != nil {
		t.Errorf("the read was cancelled with its caller: %v", err)
	}
}

func TestLiveFillAsksOnceAndKeepsAnswers(t *testing.T) {
	known, fresh, unasked := addr(1), addr(2), addr(3)
	chain := &fakeChain{}
	var gotStandard domain.Standard
	chain.meta = func(_ context.Context, tokens []domain.TokenRef) ([]domain.TokenMeta, error) {
		if len(tokens) != 2 {
			t.Errorf("asked about %d tokens, want only the unknown ones", len(tokens))
		}
		gotStandard = tokens[0].Standard
		// The outage took one of the two: it is not an answer.
		return []domain.TokenMeta{{Token: fresh, Symbol: "NEW", Resolved: true}}, errors.New("partial outage")
	}
	store := &recordingStore{}
	l, _ := newLive(t, store, chain, time.Second)

	start := map[domain.Address]domain.TokenMeta{known: {Token: known, Symbol: "OLD", Resolved: true}}
	got := l.Fill(context.Background(), liveChain, domain.StandardERC721, []domain.Address{known, fresh, unasked}, start)

	if got[known].Symbol != "OLD" || got[fresh].Symbol != "NEW" {
		t.Errorf("got %+v", got)
	}
	if _, ok := got[unasked]; ok {
		t.Error("a token nobody answered for was recorded")
	}
	if gotStandard != domain.StandardERC721 {
		t.Errorf("standard = %v, want it passed through", gotStandard)
	}
	if len(store.meta) != 1 || store.meta[0].Token != fresh {
		t.Errorf("stored %+v, want only the answered token", store.meta)
	}

	// All known: no call at all.
	before := chain.metaReads.Load()
	l.Fill(context.Background(), liveChain, domain.StandardERC20, []domain.Address{known}, start)
	if chain.metaReads.Load() != before {
		t.Error("a fully known page reached the chain")
	}
}

func TestLiveStoreFailureIsAnError(t *testing.T) {
	// Unlike RPC, a failing store means the index itself cannot be trusted.
	store := &recordingStore{fakeStore: fakeStore{err: errors.New("disk")}}
	l, _ := newLive(t, store, &fakeChain{}, time.Second)
	if _, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{addr(1)}, liveHead); err == nil {
		t.Fatal("expected the store error")
	}
}

func TestLiveRereadsAnOldCleanEntry(t *testing.T) {
	// stETH changes every holder's balance with no Transfer, so nothing would
	// ever mark this entry dirty. Without an age limit it reads ok forever.
	token := addr(1)
	store := &recordingStore{fakeStore: fakeStore{cached: map[domain.Address]ports.CachedBalance{
		token: {Balance: domain.Balance{Token: token, Raw: big.NewInt(100), Block: liveHead}},
	}}}
	chain := &fakeChain{}
	l, _ := newLive(t, store, chain, time.Second)
	resolve := func(atBlock uint64) Resolved {
		t.Helper()
		got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, atBlock)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		return got[token]
	}

	if b := resolve(liveHead + defaultMaxAgeBlocks - 1); b.Status != StatusOK || b.Raw.Int64() != 100 {
		t.Errorf("inside the limit = %+v", b)
	}
	if n := chain.reads.Load(); n != 0 {
		t.Errorf("%d reads inside the limit, want 0", n)
	}

	at := liveHead + defaultMaxAgeBlocks
	if b := resolve(at); b.Status != StatusOK || b.Raw.Int64() != 42 || b.Block != at {
		t.Errorf("at the limit = %+v, want the re-read value", b)
	}
	if n := chain.reads.Load(); n != 1 {
		t.Errorf("%d reads at the limit, want 1", n)
	}

	// If the re-read fails, the old value is still served, but no longer as ok.
	chain.read = func(_ context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		return []domain.Balance{{Token: token, Err: "balance could not be read"}}, errors.New("provider down")
	}
	if b := resolve(at); b.Status != StatusStale || b.Raw.Int64() != 100 || b.Err == "" {
		t.Errorf("failed re-read = %+v, want the old value marked stale", b)
	}
}

func TestLiveReadOutlivesTheBudget(t *testing.T) {
	// Under load a read can take longer than a request will wait. Throwing it
	// away at the budget is what stopped a cold cache from ever filling.
	token := addr(1)
	release := make(chan struct{})
	chain := &fakeChain{}
	chain.read = func(ctx context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		<-release
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return []domain.Balance{{Token: token, Raw: big.NewInt(1), Block: liveHead}}, nil
	}
	store := &recordingStore{}
	l, _ := newLive(t, store, chain, time.Millisecond)

	got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got[token].Status != StatusUnavailable {
		t.Errorf("got %+v, want unavailable once the budget has passed", got[token])
	}
	close(release)

	deadline := time.Now().Add(5 * time.Second)
	for {
		store.mu.Lock()
		n := len(store.puts)
		store.mu.Unlock()
		if n == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the read was not written back after the budget passed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLiveShedsReadsPastTheLimit(t *testing.T) {
	// Demand above the provider rate cannot be queued away, so a request past the
	// limit gets the cache's answer at once instead of waiting out its budget.
	token := addr(1)
	started := make(chan struct{})
	release := make(chan struct{})
	chain := &fakeChain{}
	chain.read = func(ctx context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		close(started)
		<-release
		return nil, nil
	}
	m := obs.NewMetrics()
	l, err := NewLive(&recordingStore{}, chain, LiveOptions{Budget: time.Hour, MaxReads: 1, Metrics: m})
	if err != nil {
		t.Fatalf("NewLive: %v", err)
	}
	t.Cleanup(func() {
		close(release)
		l.Close()
	})

	go l.Resolve(context.Background(), liveChain, addr(8), []domain.Address{token}, liveHead)
	<-started

	got, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got[token].Status != StatusUnavailable {
		t.Errorf("got %+v, want unavailable", got[token])
	}
	if chain.reads.Load() != 1 {
		t.Errorf("%d chain reads, want 1", chain.reads.Load())
	}
	if n := testutil.ToFloat64(m.BalanceReadsShed); n != 1 {
		t.Errorf("shed = %v, want 1", n)
	}
}

func TestLiveCloseStopsReadsBeforeTheStoreCloses(t *testing.T) {
	// A read ends by writing to the store, and Pebble panics on a closed
	// database, so Close has to wait for it. This crashed serve on SIGTERM.
	token := addr(1)
	started := make(chan struct{})
	var finished atomic.Bool
	chain := &fakeChain{}
	chain.read = func(ctx context.Context, tokens []domain.TokenRef) ([]domain.Balance, error) {
		close(started)
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond)
		finished.Store(true)
		return nil, ctx.Err()
	}
	l, _ := newLive(t, &recordingStore{}, chain, time.Millisecond)

	if _, err := l.Resolve(context.Background(), liveChain, addr(9), []domain.Address{token}, liveHead); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	<-started
	l.Close()
	if !finished.Load() {
		t.Error("Close returned while a read was still running")
	}

	if _, err := l.Resolve(context.Background(), liveChain, addr(7), []domain.Address{token}, liveHead); err != nil {
		t.Fatalf("Resolve after Close: %v", err)
	}
	if chain.reads.Load() != 1 {
		t.Errorf("%d chain reads, want none after Close", chain.reads.Load())
	}
}
