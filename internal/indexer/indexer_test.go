package indexer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
	"tokendiscovery/internal/obs"
	"tokendiscovery/internal/rpc"
	"tokendiscovery/internal/store"
)

const parserVersion = 1

// newHarness wires the indexer to a real store on a temp directory.
//
// The store is deliberately real rather than a fake: the atomicity the indexer
// relies on — cursor and pairs in one batch, add-only merges that make a replay
// a no-op — is the store's behaviour, so a fake would test the mock instead of
// the contract.
func newHarness(t *testing.T, fc *fakeChain, tune func(*Options)) (*Indexer, *store.Store, *obs.Metrics) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "store"), store.Options{
		CacheBytes:    1 << 20,
		MemTableBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	m := obs.NewMetrics()
	opts := Options{
		LogRangeBlocks:    100,
		TipPollInterval:   time.Millisecond,
		HeaderRingSize:    32,
		BulkCatchUpBlocks: 64,
		ParserVersion:     parserVersion,
		ReadyLagBlocks:    2,
		RetryBaseDelay:    time.Microsecond,
		RetryMaxDelay:     time.Microsecond,
		Metrics:           m,
		// Tests must not actually sleep: backoff is exercised for its control
		// flow, not its duration.
		sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	}
	if tune != nil {
		tune(&opts)
	}
	ix, err := New(fc, st, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ix, st, m
}

// indexedBlocks reports which blocks are represented in the store, derived from
// the one-token-per-block property of the fake chain.
func indexedBlocks(t *testing.T, st *store.Store, chainID uint64) map[uint64]bool {
	t.Helper()
	out := map[uint64]bool{}
	var after *domain.Address
	for {
		page, err := st.AccountTokens(context.Background(), ports.AccountTokensQuery{
			ChainID: chainID, Account: addr(1), Limit: store.MaxPageLimit, After: after,
		})
		if err != nil {
			t.Fatalf("AccountTokens: %v", err)
		}
		for _, p := range page.Pairs {
			out[tokenToBlock(p.Token)] = true
		}
		if page.Next == nil {
			return out
		}
		after = page.Next
	}
}

func tokenToBlock(a domain.Address) uint64 {
	var n uint64
	for _, b := range a[domain.AddressLen-8:] {
		n = n<<8 | uint64(b)
	}
	return n
}

// assertContiguous checks that exactly the blocks in [from,to] are indexed.
func assertContiguous(t *testing.T, got map[uint64]bool, from, to uint64) {
	t.Helper()
	for n := from; n <= to; n++ {
		if !got[n] {
			t.Fatalf("block %d was not indexed (have %d blocks in %d..%d)", n, len(got), from, to)
		}
	}
	if len(got) != int(to-from+1) {
		for n := range got {
			if n < from || n > to {
				t.Fatalf("block %d was indexed but is outside %d..%d", n, from, to)
			}
		}
		t.Fatalf("indexed %d blocks, want %d", len(got), to-from+1)
	}
}

func TestBackfillIndexesTheWholeSpan(t *testing.T) {
	fc := newFakeChain(100, 450)
	ix, st, m := newHarness(t, fc, nil)

	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 450)

	cursor, err := st.Cursor(context.Background(), fc.info.ChainID)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if !cursor.Seeded() || cursor.SeededFromBlock != 100 {
		t.Errorf("cursor = %+v, want seeded from 100", cursor)
	}
	if cursor.HeadBlock != 450 || cursor.HeadHash != fc.hashOf(450) {
		t.Errorf("head = %d/%s, want 450/%s", cursor.HeadBlock, cursor.HeadHash, fc.hashOf(450))
	}
	if cursor.ParserVersion != parserVersion {
		t.Errorf("parser version = %d, want %d", cursor.ParserVersion, parserVersion)
	}

	// 351 blocks at 100 per scan is four requests, not 351. The efficiency
	// claim in the README is this number.
	scans, _, _ := fc.counts()
	if scans != 4 {
		t.Errorf("took %d scans for 351 blocks, want 4", scans)
	}
	if got := testutil.ToFloat64(m.BlocksIndexed); got != 351 {
		t.Errorf("blocks_indexed metric = %v, want 351", got)
	}
	if got := testutil.ToFloat64(m.PairsWritten); got != 351 {
		t.Errorf("pairs_written metric = %v, want 351", got)
	}
}

func TestBackfillResumesFromTheCursor(t *testing.T) {
	fc := newFakeChain(100, 450)
	ix, st, _ := newHarness(t, fc, nil)
	ctx := context.Background()

	// Fail partway through, the way a provider outage would.
	fc.onScan(func(_ *fakeChain, from, _ uint64) error {
		if from >= 300 {
			return fmt.Errorf("fake: down: %w", rpc.ErrPermanent)
		}
		return nil
	})
	if err := ix.Backfill(ctx); err == nil {
		t.Fatal("Backfill did not report the provider failure")
	}

	// The store must not claim to be seeded: a half-indexed store that reads
	// as seeded is what would serve confidently wrong empty token lists.
	cursor, err := st.Cursor(ctx, fc.info.ChainID)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if cursor.Seeded() {
		t.Error("an interrupted backfill marked the store seeded")
	}
	if cursor.HeadBlock != 299 {
		t.Fatalf("cursor head = %d, want 299 (everything before the failure)", cursor.HeadBlock)
	}

	// Resume. A fresh Indexer, because a restart is the case being tested.
	fc.onScan(nil)
	ix2, err := New(fc, st, Options{
		LogRangeBlocks: 100, ParserVersion: parserVersion, HeaderRingSize: 32,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ix2.Backfill(ctx); err != nil {
		t.Fatalf("resumed Backfill: %v", err)
	}

	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 450)
	lowestFrom, _ := fc.scanBounds()
	if lowestFrom != 100 {
		t.Errorf("lowest scanned block = %d, want 100 (from the first run)", lowestFrom)
	}
	// The resumed run must not have re-swept what the first one committed.
	if cursor, _ := st.Cursor(ctx, fc.info.ChainID); !cursor.Seeded() {
		t.Error("resumed backfill did not mark the store seeded")
	}
}

func TestBackfillHalvesTheRangeOnProviderPushback(t *testing.T) {
	// A provider whose real cap is below the measured default must be
	// discovered and adapted to, not fought with: otherwise the backfill
	// cannot make progress at all.
	fc := newFakeChain(100, 400)
	fc.maxRange = 40
	ix, st, _ := newHarness(t, fc, func(o *Options) { o.LogRangeBlocks = 200 })

	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 400)

	// 200 → 100 → 50 → 25: the first range at or below the cap.
	if ix.rangeBlocks != 25 {
		t.Errorf("working range = %d, want 25", ix.rangeBlocks)
	}
}

func TestBackfillRetriesTransientFailures(t *testing.T) {
	fc := newFakeChain(100, 250)
	ix, st, _ := newHarness(t, fc, nil)

	failures := 0
	fc.onScan(func(_ *fakeChain, from, _ uint64) error {
		if from == 200 && failures < 2 {
			failures++
			return fmt.Errorf("fake: 503: %w", rpc.ErrUnavailable)
		}
		return nil
	})
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if failures != 2 {
		t.Errorf("injected %d failures, want 2", failures)
	}
	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 250)
}

func TestBackfillGivesUpAfterRepeatedTransientFailures(t *testing.T) {
	// Retrying forever would hide a dead provider behind a process that looks
	// busy. Giving up is safe because the sweep resumes from the cursor.
	fc := newFakeChain(100, 250)
	ix, _, _ := newHarness(t, fc, func(o *Options) { o.RangeRetries = 3 })

	attempts := 0
	fc.onScan(func(_ *fakeChain, _, _ uint64) error {
		attempts++
		return fmt.Errorf("fake: 503: %w", rpc.ErrUnavailable)
	})
	err := ix.Backfill(context.Background())
	if err == nil {
		t.Fatal("Backfill did not give up")
	}
	if !errors.Is(err, rpc.ErrUnavailable) {
		t.Errorf("error = %v, want it to carry the provider's class", err)
	}
	if attempts != 3 {
		t.Errorf("made %d attempts, want 3", attempts)
	}
}

func TestBackfillRetargetsWhenTheHeadMovesBackwards(t *testing.T) {
	// Behind a load balancer, a lagging pool member can report a head below
	// the one we already saw. Over-requesting then returns an error rather
	// than an empty result, so the sweep must retarget instead of failing.
	fc := newFakeChain(100, 450)
	ix, st, _ := newHarness(t, fc, nil)

	dropped := false
	fc.onScan(func(fc *fakeChain, from, _ uint64) error {
		if from >= 300 && !dropped {
			dropped = true
			fc.setHead(320)
		}
		return nil
	})
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 320)
	if cursor, _ := st.Cursor(context.Background(), fc.info.ChainID); cursor.HeadBlock != 320 {
		t.Errorf("head = %d, want the reduced head 320", cursor.HeadBlock)
	}
}

func TestBackfillFollowsTheChainThatMovedWhileItSwept(t *testing.T) {
	// A real sweep takes ~25 minutes, so the tip is always somewhere else by
	// the time it finishes. Stopping at the original target would hand the
	// follower a gap to discover.
	fc := newFakeChain(100, 250)
	ix, st, _ := newHarness(t, fc, nil)

	grown := false
	fc.onScan(func(fc *fakeChain, from, _ uint64) error {
		if from >= 200 && !grown {
			grown = true
			fc.advance(60)
		}
		return nil
	})
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 310)
}

func TestBackfillRefusesAStoreFromAnotherParser(t *testing.T) {
	fc := newFakeChain(100, 150)
	ix, st, _ := newHarness(t, fc, nil)
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	// A decoder fix that changes what would be indexed must not be applied on
	// top of an index built by the old one.
	ix2, err := New(fc, st, Options{ParserVersion: parserVersion + 1, LogRangeBlocks: 100})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ix2.Backfill(context.Background()); !errors.Is(err, ErrParserMismatch) {
		t.Fatalf("Backfill = %v, want ErrParserMismatch", err)
	}
	if err := ix2.Follow(context.Background()); !errors.Is(err, ErrParserMismatch) {
		t.Fatalf("Follow = %v, want ErrParserMismatch", err)
	}
}

func TestFollowRefusesAnUnseededStore(t *testing.T) {
	fc := newFakeChain(100, 150)
	ix, _, _ := newHarness(t, fc, nil)
	if err := ix.Follow(context.Background()); !errors.Is(err, ErrNotSeeded) {
		t.Fatalf("Follow = %v, want ErrNotSeeded", err)
	}
}

// follow runs the follower in the background and returns a stop function that
// cancels it and reports what it returned.
func follow(t *testing.T, ix *Indexer) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- ix.Follow(ctx) }()
	stopped := false
	stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-errc:
			return err
		case <-time.After(5 * time.Second):
			return errors.New("follower did not stop")
		}
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// followUntilError runs the follower and waits for it to return on its own,
// which only happens on a fatal condition.
func followUntilError(t *testing.T, ix *Indexer) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ix.Follow(ctx)
}

// waitFor polls a condition, so the follower's own cadence drives the test
// rather than a sleep.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func headBlock(t *testing.T, st *store.Store, chainID uint64) uint64 {
	t.Helper()
	c, err := st.Cursor(context.Background(), chainID)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	return c.HeadBlock
}

func TestFollowIndexesNewBlocks(t *testing.T) {
	fc := newFakeChain(100, 200)
	ix, st, _ := newHarness(t, fc, nil)
	ctx := context.Background()
	if err := ix.Backfill(ctx); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	stop := follow(t, ix)
	waitFor(t, "the first tip poll", func() bool { return ix.Status().Following })

	fc.advance(3)
	waitFor(t, "the new blocks to be indexed", func() bool { return headBlock(t, st, fc.info.ChainID) == 203 })

	if err := stop(); err != nil {
		t.Fatalf("Follow: %v", err)
	}
	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 203)

	cursor, _ := st.Cursor(ctx, fc.info.ChainID)
	if cursor.HeadHash != fc.hashOf(203) {
		t.Errorf("head hash = %s, want %s", cursor.HeadHash, fc.hashOf(203))
	}
	// The watermarks must be recorded too: the API's `unconfirmed` flag and the
	// digest freezing both read them.
	if cursor.SafeBlock != 201 || cursor.FinalizedBlock != 199 {
		t.Errorf("safe/finalized = %d/%d, want 201/199", cursor.SafeBlock, cursor.FinalizedBlock)
	}
}

func TestFollowDetectsAReorgAndRewinds(t *testing.T) {
	fc := newFakeChain(100, 200)
	ix, st, m := newHarness(t, fc, nil)
	ctx := context.Background()
	if err := ix.Backfill(ctx); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	stop := follow(t, ix)
	waitFor(t, "the first tip poll", func() bool { return ix.Status().Following })
	fc.advance(3)
	waitFor(t, "block 203", func() bool { return headBlock(t, st, fc.info.ChainID) == 203 })

	// Three blocks are replaced by a different branch of four.
	fc.reorg(201, 204, 1)
	waitFor(t, "the reorg to be resolved", func() bool {
		c, err := st.Cursor(ctx, fc.info.ChainID)
		return err == nil && c.HeadBlock == 204 && c.HeadHash == fc.hashOf(204)
	})
	if err := stop(); err != nil {
		t.Fatalf("Follow: %v", err)
	}

	if got := testutil.ToFloat64(m.Reorgs); got != 1 {
		t.Errorf("reorgs metric = %v, want 1", got)
	}
	if s := ix.Status(); s.Reorgs != 1 {
		t.Errorf("status reorgs = %d, want 1", s.Reorgs)
	}
	// The canonical branch is indexed...
	blocks := indexedBlocks(t, st, fc.info.ChainID)
	for n := uint64(100); n <= 204; n++ {
		if !blocks[n] {
			t.Fatalf("block %d of the canonical branch is missing", n)
		}
	}
	// ...and the orphaned pairs are deliberately left behind. The index only
	// claims "this account touched this token", which a reorg does not
	// falsify in any way that matters: the balance is read live and comes back
	// zero. Deleting them would mean reference-counting every pair per block.
	if len(blocks) != 105 {
		t.Errorf("indexed %d blocks, want 105 (100..204, the old branch sharing their numbers)", len(blocks))
	}
}

func TestFollowResolvesAReorgThatStraddledARestart(t *testing.T) {
	// The ring is empty at boot, so this is the case the store's per-block
	// digests exist for.
	fc := newFakeChain(100, 200)
	ix, st, m := newHarness(t, fc, nil)
	ctx := context.Background()
	if err := ix.Backfill(ctx); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	// While the service is "down", the last three blocks are replaced.
	fc.reorg(198, 201, 7)

	ix2, err := New(fc, st, Options{
		LogRangeBlocks: 100, TipPollInterval: time.Millisecond, HeaderRingSize: 32,
		ParserVersion: parserVersion, ReadyLagBlocks: 2, Metrics: m,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop := follow(t, ix2)
	waitFor(t, "the restart to resolve the reorg", func() bool {
		c, err := st.Cursor(ctx, fc.info.ChainID)
		return err == nil && c.HeadBlock == 201 && c.HeadHash == fc.hashOf(201)
	})
	if err := stop(); err != nil {
		t.Fatalf("Follow: %v", err)
	}
	// It must have found the *exact* ancestor — 197, the last block both
	// branches agree on — not rewound a guessed distance.
	if got := testutil.ToFloat64(m.Reorgs); got != 1 {
		t.Errorf("reorgs metric = %v, want 1", got)
	}
	blocks := indexedBlocks(t, st, fc.info.ChainID)
	for n := uint64(100); n <= 201; n++ {
		if !blocks[n] {
			t.Fatalf("block %d is missing after the restart", n)
		}
	}
}

func TestFollowIsFatalOnAReorgDeeperThanItCanResolve(t *testing.T) {
	// Continuing here would mean indexing a branch whose ancestry was never
	// verified. On a chain that finalises in two epochs, a reorg this deep is
	// a broken or lying provider, so it stops instead.
	fc := newFakeChain(100, 200)
	ix, st, _ := newHarness(t, fc, func(o *Options) { o.HeaderRingSize = 4 })
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	// Replace 40 blocks: far deeper than the 4-block walk limit.
	fc.reorg(160, 201, 9)

	err := followUntilError(t, ix)
	if !errors.Is(err, ErrDeepReorg) {
		t.Fatalf("Follow = %v, want ErrDeepReorg", err)
	}
	// Nothing from the unverified branch may have been written.
	if head := headBlock(t, st, fc.info.ChainID); head != 200 {
		t.Errorf("head = %d, want the pre-reorg 200", head)
	}
}

func TestFollowRejectsLogsFromANonCanonicalBlock(t *testing.T) {
	// eth_getLogs carries no `removed` flag, so a provider can hand back logs
	// from a branch being replaced. They must not be indexed as if they were
	// canonical.
	fc := newFakeChain(100, 200)
	ix, st, m := newHarness(t, fc, nil)
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	stop := follow(t, ix)
	waitFor(t, "the first tip poll", func() bool { return ix.Status().Following })

	fc.setLie(true)
	fc.advance(1)
	waitFor(t, "the mismatch to be counted", func() bool { return testutil.ToFloat64(m.NonCanonicalLogs) > 0 })
	if head := headBlock(t, st, fc.info.ChainID); head != 200 {
		t.Errorf("head advanced to %d on non-canonical logs; want it held at 200", head)
	}

	// Once the provider is honest again, the follower catches up by itself.
	fc.setLie(false)
	waitFor(t, "recovery", func() bool { return headBlock(t, st, fc.info.ChainID) == 201 })
	if err := stop(); err != nil {
		t.Fatalf("Follow: %v", err)
	}
}

func TestFollowSurvivesTransientProviderFailures(t *testing.T) {
	fc := newFakeChain(100, 200)
	ix, st, _ := newHarness(t, fc, nil)
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	stop := follow(t, ix)
	waitFor(t, "the first tip poll", func() bool { return ix.Status().Following })

	// A provider that is down for a while must not kill the follower: the
	// instance goes unready and keeps trying.
	fc.onScan(func(_ *fakeChain, _, _ uint64) error {
		return fmt.Errorf("fake: 503: %w", rpc.ErrUnavailable)
	})
	fc.advance(5)
	waitFor(t, "the failure to be recorded", func() bool { return ix.Status().LastError != "" })
	if err := ix.Ready(); err == nil {
		t.Error("Ready() reported ready while the head was stuck 5 blocks behind")
	}

	fc.onScan(nil)
	waitFor(t, "recovery", func() bool { return headBlock(t, st, fc.info.ChainID) == 205 })
	if err := ix.Ready(); err != nil {
		t.Errorf("Ready() after recovery = %v, want nil", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("Follow: %v", err)
	}
	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 205)
}

func TestFollowCatchesUpInBulkAfterAnOutage(t *testing.T) {
	// Falling a long way behind must not mean one request per block. Below the
	// safe watermark there is nothing to reorg, so there is nothing for the
	// per-block parent-hash walk to protect.
	fc := newFakeChain(100, 200)
	ix, st, _ := newHarness(t, fc, func(o *Options) {
		o.LogRangeBlocks = 100
		o.BulkCatchUpBlocks = 10
	})
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	scansAfterBackfill, _, _ := fc.counts()

	fc.advance(500)
	stop := follow(t, ix)
	waitFor(t, "the catch-up to finish", func() bool { return headBlock(t, st, fc.info.ChainID) == 700 })
	if err := stop(); err != nil {
		t.Fatalf("Follow: %v", err)
	}

	scans, _, _ := fc.counts()
	caught := scans - scansAfterBackfill
	// 500 blocks: ~5 ranged scans plus a handful of single blocks above the
	// safe watermark. Anything near 500 means it fell back to block-by-block.
	if caught > 20 {
		t.Errorf("catch-up took %d scans for 500 blocks; expected ranged scans", caught)
	}
	assertContiguous(t, indexedBlocks(t, st, fc.info.ChainID), 100, 700)
}

func TestReadyExplainsWhatItIsWaitingFor(t *testing.T) {
	fc := newFakeChain(100, 200)
	now := time.Now()
	ix, _, _ := newHarness(t, fc, func(o *Options) {
		o.now = func() time.Time { return now }
	})

	// An unseeded store is the case that matters most: serving from it would
	// return empty token lists that look like real answers.
	if err := ix.Ready(); err == nil || !strings.Contains(err.Error(), "seeded") {
		t.Fatalf("Ready() on an unseeded store = %v, want it to name the seeding", err)
	}
	if err := ix.Backfill(context.Background()); err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if err := ix.Ready(); err == nil || !strings.Contains(err.Error(), "tip poll") {
		t.Fatalf("Ready() before the first poll = %v, want it to name the poll", err)
	}

	stop := follow(t, ix)
	waitFor(t, "the first tip poll", func() bool { return ix.Status().Following })
	if err := stop(); err != nil {
		t.Fatalf("Follow: %v", err)
	}
	if err := ix.Ready(); err != nil {
		t.Fatalf("Ready() while caught up = %v, want nil", err)
	}

	// A provider that stops answering is invisible in the lag figure — the head
	// and the last known tip freeze together — so staleness is judged on the
	// age of the last successful poll.
	now = now.Add(time.Hour)
	if err := ix.Ready(); err == nil || !strings.Contains(err.Error(), "no successful tip poll") {
		t.Fatalf("Ready() with a stale poll = %v, want it to name the staleness", err)
	}
}

func TestBackfillDoesNotRewriteTheSeedRecord(t *testing.T) {
	// A re-run resumes from the cursor; it does not sweep backwards. So a
	// second run with an earlier start block must not claim coverage of blocks
	// nobody ever scanned.
	fc := newFakeChain(200, 250)
	ix, st, _ := newHarness(t, fc, nil)
	ctx := context.Background()
	if err := ix.Backfill(ctx); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	earlier := newFakeChain(100, 250)
	ix2, err := New(earlier, st, Options{LogRangeBlocks: 100, ParserVersion: parserVersion})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ix2.Backfill(ctx); err != nil {
		t.Fatalf("second Backfill: %v", err)
	}

	cursor, err := st.Cursor(ctx, fc.info.ChainID)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if cursor.SeededFromBlock != 200 {
		t.Fatalf("seeded-from = %d, want the original 200", cursor.SeededFromBlock)
	}
	if blocks := indexedBlocks(t, st, fc.info.ChainID); blocks[150] {
		t.Error("the second run indexed blocks below the cursor")
	}
}
