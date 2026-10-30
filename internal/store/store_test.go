package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
)

const testChain = uint64(1)

func open(t *testing.T, dir string) *Store {
	t.Helper()
	// A small cache and memtable: these tests care about semantics, and the
	// defaults would reserve 192 MB per store across dozens of subtests.
	s, err := Open(dir, Options{CacheBytes: 1 << 20, MemTableBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open(%q): %v", dir, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newStore(t *testing.T) *Store {
	t.Helper()
	return open(t, filepath.Join(t.TempDir(), "store"))
}

// addr builds a distinct, ordered address from a counter, so tests can reason
// about scan order without hardcoding hex.
func addr(n int) domain.Address {
	var a domain.Address
	binary.BigEndian.PutUint64(a[domain.AddressLen-8:], uint64(n))
	return a
}

func pair(account, token int, std domain.Standard, first, last uint64) domain.AccountToken {
	return domain.AccountToken{
		Account:    addr(account),
		Token:      addr(token),
		Standard:   std,
		FirstBlock: first,
		LastBlock:  last,
	}
}

func cursorAt(head uint64) domain.Cursor {
	return domain.Cursor{
		SeededFromBlock: 100,
		HeadBlock:       head,
		HeadHash:        domain.Hash{byte(head), 2, 3},
		SafeBlock:       head - 1,
		FinalizedBlock:  head - 2,
		ParserVersion:   1,
	}
}

func apply(t *testing.T, s *Store, head uint64, pairs ...domain.AccountToken) {
	t.Helper()
	batch := domain.AccountTokenBatch{FromBlock: head, ToBlock: head, Pairs: pairs}
	if err := s.ApplyBatch(context.Background(), testChain, batch, cursorAt(head)); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}

func tokensOf(page ports.AccountTokensPage) []domain.Address {
	out := make([]domain.Address, len(page.Pairs))
	for i, p := range page.Pairs {
		out[i] = p.Token
	}
	return out
}

func TestFreshStoreHasNoCursor(t *testing.T) {
	// An unseeded store must be recognisable as such rather than erroring:
	// `serve` refuses to boot on it, which is what prevents a half-indexed
	// store from answering with confidently wrong empty token lists.
	s := newStore(t)
	c, err := s.Cursor(context.Background(), testChain)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if c.Seeded() || c != (domain.Cursor{}) {
		t.Fatalf("fresh store cursor = %+v, want zero", c)
	}
}

func TestApplyBatchWritesPairsAndCursorTogether(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	want := []domain.AccountToken{
		pair(1, 10, domain.StandardERC20, 500, 500),
		pair(1, 20, domain.StandardERC721, 500, 500),
	}
	apply(t, s, 500, want...)

	c, err := s.Cursor(ctx, testChain)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if c != cursorAt(500) {
		t.Fatalf("cursor = %+v, want %+v", c, cursorAt(500))
	}

	page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: addr(1)})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != 2 || page.Next != nil {
		t.Fatalf("page = %+v, want 2 pairs and no cursor", page)
	}
	if page.Pairs[0] != want[0] || page.Pairs[1] != want[1] {
		t.Fatalf("pairs = %+v, want %+v", page.Pairs, want)
	}
}

func TestApplyBatchIsIdempotentAndWidensRanges(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	// The same pair seen again later, and once in an *earlier* range that a
	// retry replayed out of order. FirstBlock must stay the minimum and
	// LastBlock become the maximum regardless of arrival order.
	apply(t, s, 500, pair(1, 10, domain.StandardERC20, 500, 500))
	apply(t, s, 500, pair(1, 10, domain.StandardERC20, 500, 500)) // replay
	apply(t, s, 700, pair(1, 10, domain.StandardERC20, 700, 700))
	apply(t, s, 300, pair(1, 10, domain.StandardERC20, 300, 300))

	page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: addr(1)})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != 1 {
		t.Fatalf("got %d pairs, want 1 (the set is add-only and deduplicated)", len(page.Pairs))
	}
	if got := page.Pairs[0]; got.FirstBlock != 300 || got.LastBlock != 700 {
		t.Fatalf("pair range = %d..%d, want 300..700", got.FirstBlock, got.LastBlock)
	}
}

func TestApplyBatchRejectsInvalidInput(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	bad := domain.AccountTokenBatch{FromBlock: 10, ToBlock: 9}
	if err := s.ApplyBatch(ctx, testChain, bad, cursorAt(10)); err == nil {
		t.Error("an inverted block range was accepted")
	}

	// StandardUnknown is the zero value and must never reach the store: it
	// would occupy a third key per token and read back as "unknown".
	unknown := domain.AccountTokenBatch{
		FromBlock: 10, ToBlock: 10,
		Pairs: []domain.AccountToken{pair(1, 10, domain.StandardUnknown, 10, 10)},
	}
	if err := s.ApplyBatch(ctx, testChain, unknown, cursorAt(10)); err == nil {
		t.Error("a pair with an unknown standard was accepted")
	}
}

func TestCancelledContextDoesNoWork(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.ApplyBatch(ctx, testChain, domain.AccountTokenBatch{}, cursorAt(500)); !errors.Is(err, context.Canceled) {
		t.Errorf("ApplyBatch on a cancelled context = %v, want context.Canceled", err)
	}
	if _, err := s.Cursor(ctx, testChain); !errors.Is(err, context.Canceled) {
		t.Errorf("Cursor on a cancelled context = %v, want context.Canceled", err)
	}
	if _, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain}); !errors.Is(err, context.Canceled) {
		t.Errorf("AccountTokens on a cancelled context = %v, want context.Canceled", err)
	}
	// The cancelled ApplyBatch must not have advanced anything.
	if c, err := s.Cursor(context.Background(), testChain); err != nil || c.Seeded() {
		t.Errorf("cursor = %+v (err %v), want zero", c, err)
	}
}

func TestDataSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s := open(t, dir)
	apply(t, s, 500, pair(1, 10, domain.StandardERC20, 500, 500))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := open(t, dir)
	c, err := s2.Cursor(context.Background(), testChain)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if c.HeadBlock != 500 {
		t.Fatalf("head block after reopen = %d, want 500", c.HeadBlock)
	}
	page, err := s2.AccountTokens(context.Background(), ports.AccountTokensQuery{ChainID: testChain, Account: addr(1)})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != 1 {
		t.Fatalf("got %d pairs after reopen, want 1", len(page.Pairs))
	}
}

func TestOpenRefusesAForeignSchema(t *testing.T) {
	// A layout change must refuse the old store rather than misread it. The
	// store is cheap to rebuild (~1.8k RPC calls), a silent misread is not.
	dir := filepath.Join(t.TempDir(), "store")
	s := open(t, dir)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], SchemaVersion+1)
	if err := s.db.Set(schemaKey, buf[:], pebble.Sync); err != nil {
		t.Fatalf("stamp a future schema version: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(dir, Options{})
	if !errors.Is(err, ErrSchemaMismatch) {
		if err == nil {
			_ = reopened.Close()
		}
		t.Fatalf("Open with a foreign schema = %v, want ErrSchemaMismatch", err)
	}
}

func TestAccountTokensIsolatesAccountsAndChains(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	apply(t, s, 500,
		pair(1, 10, domain.StandardERC20, 500, 500),
		pair(2, 20, domain.StandardERC20, 500, 500),
	)
	batch := domain.AccountTokenBatch{
		FromBlock: 500, ToBlock: 500,
		Pairs: []domain.AccountToken{pair(1, 30, domain.StandardERC20, 500, 500)},
	}
	if err := s.ApplyBatch(ctx, 137, batch, cursorAt(500)); err != nil {
		t.Fatalf("ApplyBatch on chain 137: %v", err)
	}

	// An account's page must contain only its own pairs, from only its chain.
	page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: addr(1)})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != 1 || page.Pairs[0].Token != addr(10) {
		t.Fatalf("page = %+v, want only token 10", tokensOf(page))
	}
	// The cursors are per chain too.
	if c, err := s.Cursor(ctx, 137); err != nil || c.HeadBlock != 500 {
		t.Fatalf("chain 137 cursor = %+v (err %v)", c, err)
	}
	// An account with nothing stored is an empty page, not an error.
	page, err = s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: addr(99)})
	if err != nil || len(page.Pairs) != 0 || page.Next != nil {
		t.Fatalf("unknown account page = %+v (err %v), want empty", page, err)
	}
}

func TestAccountTokensPagination(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	const total = 25
	pairs := make([]domain.AccountToken, 0, total)
	for i := 1; i <= total; i++ {
		pairs = append(pairs, pair(1, i, domain.StandardERC20, 500, 500))
	}
	apply(t, s, 500, pairs...)

	// Page through with a small limit and assert the pages concatenate to the
	// full set, in token order, with no duplicates and no gaps. Key order *is*
	// page order, which is what makes a cursor valid without a snapshot.
	var seen []domain.Address
	var after *domain.Address
	for pageNum := 0; ; pageNum++ {
		if pageNum > total {
			t.Fatal("pagination did not terminate")
		}
		page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{
			ChainID: testChain, Account: addr(1), Limit: 10, After: after,
		})
		if err != nil {
			t.Fatalf("AccountTokens: %v", err)
		}
		seen = append(seen, tokensOf(page)...)
		if page.Next == nil {
			if len(page.Pairs) == 0 && pageNum != 0 {
				t.Error("last page was empty; the cursor should have been nil one page earlier")
			}
			break
		}
		after = page.Next
	}

	if len(seen) != total {
		t.Fatalf("paged through %d tokens, want %d", len(seen), total)
	}
	for i, got := range seen {
		if want := addr(i + 1); got != want {
			t.Fatalf("token %d = %s, want %s", i, got, want)
		}
	}
}

func TestAccountTokensLimitIsBounded(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	pairs := make([]domain.AccountToken, 0, 5)
	for i := 1; i <= 5; i++ {
		pairs = append(pairs, pair(1, i, domain.StandardERC20, 500, 500))
	}
	apply(t, s, 500, pairs...)

	for _, limit := range []int{0, -1, MaxPageLimit + 1_000_000} {
		page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: addr(1), Limit: limit})
		if err != nil {
			t.Fatalf("AccountTokens(limit=%d): %v", limit, err)
		}
		if len(page.Pairs) != 5 {
			t.Errorf("limit=%d returned %d pairs, want all 5", limit, len(page.Pairs))
		}
	}
}

func TestAccountTokensFiltersByStandard(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	// Token 10 is held under both standards — a real case: an NFT contract
	// whose Transfer logs are sometimes ERC-20-shaped, or a token address
	// reused across standards.
	apply(t, s, 500,
		pair(1, 10, domain.StandardERC20, 500, 500),
		pair(1, 10, domain.StandardERC721, 500, 500),
		pair(1, 20, domain.StandardERC721, 500, 500),
		pair(1, 30, domain.StandardERC20, 500, 500),
	)

	for _, tc := range []struct {
		standard domain.Standard
		want     []domain.Address
	}{
		{domain.StandardUnknown, []domain.Address{addr(10), addr(10), addr(20), addr(30)}}, // unfiltered
		{domain.StandardERC20, []domain.Address{addr(10), addr(30)}},
		{domain.StandardERC721, []domain.Address{addr(10), addr(20)}},
	} {
		page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{
			ChainID: testChain, Account: addr(1), Standard: tc.standard,
		})
		if err != nil {
			t.Fatalf("AccountTokens(%s): %v", tc.standard, err)
		}
		got := tokensOf(page)
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("standard %s returned %v, want %v", tc.standard, got, tc.want)
		}
	}

	if _, err := s.AccountTokens(ctx, ports.AccountTokensQuery{
		ChainID: testChain, Account: addr(1), Standard: domain.Standard(77),
	}); err == nil {
		t.Error("an unsupported standard was accepted")
	}
}

func TestAccountTokensNeverSplitsATokenAcrossPages(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	// With both standards of token 2 straddling a limit of 2, ending the page
	// at exactly 2 rows would make the cursor "after token 2" and silently drop
	// the second row. The page is allowed to run one row long instead.
	apply(t, s, 500,
		pair(1, 1, domain.StandardERC20, 500, 500),
		pair(1, 2, domain.StandardERC20, 500, 500),
		pair(1, 2, domain.StandardERC721, 500, 500),
		pair(1, 3, domain.StandardERC20, 500, 500),
	)

	page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: addr(1), Limit: 2})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != 3 {
		t.Fatalf("first page = %v, want token 1 plus both rows of token 2", tokensOf(page))
	}
	if page.Next == nil || *page.Next != addr(2) {
		t.Fatalf("cursor = %v, want token 2", page.Next)
	}

	page2, err := s.AccountTokens(ctx, ports.AccountTokensQuery{
		ChainID: testChain, Account: addr(1), Limit: 2, After: page.Next,
	})
	if err != nil {
		t.Fatalf("AccountTokens(page 2): %v", err)
	}
	if len(page2.Pairs) != 1 || page2.Pairs[0].Token != addr(3) || page2.Next != nil {
		t.Fatalf("second page = %v (cursor %v), want only token 3", tokensOf(page2), page2.Next)
	}
}

func TestBalanceCacheLifecycle(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	account, token := addr(1), addr(10)

	cached := func() ports.CachedBalance {
		t.Helper()
		got, err := s.CachedBalances(ctx, testChain, account, []domain.Address{token})
		if err != nil {
			t.Fatalf("CachedBalances: %v", err)
		}
		return got[token]
	}
	has := func() bool {
		t.Helper()
		got, err := s.CachedBalances(ctx, testChain, account, []domain.Address{token})
		if err != nil {
			t.Fatalf("CachedBalances: %v", err)
		}
		_, ok := got[token]
		return ok
	}
	put := func(raw int64, block uint64) {
		t.Helper()
		err := s.PutBalances(ctx, testChain, account, []domain.Balance{
			{Token: token, Raw: big.NewInt(raw), Block: block},
		})
		if err != nil {
			t.Fatalf("PutBalances: %v", err)
		}
	}

	// Nothing cached yet.
	if has() {
		t.Fatal("a fresh store reported a cached balance")
	}

	// A transfer the indexer sees stamps the pair, but a stamp is not a value:
	// it must read as a miss, not as a stale hit of zero.
	apply(t, s, 600, pair(1, 10, domain.StandardERC20, 600, 600))
	if has() {
		t.Fatal("an invalidation stamp was served as a cached balance")
	}

	// A read at a block above the stamp is clean, and clean means *provably*
	// correct: it can be served forever without an RPC call.
	put(5, 700)
	if c := cached(); c.Dirty() || c.Balance.Raw.Int64() != 5 || c.Balance.Block != 700 {
		t.Fatalf("after a fresh read: %+v, want clean 5 at block 700", c)
	}

	// A later transfer invalidates it, and the stale value is still served —
	// with an honest flag — so a degraded read path has something to return.
	apply(t, s, 800, pair(1, 10, domain.StandardERC20, 800, 800))
	if c := cached(); !c.Dirty() || c.DirtyAtBlock != 800 || c.Balance.Raw.Int64() != 5 {
		t.Fatalf("after a transfer: %+v, want the old value marked dirty at 800", c)
	}

	// A re-read that lands below the invalidation does not clear it.
	put(6, 750)
	if c := cached(); !c.Dirty() || c.Balance.Raw.Int64() != 6 {
		t.Fatalf("after a stale re-read: %+v, want value 6 still dirty", c)
	}

	// A re-read above it does.
	put(7, 900)
	if c := cached(); c.Dirty() || c.Balance.Raw.Int64() != 7 {
		t.Fatalf("after a fresh re-read: %+v, want clean 7", c)
	}
}

func TestPutBalancesSkipsFailedReads(t *testing.T) {
	// A revert or a timeout says nothing about the balance. Caching it would
	// need its own expiry policy to ever recover, so it is simply not cached.
	s := newStore(t)
	ctx := context.Background()
	account := addr(1)

	err := s.PutBalances(ctx, testChain, account, []domain.Balance{
		{Token: addr(10), Err: "execution reverted"},
		{Token: addr(20), Raw: big.NewInt(3), Block: 700},
	})
	if err != nil {
		t.Fatalf("PutBalances: %v", err)
	}
	got, err := s.CachedBalances(ctx, testChain, account, []domain.Address{addr(10), addr(20)})
	if err != nil {
		t.Fatalf("CachedBalances: %v", err)
	}
	if _, ok := got[addr(10)]; ok {
		t.Error("a failed read was cached")
	}
	if c, ok := got[addr(20)]; !ok || c.Balance.Raw.Int64() != 3 {
		t.Errorf("successful read = %+v, ok %v; want 3", c, ok)
	}
}

func TestRewind(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	account, token := addr(1), addr(10)

	digest := func(n uint64) domain.BlockDigest {
		return domain.BlockDigest{Number: n, Hash: domain.Hash{byte(n)}, Digest: domain.Hash{0xAA, byte(n)}, LogCount: 574}
	}
	batch := domain.AccountTokenBatch{
		FromBlock: 600, ToBlock: 602,
		Pairs:  []domain.AccountToken{pair(1, 10, domain.StandardERC20, 600, 602)},
		Blocks: []domain.BlockDigest{digest(600), digest(601), digest(602)},
	}
	if err := s.ApplyBatch(ctx, testChain, batch, cursorAt(602)); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := s.PutBalances(ctx, testChain, account, []domain.Balance{{Token: token, Raw: big.NewInt(5), Block: 602}}); err != nil {
		t.Fatalf("PutBalances: %v", err)
	}

	if err := s.Rewind(ctx, testChain, 601); err != nil {
		t.Fatalf("Rewind: %v", err)
	}

	// The cursor is back at the ancestor, with the hash taken from that
	// block's stored digest so the boot-time canonicality check still works.
	c, err := s.Cursor(ctx, testChain)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if c.HeadBlock != 601 || c.HeadHash != digest(601).Hash {
		t.Fatalf("cursor after rewind = block %d hash %s, want 601 / %s", c.HeadBlock, c.HeadHash, digest(601).Hash)
	}
	if c.SeededFromBlock != 100 {
		t.Errorf("rewind lost the seeding record: SeededFromBlock = %d", c.SeededFromBlock)
	}

	// Per-block state above the ancestor is gone; at and below it is kept.
	if _, ok, err := s.blockDigest(testChain, 602); err != nil || ok {
		t.Errorf("digest for 602 survived the rewind (ok=%v, err=%v)", ok, err)
	}
	for _, n := range []uint64{600, 601} {
		if _, ok, err := s.blockDigest(testChain, n); err != nil || !ok {
			t.Errorf("digest for %d was dropped (ok=%v, err=%v)", n, ok, err)
		}
	}

	// The balance cache is flushed wholesale: every cached value is a claim
	// about a block height, and a reorg invalidates all of them at once.
	got, err := s.CachedBalances(ctx, testChain, account, []domain.Address{token})
	if err != nil {
		t.Fatalf("CachedBalances: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("balance cache survived the rewind: %+v", got)
	}

	// Pairs are deliberately kept: re-scanning re-adds the canonical ones, and
	// an orphaned pair is at worst an extra token whose live balance reads zero.
	page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: account})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != 1 {
		t.Errorf("rewind deleted account-token pairs: %+v", page.Pairs)
	}
}

func TestRewindNeverMovesTheCursorForward(t *testing.T) {
	// A rewind to above the head would claim blocks that were never indexed.
	s := newStore(t)
	ctx := context.Background()
	apply(t, s, 600, pair(1, 10, domain.StandardERC20, 600, 600))

	if err := s.Rewind(ctx, testChain, 900); err != nil {
		t.Fatalf("Rewind: %v", err)
	}
	c, err := s.Cursor(ctx, testChain)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if c.HeadBlock != 600 || c.HeadHash != cursorAt(600).HeadHash {
		t.Fatalf("cursor = block %d, want an unchanged 600", c.HeadBlock)
	}
}

func TestRewindToABlockWithNoDigestClearsTheHeadHash(t *testing.T) {
	// A block with no transfer logs has no stored digest, so there is no hash
	// to restore. Clearing it is the honest outcome: the caller re-reads the
	// header, and a guessed hash would defeat the check it feeds.
	s := newStore(t)
	ctx := context.Background()
	apply(t, s, 600, pair(1, 10, domain.StandardERC20, 600, 600))

	if err := s.Rewind(ctx, testChain, 599); err != nil {
		t.Fatalf("Rewind: %v", err)
	}
	c, err := s.Cursor(ctx, testChain)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if c.HeadBlock != 599 || !c.HeadHash.IsZero() {
		t.Fatalf("cursor = block %d hash %s, want 599 with a zero hash", c.HeadBlock, c.HeadHash)
	}
}

func TestTokenMetaVersioning(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	token := addr(10)

	read := func() domain.TokenMeta {
		t.Helper()
		got, err := s.TokenMeta(ctx, testChain, []domain.Address{token})
		if err != nil {
			t.Fatalf("TokenMeta: %v", err)
		}
		return got[token]
	}

	// Absent metadata is a normal state, not an error: "not asked yet".
	got, err := s.TokenMeta(ctx, testChain, []domain.Address{token})
	if err != nil {
		t.Fatalf("TokenMeta: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("fresh store returned metadata: %+v", got)
	}

	base := domain.TokenMeta{Token: token, Standard: domain.StandardERC20, Symbol: "USDC", Name: "USD Coin", Decimals: 6, Resolved: true}
	if err := s.PutTokenMeta(ctx, testChain, []domain.TokenMeta{base}); err != nil {
		t.Fatalf("PutTokenMeta: %v", err)
	}
	if m := read(); m.Version != 1 || m.Symbol != "USDC" {
		t.Fatalf("first write = %+v, want version 1", m)
	}

	// Re-resolving to the same values must not bump the version: the version
	// feeds the API's ETag, so a pointless bump would invalidate every client's
	// cached page for nothing.
	if err := s.PutTokenMeta(ctx, testChain, []domain.TokenMeta{base}); err != nil {
		t.Fatalf("PutTokenMeta: %v", err)
	}
	if m := read(); m.Version != 1 {
		t.Fatalf("re-resolving to the same value bumped the version to %d", m.Version)
	}

	// A symbol that resolves from null, on the other hand, must invalidate it.
	changed := base
	changed.Symbol = "USDC.e"
	if err := s.PutTokenMeta(ctx, testChain, []domain.TokenMeta{changed}); err != nil {
		t.Fatalf("PutTokenMeta: %v", err)
	}
	if m := read(); m.Version != 2 || m.Symbol != "USDC.e" {
		t.Fatalf("after a change = %+v, want version 2", m)
	}

	// The caller's own Version is ignored: the invariant has one owner.
	lying := changed
	lying.Version = 99
	lying.Decimals = 18
	if err := s.PutTokenMeta(ctx, testChain, []domain.TokenMeta{lying}); err != nil {
		t.Fatalf("PutTokenMeta: %v", err)
	}
	if m := read(); m.Version != 3 {
		t.Fatalf("version = %d, want 3 (the caller's 99 ignored)", m.Version)
	}
}

func TestCheckpointIsAnIndependentStore(t *testing.T) {
	// This is the scale-out story: a replica starts from a snapshot instead of
	// paying for its own backfill. The snapshot must be internally consistent —
	// the cursor it carries describes exactly the pairs it carries — and fully
	// detached, so the original keeps moving without touching it.
	root := t.TempDir()
	s := open(t, filepath.Join(root, "store"))
	ctx := context.Background()

	apply(t, s, 600, pair(1, 10, domain.StandardERC20, 600, 600))

	dir := filepath.Join(root, "checkpoint")
	if err := s.Checkpoint(ctx, dir); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("checkpoint directory: %v", err)
	}

	// The original advances after the snapshot was taken.
	apply(t, s, 700, pair(1, 20, domain.StandardERC20, 700, 700))

	snap := open(t, dir)
	c, err := snap.Cursor(ctx, testChain)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if c.HeadBlock != 600 {
		t.Fatalf("checkpoint cursor = %d, want 600 (the state at snapshot time)", c.HeadBlock)
	}
	page, err := snap.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: addr(1)})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != 1 || page.Pairs[0].Token != addr(10) {
		t.Fatalf("checkpoint holds %v, want only token 10", tokensOf(page))
	}

	// Checkpointing onto an existing directory would silently mix two stores.
	if err := s.Checkpoint(ctx, dir); err == nil {
		t.Error("checkpoint onto an existing directory was accepted")
	}
}

func TestCheckpointHardlinksTheData(t *testing.T) {
	// Hardlinked checkpoints are the reason the "pay for one backfill, then
	// clone" plan works: snapshotting a multi-GB store costs no extra disk up
	// front. That property is Pebble's, so what is asserted here is that this
	// store is configured in a way that keeps it — a WAL-only store, or one
	// checkpointed across filesystems, would silently copy instead.
	root := t.TempDir()
	liveDir := filepath.Join(root, "store")
	s := open(t, liveDir)
	ctx := context.Background()

	for a := 1; a <= 200; a++ {
		pairs := make([]domain.AccountToken, 0, 20)
		for i := 0; i < 20; i++ {
			pairs = append(pairs, pair(a, a*1_000+i, domain.StandardERC20, 600, 600))
		}
		apply(t, s, 600, pairs...)
	}
	// Flush so the data lives in sstables rather than the memtable; only
	// sstables can be hardlinked.
	if err := s.db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	dir := filepath.Join(root, "checkpoint")
	if err := s.Checkpoint(ctx, dir); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	liveInodes, ok := sstInodes(t, liveDir)
	if !ok {
		t.Skip("filesystem does not report inodes")
	}
	snapInodes, _ := sstInodes(t, dir)
	if len(snapInodes) == 0 {
		t.Fatal("checkpoint contains no sstables")
	}
	shared := 0
	for ino := range snapInodes {
		if liveInodes[ino] {
			shared++
		}
	}
	if shared != len(snapInodes) {
		t.Errorf("%d of %d checkpoint sstables are hardlinks to the live store; want all of them",
			shared, len(snapInodes))
	}

	// And the snapshot must still read correctly.
	snap := open(t, dir)
	page, err := snap.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: addr(200), Limit: 20})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != 20 {
		t.Fatalf("checkpoint page has %d pairs, want 20", len(page.Pairs))
	}
}

// sstInodes maps the inodes of the sstables in dir. Two files that share an
// inode are the same data on disk, which is what "hardlinked" means.
func sstInodes(t *testing.T, dir string) (map[uint64]bool, bool) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	out := map[uint64]bool{}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".sst" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", e.Name(), err)
		}
		ino, ok := inodeOf(info)
		if !ok {
			return nil, false
		}
		out[ino] = true
	}
	return out, true
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	// The service's shape is one writer (the indexer) against many concurrent
	// readers (5,000 users polling once a minute, per the assignment). Pebble
	// is safe for that by design, but the merge operators and the balance
	// invalidation logic are this package's own, so they are exercised under
	// -race rather than assumed.
	s := newStore(t)
	ctx := context.Background()
	account := addr(1)
	tokens := []domain.Address{addr(10), addr(20), addr(30)}

	done := make(chan struct{})
	var wg sync.WaitGroup

	// The indexer: advances the head, re-observing the same pairs and stamping
	// their cached balances.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for block := uint64(600); block < 700; block++ {
			pairs := make([]domain.AccountToken, 0, len(tokens))
			for i := range tokens {
				pairs = append(pairs, pair(1, 10*(i+1), domain.StandardERC20, block, block))
			}
			batch := domain.AccountTokenBatch{FromBlock: block, ToBlock: block, Pairs: pairs}
			if err := s.ApplyBatch(ctx, testChain, batch, cursorAt(block)); err != nil {
				t.Errorf("ApplyBatch: %v", err)
				break
			}
		}
		close(done)
	}()

	// Readers: discovery pages and cached balances, plus the balance writes a
	// request makes after a cache miss.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: account}); err != nil {
					t.Errorf("AccountTokens: %v", err)
					return
				}
				if _, err := s.CachedBalances(ctx, testChain, account, tokens); err != nil {
					t.Errorf("CachedBalances: %v", err)
					return
				}
				balances := make([]domain.Balance, 0, len(tokens))
				for _, tok := range tokens {
					balances = append(balances, domain.Balance{Token: tok, Raw: big.NewInt(int64(n)), Block: 650})
				}
				if err := s.PutBalances(ctx, testChain, account, balances); err != nil {
					t.Errorf("PutBalances: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	// The index must have converged on the full observed range despite the
	// interleaving.
	page, err := s.AccountTokens(ctx, ports.AccountTokensQuery{ChainID: testChain, Account: account})
	if err != nil {
		t.Fatalf("AccountTokens: %v", err)
	}
	if len(page.Pairs) != len(tokens) {
		t.Fatalf("got %d pairs, want %d", len(page.Pairs), len(tokens))
	}
	for _, p := range page.Pairs {
		if p.FirstBlock != 600 || p.LastBlock != 699 {
			t.Errorf("pair %s range = %d..%d, want 600..699", p.Token, p.FirstBlock, p.LastBlock)
		}
	}
}
