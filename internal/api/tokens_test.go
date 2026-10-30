package api

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"tokendiscovery/internal/balances"
	"tokendiscovery/internal/chains"
	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/obs"
	"tokendiscovery/internal/store"
)

const (
	testChainID = uint64(1)
	testSlug    = "ethereum"
	headBlock   = uint64(26_304_142)
	safeBlock   = uint64(26_304_100)
)

// fakeAdapter is a chain in the registry. The API only reads Chain() from an
// adapter — discovery never touches the network — so the rest exists to satisfy
// the port.
type fakeAdapter struct{ info domain.ChainInfo }

func (f fakeAdapter) Chain() domain.ChainInfo { return f.info }
func (f fakeAdapter) Watermarks(context.Context) (domain.Watermarks, error) {
	return domain.Watermarks{}, errors.New("not used")
}
func (f fakeAdapter) Header(context.Context, uint64) (domain.BlockRef, error) {
	return domain.BlockRef{}, errors.New("not used")
}
func (f fakeAdapter) ScanAccountTokens(context.Context, uint64, uint64) (domain.AccountTokenBatch, error) {
	return domain.AccountTokenBatch{}, errors.New("not used")
}
func (f fakeAdapter) Balances(context.Context, domain.Address, []domain.TokenRef, uint64) ([]domain.Balance, error) {
	return nil, errors.New("not used")
}
func (f fakeAdapter) Metadata(context.Context, []domain.TokenRef) ([]domain.TokenMeta, error) {
	return nil, errors.New("not used")
}

type stubReadiness struct{ err error }

func (s stubReadiness) Ready() error            { return s.err }
func (s stubReadiness) LagBlocks() uint64       { return 0 }
func (s stubReadiness) LatestSeenBlock() uint64 { return headBlock }

type harness struct {
	handler http.Handler
	store   *store.Store
	metrics *obs.Metrics
}

func newHarness(t *testing.T, ready Readiness) *harness {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "store"), store.Options{
		CacheBytes: 1 << 20, MemTableBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	registry, err := chains.NewRegistry(fakeAdapter{info: domain.ChainInfo{
		Slug:       testSlug,
		ChainID:    testChainID,
		StartBlock: 25_933_634,
		Standards:  []domain.Standard{domain.StandardERC20, domain.StandardERC721},
		Watermarks: domain.WatermarkConsensusTags,
	}})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	bal, err := balances.NewCached(st, nil)
	if err != nil {
		t.Fatalf("NewCached: %v", err)
	}

	m := obs.NewMetrics()
	indexing := stubReadiness{}
	if ready != nil {
		indexing.err = ready.Ready()
	}
	return &harness{
		handler: NewRouter(Deps{
			Registry: m.Registry,
			Gatherer: m.Registry,
			Metrics:  m,
			Indexer:  indexing,
			Chains:   registry,
			Store:    st,
			Balances: bal,
			Version:  "test",
		}),
		store:   st,
		metrics: m,
	}
}

func addr(n uint64) domain.Address {
	var a domain.Address
	binary.BigEndian.PutUint64(a[domain.AddressLen-8:], n)
	return a
}

// seed writes pairs and a cursor, standing in for a completed backfill.
func (h *harness) seed(t *testing.T, pairs ...domain.AccountToken) {
	t.Helper()
	cursor := domain.Cursor{
		SeededFromBlock: 25_933_634,
		HeadBlock:       headBlock,
		HeadHash:        domain.Hash{0xAB, 0xCD},
		SafeBlock:       safeBlock,
		FinalizedBlock:  safeBlock - 32,
		ParserVersion:   1,
	}
	batch := domain.AccountTokenBatch{FromBlock: headBlock, ToBlock: headBlock, Pairs: pairs}
	if err := h.store.ApplyBatch(context.Background(), testChainID, batch, cursor); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}

func pair(token uint64, std domain.Standard, first, last uint64) domain.AccountToken {
	return domain.AccountToken{
		Account:    addr(1),
		Token:      addr(token),
		Standard:   std,
		FirstBlock: first,
		LastBlock:  last,
	}
}

func (h *harness) get(t *testing.T, path string, headers ...[2]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for _, kv := range headers {
		req.Header.Set(kv[0], kv[1])
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func tokensPath(account string) string {
	return "/v1/chains/" + testSlug + "/accounts/" + account + "/tokens"
}

func decodeTokens(t *testing.T, rec *httptest.ResponseRecorder) tokensResponse {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	var out tokensResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body is not a tokens response: %v\n%s", err, rec.Body.String())
	}
	return out
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int) problem {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body %s", rec.Code, wantStatus, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	var p problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body is not a problem document: %v", err)
	}
	return p
}

func TestAccountTokens(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t,
		pair(10, domain.StandardERC20, 26_000_000, 26_304_000),
		pair(20, domain.StandardERC721, 26_100_000, 26_200_000),
	)

	resp := decodeTokens(t, h.get(t, tokensPath(addr(1).Checksum())))

	if resp.Chain != testSlug || resp.ChainID != testChainID || resp.Standard != "erc20" {
		t.Errorf("response identity = %+v", resp)
	}
	// Addresses go out checksummed, so a client can round-trip what it is given.
	if resp.Account != addr(1).Checksum() {
		t.Errorf("account = %s, want the checksummed form %s", resp.Account, addr(1).Checksum())
	}
	// The assignment's requirement: the height these data are valid for.
	if resp.Block.Height != headBlock || resp.Block.Safe != safeBlock {
		t.Errorf("block = %+v, want height %d / safe %d", resp.Block, headBlock, safeBlock)
	}
	if len(resp.Tokens) != 1 {
		t.Fatalf("got %d tokens, want 1 erc20", len(resp.Tokens))
	}
	got := resp.Tokens[0]
	if got.Address != addr(10).Checksum() || got.Standard != "erc20" {
		t.Errorf("token = %+v", got)
	}
	if got.FirstBlock != 26_000_000 || got.LastBlock != 26_304_000 {
		t.Errorf("interaction range = %d..%d", got.FirstBlock, got.LastBlock)
	}
	// Balances are opt-in: the default response is pure discovery.
	if got.Balance != nil {
		t.Errorf("balance = %+v without ?balances=true", got.Balance)
	}
	if resp.NextCursor != "" {
		t.Errorf("nextCursor = %q on a complete page", resp.NextCursor)
	}
}

func TestUnconfirmedIsPerToken(t *testing.T) {
	// The head is above the safe watermark in all of these, so a page-level
	// answer could not tell them apart.
	tests := []struct {
		name        string
		first, last uint64
		want        bool
	}{
		{"entirely below safe", safeBlock - 10, safeBlock - 5, false},
		{"first transfer at safe", safeBlock, safeBlock, false},
		// Touched recently, but it would stay listed whatever that block's fate.
		{"held, then touched above safe", safeBlock - 10, headBlock, false},
		{"first transfer above safe", safeBlock + 1, headBlock, true},
	}

	h := newHarness(t, nil)
	var pairs []domain.AccountToken
	for i, tc := range tests {
		pairs = append(pairs,
			pair(uint64(10+i), domain.StandardERC20, tc.first, tc.last),
			pair(uint64(20+i), domain.StandardERC721, tc.first, tc.last),
		)
	}
	h.seed(t, pairs...)
	account := addr(1).Checksum()

	tokens := decodeTokens(t, h.get(t, tokensPath(account)))
	nfts := decodeNFTs(t, h.get(t, nftsPath(account)))
	if len(tokens.Tokens) != len(tests) || len(nfts.Collections) != len(tests) {
		t.Fatalf("got %d tokens and %d collections, want %d of each", len(tokens.Tokens), len(nfts.Collections), len(tests))
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tokens.Tokens[i]; got.Address != addr(uint64(10+i)).Checksum() || got.Unconfirmed != tc.want {
				t.Errorf("token = %+v, want unconfirmed %v", got, tc.want)
			}
			if got := nfts.Collections[i]; got.Address != addr(uint64(20+i)).Checksum() || got.Unconfirmed != tc.want {
				t.Errorf("collection = %+v, want unconfirmed %v", got, tc.want)
			}
		})
	}

	// The field is there when false too: "confirmed" has to be stated, and the
	// page no longer answers for its tokens.
	body := h.get(t, tokensPath(account)).Body.String()
	if !strings.Contains(body, `"unconfirmed":false`) {
		t.Errorf("a confirmed token should say so: %s", body)
	}
	if strings.Count(body, `"unconfirmed":`) != len(tests) {
		t.Errorf("want the flag once per token and nowhere else: %s", body)
	}
}

func TestDiscoveryETagChangesWhenATokenConfirms(t *testing.T) {
	// The flag clears once, with nothing else on the token changing. If the
	// validator ignored it, a polling client would get 304s and show the token
	// as unconfirmed for good.
	h := newHarness(t, nil)
	h.seed(t, pair(10, domain.StandardERC20, headBlock, headBlock))
	path := tokensPath(addr(1).Checksum())

	first := h.get(t, path)
	if !decodeTokens(t, first).Tokens[0].Unconfirmed {
		t.Fatal("a token first seen above safe is reported as confirmed")
	}

	// The cursor advances until the watermark passes the token's first block.
	h.seedNFTs(t, headBlock+1, safeBlock)

	second := h.get(t, path, [2]string{"If-None-Match", first.Header().Get("ETag")})
	if second.Code != http.StatusOK {
		t.Fatalf("after the token confirmed: status %d, want 200 with a new validator", second.Code)
	}
	if decodeTokens(t, second).Tokens[0].Unconfirmed {
		t.Error("still unconfirmed once the watermark passed its first block")
	}
}

func TestAccountTokensRejectsBadInput(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))
	account := addr(1).Checksum()

	tests := []struct {
		name     string
		path     string
		status   int
		wantKind string
	}{
		{
			// A single flipped case bit is the typo this exists for: without
			// the check the request would return a valid-looking empty list.
			name:     "bad checksum",
			path:     tokensPath(flipCase("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")),
			status:   http.StatusBadRequest,
			wantKind: "address-checksum",
		},
		{
			// All-lowercase carries no checksum information, so it must be
			// accepted rather than rejected as a bad checksum.
			name:   "all lowercase is accepted",
			path:   tokensPath("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"),
			status: http.StatusOK,
		},
		{"not an address", tokensPath("0xnothex"), http.StatusBadRequest, "invalid-address"},
		{"unknown chain", "/v1/chains/dogecoin/accounts/" + account + "/tokens", http.StatusNotFound, "unknown-chain"},
		{"limit not a number", tokensPath(account) + "?limit=lots", http.StatusBadRequest, "invalid-limit"},
		{"limit too large", tokensPath(account) + "?limit=100000", http.StatusBadRequest, "invalid-limit"},
		{"limit zero", tokensPath(account) + "?limit=0", http.StatusBadRequest, "invalid-limit"},
		{"cursor not an address", tokensPath(account) + "?cursor=nope", http.StatusBadRequest, "invalid-cursor"},
		{"balances not a bool", tokensPath(account) + "?balances=maybe", http.StatusBadRequest, "invalid-parameter"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantKind == "" {
				if rec := h.get(t, tc.path); rec.Code != tc.status {
					t.Fatalf("status = %d, want %d; body %s", rec.Code, tc.status, rec.Body.String())
				}
				return
			}
			p := decodeProblem(t, h.get(t, tc.path), tc.status)
			if p.Type != problemBase+tc.wantKind {
				t.Errorf("problem type = %s, want %s", p.Type, problemBase+tc.wantKind)
			}
			if p.RequestID == "" {
				t.Error("problem document is missing requestId")
			}
		})
	}
}

// flipCase inverts one hex letter's case, breaking an EIP-55 checksum.
func flipCase(checksummed string) string {
	for i := 2; i < len(checksummed); i++ {
		c := checksummed[i]
		switch {
		case c >= 'a' && c <= 'f':
			return checksummed[:i] + strings.ToUpper(string(c)) + checksummed[i+1:]
		case c >= 'A' && c <= 'F':
			return checksummed[:i] + strings.ToLower(string(c)) + checksummed[i+1:]
		}
	}
	return checksummed
}

func TestAccountTokensRefusesAnUnseededIndex(t *testing.T) {
	// An empty list from an unseeded index looks exactly like a real answer,
	// which is the one failure mode that genuinely misleads a user.
	h := newHarness(t, nil)
	p := decodeProblem(t, h.get(t, tokensPath(addr(1).Checksum())), http.StatusServiceUnavailable)
	if p.Type != problemBase+"not-seeded" {
		t.Errorf("problem type = %s, want not-seeded", p.Type)
	}
}

func TestAccountTokensPagination(t *testing.T) {
	h := newHarness(t, nil)
	var pairs []domain.AccountToken
	for i := uint64(1); i <= 5; i++ {
		pairs = append(pairs, pair(i*10, domain.StandardERC20, 1, 2))
	}
	h.seed(t, pairs...)

	seen := map[string]bool{}
	path := tokensPath(addr(1).Checksum()) + "?limit=2"
	for page := 0; ; page++ {
		if page > 5 {
			t.Fatal("pagination did not terminate")
		}
		resp := decodeTokens(t, h.get(t, path))
		for _, tok := range resp.Tokens {
			if seen[tok.Address] {
				t.Fatalf("token %s appeared on two pages", tok.Address)
			}
			seen[tok.Address] = true
		}
		if resp.NextCursor == "" {
			break
		}
		path = tokensPath(addr(1).Checksum()) + "?limit=2&cursor=" + resp.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("paged through %d tokens, want 5", len(seen))
	}
}

func TestEachEndpointServesItsOwnStandard(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t,
		pair(10, domain.StandardERC20, 1, 2),
		pair(20, domain.StandardERC721, 1, 2),
	)
	account := addr(1).Checksum()

	if resp := decodeTokens(t, h.get(t, tokensPath(account))); len(resp.Tokens) != 1 ||
		resp.Tokens[0].Address != addr(10).Checksum() {
		t.Errorf("/tokens returned %+v, want only the ERC-20", resp.Tokens)
	}
	if resp := decodeNFTs(t, h.get(t, nftsPath(account))); len(resp.Collections) != 1 ||
		resp.Collections[0].Address != addr(20).Checksum() {
		t.Errorf("/nfts returned %+v, want only the ERC-721", resp.Collections)
	}
}

func TestAccountTokensBalances(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t,
		pair(10, domain.StandardERC20, 1, headBlock),
		pair(20, domain.StandardERC20, 1, headBlock),
		pair(30, domain.StandardERC20, 1, headBlock),
		pair(40, domain.StandardERC20, 1, headBlock),
	)
	ctx := context.Background()

	// A uint256 that does not fit in a float64, which is why the wire format
	// is a decimal string.
	huge, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
	err := h.store.PutBalances(ctx, testChainID, addr(1), []domain.Balance{
		{Token: addr(10), Raw: huge, Block: headBlock},
		{Token: addr(20), Raw: big.NewInt(500), Block: headBlock},
		{Token: addr(30), Raw: big.NewInt(0), Block: headBlock},
	})
	if err != nil {
		t.Fatalf("PutBalances: %v", err)
	}
	// Token 20 is then touched by a later transfer, which invalidates it.
	h.seedAt(t, headBlock+1, pair(20, domain.StandardERC20, 1, headBlock+1))

	resp := decodeTokens(t, h.get(t, tokensPath(addr(1).Checksum())+"?balances=true"))
	byToken := map[string]*balanceEntry{}
	for _, tok := range resp.Tokens {
		byToken[tok.Address] = tok.Balance
	}

	// Clean: read at or after the last transfer, so provably current.
	if b := byToken[addr(10).Checksum()]; b == nil || b.Status != "ok" || b.Raw != huge.String() {
		t.Errorf("token 10 balance = %+v, want an exact uint256 marked ok", b)
	}
	// Dirty: a real value a later transfer invalidated. Served, flagged.
	if b := byToken[addr(20).Checksum()]; b == nil || b.Status != "stale" || b.Raw != "500" {
		t.Errorf("token 20 balance = %+v, want 500 marked stale", b)
	}
	// Zero is dropped by default: airdrop spam and reorged-out logs.
	if _, present := byToken[addr(30).Checksum()]; present {
		t.Error("a zero balance was returned without include_zero")
	}
	if resp.OmittedZeroBalances != 1 {
		t.Errorf("omittedZeroBalances = %d, want 1", resp.OmittedZeroBalances)
	}
	// Never read: the token is still reported, with no number invented for it.
	if b := byToken[addr(40).Checksum()]; b == nil || b.Status != "unavailable" || b.Raw != "" {
		t.Errorf("token 40 balance = %+v, want unavailable with no value", b)
	}

	// include_zero brings the zero row back rather than hiding information.
	resp = decodeTokens(t, h.get(t, tokensPath(addr(1).Checksum())+"?balances=true&include_zero=true"))
	if len(resp.Tokens) != 4 || resp.OmittedZeroBalances != 0 {
		t.Errorf("include_zero returned %d tokens (omitted %d), want 4 and 0",
			len(resp.Tokens), resp.OmittedZeroBalances)
	}
}

// seedAt applies a batch at a later height, advancing the cursor with it.
func (h *harness) seedAt(t *testing.T, block uint64, pairs ...domain.AccountToken) {
	t.Helper()
	cursor := domain.Cursor{
		SeededFromBlock: 25_933_634,
		HeadBlock:       block,
		HeadHash:        domain.Hash{0xAB, 0xCD},
		SafeBlock:       safeBlock,
		FinalizedBlock:  safeBlock - 32,
		ParserVersion:   1,
	}
	batch := domain.AccountTokenBatch{FromBlock: block, ToBlock: block, Pairs: pairs}
	if err := h.store.ApplyBatch(context.Background(), testChainID, batch, cursor); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}

func TestAccountTokensETag(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))
	path := tokensPath(addr(1).Checksum())

	first := h.get(t, path)
	etag := first.Header().Get("ETag")
	if etag == "" {
		t.Fatal("no ETag on a cacheable response")
	}
	if cc := first.Header().Get("Cache-Control"); !strings.Contains(cc, "no-cache") {
		t.Errorf("Cache-Control = %q; a page is only valid for the block it names", cc)
	}

	// A conditional repeat must not re-send the body.
	second := h.get(t, path, [2]string{"If-None-Match", etag})
	if second.Code != http.StatusNotModified {
		t.Fatalf("conditional request = %d, want 304", second.Code)
	}
	if second.Body.Len() != 0 {
		t.Errorf("304 carried a %d byte body", second.Body.Len())
	}
	if saved := testutil.ToFloat64(h.metrics.HTTPSavedBy304); saved <= 0 {
		t.Error("the 304 did not record the bytes it saved")
	}
	// The wildcard and a weak validator are both accepted per RFC 9110.
	if rec := h.get(t, path, [2]string{"If-None-Match", "*"}); rec.Code != http.StatusNotModified {
		t.Errorf("If-None-Match: * = %d, want 304", rec.Code)
	}
	if rec := h.get(t, path, [2]string{"If-None-Match", strings.TrimPrefix(etag, "W/")}); rec.Code != http.StatusNotModified {
		t.Errorf("strong form of the validator = %d, want 304", rec.Code)
	}
	if rec := h.get(t, path, [2]string{"If-None-Match", `"stale"`}); rec.Code != http.StatusOK {
		t.Errorf("non-matching validator = %d, want 200", rec.Code)
	}

	// Indexing a new token must change the validator, or a client would keep
	// a page that is now wrong.
	h.seedAt(t, headBlock+1, pair(50, domain.StandardERC20, 1, headBlock+1))
	third := h.get(t, path, [2]string{"If-None-Match", etag})
	if third.Code != http.StatusOK {
		t.Fatalf("after an index change the old validator still matched (%d)", third.Code)
	}
	if third.Header().Get("ETag") == etag {
		t.Error("ETag did not change after the index did")
	}
}

func TestAccountTokensAnswersWhileBehind(t *testing.T) {
	// Readiness gates the load balancer, not the handler. An instance that is
	// behind still answers — with an explicit account of how far behind — so a
	// client can decide for itself, and so "no tokens" never looks the same as
	// "not caught up".
	h := newHarness(t, stubReadiness{err: errors.New("indexed head 10 is 42 blocks behind the tip 52")})
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))

	resp := decodeTokens(t, h.get(t, tokensPath(addr(1).Checksum())))
	if resp.Freshness.Ready {
		t.Error("freshness.ready = true on a lagging instance")
	}
	if !strings.Contains(resp.Freshness.Reason, "behind") {
		t.Errorf("freshness.reason = %q, want the readiness error", resp.Freshness.Reason)
	}
	if resp.Freshness.LatestSeenBlock != headBlock {
		t.Errorf("latestSeenBlock = %d, want %d", resp.Freshness.LatestSeenBlock, headBlock)
	}
	if rec := h.get(t, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d, want 503 while behind", rec.Code)
	}
}

func TestStatusAndChains(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))

	t.Run("chains", func(t *testing.T) {
		rec := h.get(t, "/v1/chains")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		var body struct{ Chains []chainDescription }
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("body: %v", err)
		}
		if len(body.Chains) != 1 || body.Chains[0].Chain != testSlug {
			t.Fatalf("chains = %+v", body.Chains)
		}
		if got := body.Chains[0].Standards; len(got) != 2 {
			t.Errorf("standards = %v, want both", got)
		}
	})

	t.Run("status", func(t *testing.T) {
		rec := h.get(t, "/v1/status")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		var body struct {
			Version string
			Chains  []chainStatus
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("body: %v", err)
		}
		if body.Version != "test" || len(body.Chains) != 1 {
			t.Fatalf("status = %+v", body)
		}
		got := body.Chains[0]
		if got.IndexedHead != headBlock || got.SeededFromBlock != 25_933_634 || got.ParserVersion != 1 {
			t.Errorf("chain status = %+v", got)
		}
	})
}

func TestMetricsUseRoutePatternsNotAddresses(t *testing.T) {
	// An account address in a metric label would mint a time series per user:
	// 5,000 users would mean 5,000 series on one counter.
	h := newHarness(t, nil)
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))
	h.get(t, tokensPath(addr(1).Checksum()))
	h.get(t, tokensPath(addr(2).Checksum()))

	const route = "/v1/chains/{chain}/accounts/{address}/tokens"
	if got := testutil.ToFloat64(h.metrics.HTTPRequests.WithLabelValues(route, "200")); got != 2 {
		t.Errorf("requests on the route pattern = %v, want 2", got)
	}
	if got := testutil.ToFloat64(h.metrics.HTTPBodyBytes.WithLabelValues(route)); got <= 0 {
		t.Error("no body bytes recorded for the route")
	}

	text, err := h.metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range text {
		for _, metric := range mf.GetMetric() {
			for _, label := range metric.GetLabel() {
				if strings.HasPrefix(label.GetValue(), "0x") {
					t.Fatalf("metric %s carries an address label %q", mf.GetName(), label.GetValue())
				}
			}
		}
	}
}

func TestUnmatchedRoutesDoNotExplodeMetricCardinality(t *testing.T) {
	// Every 404 must land on a static label. The exact labels are chi's
	// business — an unmatched path at the root versus under /v1 — but the
	// number of them must not grow with the paths people probe.
	h := newHarness(t, nil)
	paths := []string{"/nope", "/also-nope", "/v1/nope", "/v1/chains/x/accounts"}
	for _, path := range paths {
		if rec := h.get(t, path); rec.Code != http.StatusNotFound {
			t.Fatalf("%s = %d, want 404", path, rec.Code)
		}
	}

	families, err := h.metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	routes := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != "td_http_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			var route, status string
			for _, l := range metric.GetLabel() {
				switch l.GetName() {
				case "route":
					route = l.GetValue()
				case "status":
					status = l.GetValue()
				}
			}
			if status == "404" {
				routes[route] += metric.GetCounter().GetValue()
			}
		}
	}

	var total float64
	for route, n := range routes {
		total += n
		if strings.Count(route, "/") > 4 || strings.Contains(route, "nope") {
			t.Errorf("route label %q looks like a caller-supplied path", route)
		}
	}
	if total != float64(len(paths)) {
		t.Errorf("counted %v 404s, want %d", total, len(paths))
	}
	if len(routes) > 2 {
		t.Errorf("404s spread over %d labels (%v); want a bounded set", len(routes), routes)
	}
}

func TestAccountTokensStandardNotIndexed(t *testing.T) {
	// A chain configured for ERC-20 only must say so rather than return an
	// empty ERC-721 list, which would read as "this account holds no NFTs".
	st, err := store.Open(filepath.Join(t.TempDir(), "store"), store.Options{CacheBytes: 1 << 20, MemTableBytes: 1 << 20})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	registry, err := chains.NewRegistry(fakeAdapter{info: domain.ChainInfo{
		Slug: testSlug, ChainID: testChainID, StartBlock: 1,
		Standards:  []domain.Standard{domain.StandardERC20},
		Watermarks: domain.WatermarkConsensusTags,
	}})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	m := obs.NewMetrics()
	h := &harness{
		handler: NewRouter(Deps{Gatherer: m.Registry, Metrics: m, Chains: registry, Store: st, Version: "test"}),
		store:   st,
		metrics: m,
	}
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))

	p := decodeProblem(t, h.get(t, nftsPath(addr(1).Checksum())), http.StatusBadRequest)
	if p.Type != problemBase+"standard-not-indexed" {
		t.Errorf("problem type = %s, want standard-not-indexed", p.Type)
	}
}

func TestUnknownAccountIsAnEmptyPageNotAnError(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))
	resp := decodeTokens(t, h.get(t, tokensPath(addr(99).Checksum())))
	if len(resp.Tokens) != 0 {
		t.Fatalf("got %d tokens for an unknown account", len(resp.Tokens))
	}
	// The block height still has to be there: "nothing, as of block N" is a
	// different statement from "nothing".
	if resp.Block.Height != headBlock {
		t.Errorf("block height = %d, want %d even on an empty page", resp.Block.Height, headBlock)
	}
	if resp.Tokens == nil {
		t.Error("tokens is null; an empty page must serialise as []")
	}
	_ = fmt.Sprint(resp)
}

// liveHarness is the API over a real store and a real Live resolver, with only
// the chain faked, so the assertions cover the request's whole RPC behaviour.
type liveChain struct {
	fakeAdapter
	mu        sync.Mutex
	reads     int
	metaReads int
	values    map[domain.Address]*big.Int
	meta      map[domain.Address]domain.TokenMeta
	down      bool
}

func (c *liveChain) Balances(_ context.Context, _ domain.Address, tokens []domain.TokenRef, atBlock uint64) ([]domain.Balance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	out := make([]domain.Balance, len(tokens))
	for i, t := range tokens {
		out[i] = domain.Balance{Token: t.Address, Err: "balance could not be read"}
		if !c.down {
			out[i] = domain.Balance{Token: t.Address, Raw: c.values[t.Address], Block: atBlock}
		}
	}
	if c.down {
		return out, errors.New("provider down")
	}
	return out, nil
}

func (c *liveChain) Metadata(_ context.Context, tokens []domain.TokenRef) ([]domain.TokenMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metaReads++
	out := make([]domain.TokenMeta, 0, len(tokens))
	for _, t := range tokens {
		m := c.meta[t.Address]
		m.Token, m.Standard = t.Address, t.Standard
		out = append(out, m)
	}
	return out, nil
}

func newLiveHarness(t *testing.T, chain *liveChain) *harness {
	t.Helper()
	h := newHarness(t, nil)
	chain.info = domain.ChainInfo{
		Slug: testSlug, ChainID: testChainID, StartBlock: 25_933_634,
		Standards:  []domain.Standard{domain.StandardERC20, domain.StandardERC721},
		Watermarks: domain.WatermarkConsensusTags,
	}
	registry, err := chains.NewRegistry(chain)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	live, err := balances.NewLive(h.store, registry, balances.LiveOptions{Budget: 2 * time.Second, Metrics: h.metrics})
	if err != nil {
		t.Fatalf("NewLive: %v", err)
	}
	h.handler = NewRouter(Deps{
		Registry: h.metrics.Registry,
		Gatherer: h.metrics.Registry,
		Metrics:  h.metrics,
		Indexer:  stubReadiness{},
		Chains:   registry,
		Store:    h.store,
		Balances: live,
		Metadata: live,
		Version:  "test",
	})
	return h
}

func TestLiveBalancesEndToEnd(t *testing.T) {
	usdc, nft := addr(10), addr(20)
	chain := &liveChain{
		values: map[domain.Address]*big.Int{usdc: big.NewInt(1_500_000), nft: big.NewInt(3)},
		meta: map[domain.Address]domain.TokenMeta{
			usdc: {Symbol: "USDC", Name: "USD Coin", Decimals: 6, Resolved: true},
			nft:  {Symbol: "APE", Name: "Apes", Resolved: true},
		},
	}
	h := newLiveHarness(t, chain)
	h.seed(t, pair(10, domain.StandardERC20, 1, headBlock), pair(20, domain.StandardERC721, 1, headBlock))

	// Plain discovery reaches no provider, whatever is or is not cached.
	plain := decodeTokens(t, h.get(t, tokensPath(addr(1).Checksum())))
	if chain.reads != 0 || chain.metaReads != 0 {
		t.Fatalf("discovery without balances spent RPC: %d reads, %d metadata", chain.reads, chain.metaReads)
	}
	if plain.Tokens[0].Balance != nil || plain.Tokens[0].Symbol != "" {
		t.Errorf("plain discovery = %+v", plain.Tokens[0])
	}

	// A cold page reads once, and answers in that same response.
	resp := decodeTokens(t, h.get(t, tokensPath(addr(1).Checksum())+"?balances=true"))
	if chain.reads != 1 || chain.metaReads != 1 {
		t.Errorf("cold page: %d reads, %d metadata; want 1 and 1", chain.reads, chain.metaReads)
	}
	got := resp.Tokens[0]
	if got.Balance == nil || got.Balance.Status != "ok" || got.Balance.Raw != "1500000" || got.Balance.Block != headBlock {
		t.Errorf("balance = %+v, want read live at the page's height", got.Balance)
	}
	if got.Symbol != "USDC" || got.Decimals == nil || *got.Decimals != 6 {
		t.Errorf("metadata = %+v", got)
	}

	// The second request is the steady state.
	resp = decodeTokens(t, h.get(t, tokensPath(addr(1).Checksum())+"?balances=true"))
	if chain.reads != 1 || chain.metaReads != 1 {
		t.Errorf("warm page spent RPC: %d reads, %d metadata", chain.reads, chain.metaReads)
	}
	if resp.Tokens[0].Balance.Status != "ok" {
		t.Errorf("warm balance = %+v", resp.Tokens[0].Balance)
	}

	// A transfer invalidates exactly the touched pair, and only that is re-read.
	chain.values[usdc] = big.NewInt(9)
	h.seedAt(t, headBlock+1, pair(10, domain.StandardERC20, 1, headBlock+1))
	resp = decodeTokens(t, h.get(t, tokensPath(addr(1).Checksum())+"?balances=true"))
	if chain.reads != 2 {
		t.Errorf("after a transfer: %d reads, want 2", chain.reads)
	}
	if b := resp.Tokens[0].Balance; b.Raw != "9" || b.Status != "ok" || b.Block != headBlock+1 {
		t.Errorf("refreshed balance = %+v", b)
	}

	// The NFT endpoint shares the whole path: live count, metadata, cache.
	nfts := decodeNFTs(t, h.get(t, nftsPath(addr(1).Checksum())+"?counts=true"))
	if got := nfts.Collections[0]; got.Symbol != "APE" || got.OwnedCount == nil ||
		got.OwnedCount.Count != "3" || got.OwnedCount.Status != "ok" {
		t.Errorf("collection = %+v, count %+v", got, got.OwnedCount)
	}
	readsBefore := chain.reads
	nfts = decodeNFTs(t, h.get(t, nftsPath(addr(1).Checksum())+"?counts=true"))
	if chain.reads != readsBefore {
		t.Errorf("a warm count spent RPC: %d reads, was %d", chain.reads, readsBefore)
	}
	if nfts.Collections[0].OwnedCount.Count != "3" {
		t.Errorf("warm count = %+v", nfts.Collections[0].OwnedCount)
	}
}

func TestStaleZeroIsNotHidden(t *testing.T) {
	// A zero that a later transfer invalidated is the exact state of an account
	// that has just received this token. With the provider down, dropping it as
	// "zero balance" would hide the funds.
	token := addr(10)
	chain := &liveChain{down: true, values: map[domain.Address]*big.Int{}}
	h := newLiveHarness(t, chain)
	h.seed(t, pair(10, domain.StandardERC20, 1, headBlock))
	err := h.store.PutBalances(context.Background(), testChainID, addr(1), []domain.Balance{
		{Token: token, Raw: big.NewInt(0), Block: headBlock},
	})
	if err != nil {
		t.Fatalf("PutBalances: %v", err)
	}
	h.seedAt(t, headBlock+1, pair(10, domain.StandardERC20, 1, headBlock+1))

	rec := h.get(t, tokensPath(addr(1).Checksum())+"?balances=true")
	if rec.Code != http.StatusOK {
		t.Fatalf("an RPC outage must not fail discovery: status %d", rec.Code)
	}
	resp := decodeTokens(t, rec)
	if len(resp.Tokens) != 1 || resp.OmittedZeroBalances != 0 {
		t.Fatalf("tokens = %+v, omitted %d; want the stale zero kept", resp.Tokens, resp.OmittedZeroBalances)
	}
	if b := resp.Tokens[0].Balance; b.Status != "stale" || b.Raw != "0" || b.Error == "" {
		t.Errorf("balance = %+v, want a flagged stale zero with a reason", b)
	}
}

func nftsPath(account string) string {
	return "/v1/chains/" + testSlug + "/accounts/" + account + "/nfts"
}

func decodeNFTs(t *testing.T, rec *httptest.ResponseRecorder) nftsResponse {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	var out nftsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

// seedNFTs applies mints to addr(1) in one batch, with the given block already
// final or not.
func (h *harness) seedNFTs(t *testing.T, block, finalized uint64, transfers ...domain.NFTTransfer) {
	t.Helper()
	cursor := domain.Cursor{
		SeededFromBlock: 25_933_634,
		HeadBlock:       block,
		HeadHash:        domain.Hash{0xAB, 0xCD},
		SafeBlock:       block,
		FinalizedBlock:  finalized,
		ParserVersion:   1,
	}
	batch := domain.AccountTokenBatch{FromBlock: block, ToBlock: block, NFTTransfers: transfers}
	if err := h.store.ApplyBatch(context.Background(), testChainID, batch, cursor); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}

func mintTo(token uint64, id *big.Int, block uint64, logIndex uint32) domain.NFTTransfer {
	var tokenID domain.Hash
	id.FillBytes(tokenID[:])
	return domain.NFTTransfer{Token: addr(token), TokenID: tokenID, To: addr(1), Block: block, LogIndex: logIndex}
}

func TestAccountNFTsKnownTokenIDs(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t, pair(20, domain.StandardERC721, 1, 2), pair(30, domain.StandardERC721, 1, 2))

	// A hash-style ID: it would not survive a JSON number.
	huge, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
	final := headBlock + 1
	h.seedNFTs(t, final, final, mintTo(20, big.NewInt(7), final, 0), mintTo(20, huge, final, 1))

	// Not final yet: the live count may see it, the ID list must not.
	pending := final + 1
	h.seedNFTs(t, pending, final, mintTo(20, big.NewInt(8), pending, 0))

	plain := decodeNFTs(t, h.get(t, nftsPath(addr(1).Checksum())))
	if plain.Collections[0].KnownTokenIDs != nil || plain.TokenIDsThroughBlock != 0 {
		t.Errorf("IDs without asking: %+v, through %d", plain.Collections[0], plain.TokenIDsThroughBlock)
	}
	rec := h.get(t, nftsPath(addr(1).Checksum()))
	if strings.Contains(rec.Body.String(), "knownTokenIds") {
		t.Errorf("the field must be absent unless asked for: %s", rec.Body)
	}

	resp := decodeNFTs(t, h.get(t, nftsPath(addr(1).Checksum())+"?token_ids=true"))
	if resp.TokenIDsThroughBlock != final {
		t.Errorf("through block = %d, want the finalized %d", resp.TokenIDsThroughBlock, final)
	}
	if resp.Block.Height != pending {
		t.Errorf("height = %d, want the head %d: the list trails it, the page does not", resp.Block.Height, pending)
	}
	got := resp.Collections[0].KnownTokenIDs
	if len(got) != 2 || got[0] != "7" || got[1] != huge.String() {
		t.Errorf("ids = %v, want 7 and the full uint256, in order", got)
	}
	// Asked, and none known, is an empty list rather than a missing field.
	rec = h.get(t, nftsPath(addr(1).Checksum())+"?token_ids=true")
	if !strings.Contains(rec.Body.String(), `"knownTokenIds":[]`) {
		t.Errorf("a collection with no known IDs should say so: %s", rec.Body)
	}
}

func TestAccountNFTsTruncatesLongLists(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t, pair(20, domain.StandardERC721, 1, 2))
	block := headBlock + 1
	mints := make([]domain.NFTTransfer, 0, maxKnownTokenIDs+1)
	for i := range maxKnownTokenIDs + 1 {
		mints = append(mints, mintTo(20, big.NewInt(int64(i)), block, uint32(i)))
	}
	h.seedNFTs(t, block, block, mints...)

	entry := decodeNFTs(t, h.get(t, nftsPath(addr(1).Checksum())+"?token_ids=true")).Collections[0]
	if len(entry.KnownTokenIDs) != maxKnownTokenIDs || !entry.TokenIDsTruncated {
		t.Errorf("listed %d, truncated %v; want %d and true", len(entry.KnownTokenIDs), entry.TokenIDsTruncated, maxKnownTokenIDs)
	}
}

func TestAccountNFTsZeroCountsAndBadInput(t *testing.T) {
	sold, held := addr(20), addr(30)
	chain := &liveChain{values: map[domain.Address]*big.Int{sold: big.NewInt(0), held: big.NewInt(2)}, meta: map[domain.Address]domain.TokenMeta{}}
	h := newLiveHarness(t, chain)
	h.seed(t, pair(20, domain.StandardERC721, 1, 2), pair(30, domain.StandardERC721, 1, 2))
	account := addr(1).Checksum()

	// Plain discovery, and the ID list, are local reads.
	decodeNFTs(t, h.get(t, nftsPath(account)+"?token_ids=true"))
	if chain.reads != 0 || chain.metaReads != 0 {
		t.Errorf("no-count requests spent RPC: %d reads, %d metadata", chain.reads, chain.metaReads)
	}

	resp := decodeNFTs(t, h.get(t, nftsPath(account)+"?counts=true"))
	if len(resp.Collections) != 1 || resp.Collections[0].Address != held.Checksum() || resp.OmittedEmpty != 1 {
		t.Errorf("collections = %+v, omitted %d; want only the held one", resp.Collections, resp.OmittedEmpty)
	}
	resp = decodeNFTs(t, h.get(t, nftsPath(account)+"?counts=true&include_zero=true"))
	if len(resp.Collections) != 2 || resp.OmittedEmpty != 0 {
		t.Errorf("include_zero: %+v, omitted %d", resp.Collections, resp.OmittedEmpty)
	}

	for _, q := range []string{"?token_ids=maybe", "?counts=maybe", "?limit=0", "?cursor=nope"} {
		if rec := h.get(t, nftsPath(account)+q); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, rec.Code)
		}
	}
}

func TestDiscoveryETagIgnoresTheHead(t *testing.T) {
	// A client polling once a minute always finds a newer head, so a validator
	// that included it could never produce a 304.
	h := newHarness(t, nil)
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))
	path := tokensPath(addr(1).Checksum())

	first := h.get(t, path)
	etag := first.Header().Get("ETag")
	if !strings.HasPrefix(etag, `W/"`) {
		t.Fatalf("ETag = %s, want a weak validator: the bodies differ, the meaning does not", etag)
	}
	if got := first.Header().Get("Cache-Control"); !strings.Contains(got, "private") {
		t.Errorf("Cache-Control = %q: per-account data must not sit in a shared cache", got)
	}

	// Only the head moves.
	h.seedAt(t, headBlock+50)

	second := h.get(t, path, [2]string{"If-None-Match", etag})
	if second.Code != http.StatusNotModified {
		t.Fatalf("after the head alone moved: status %d, want 304", second.Code)
	}
	// The 304 still says how current it is.
	if got := second.Header().Get("X-Block-Number"); got != strconv.FormatUint(headBlock+50, 10) {
		t.Errorf("X-Block-Number on a 304 = %q, want the current head", got)
	}
	if second.Header().Get("X-Block-Hash") == "" {
		t.Error("no X-Block-Hash on a 304")
	}
}

func TestDiscoveryETagFollowsWhatAClientActsOn(t *testing.T) {
	account := addr(1)
	token := addr(10)
	ctx := context.Background()
	path := tokensPath(account.Checksum()) + "?balances=true"

	h := newHarness(t, nil)
	h.seed(t, pair(10, domain.StandardERC20, 1, 2))
	put := func(raw int64, block uint64) {
		t.Helper()
		if err := h.store.PutBalances(ctx, testChainID, account, []domain.Balance{{Token: token, Raw: big.NewInt(raw), Block: block}}); err != nil {
			t.Fatalf("PutBalances: %v", err)
		}
	}
	put(5, headBlock)
	base := h.get(t, path).Header().Get("ETag")

	changed := func(step string) {
		t.Helper()
		rec := h.get(t, path, [2]string{"If-None-Match", base})
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d, want 200 with a new validator", step, rec.Code)
		}
		base = rec.Header().Get("ETag")
	}

	// A later balance read at the same value: only the read height moved.
	h.seedAt(t, headBlock+1)
	put(5, headBlock+1)
	if rec := h.get(t, path, [2]string{"If-None-Match", base}); rec.Code != http.StatusNotModified {
		t.Errorf("a re-read of the same value: status %d, want 304", rec.Code)
	}

	// A transfer: the range moves and the value is no longer trusted.
	h.seedAt(t, headBlock+2, pair(10, domain.StandardERC20, 1, headBlock+2))
	changed("a transfer")

	// The value is refreshed.
	put(9, headBlock+2)
	changed("a new balance")

	// Metadata resolves.
	if err := h.store.PutTokenMeta(ctx, testChainID, []domain.TokenMeta{{Token: token, Symbol: "NEW", Resolved: true}}); err != nil {
		t.Fatalf("PutTokenMeta: %v", err)
	}
	changed("resolved metadata")

	// A new token.
	h.seedAt(t, headBlock+3, pair(20, domain.StandardERC20, headBlock+3, headBlock+3))
	changed("a new token")
}

func TestNFTETagIgnoresTheViewHeight(t *testing.T) {
	h := newHarness(t, nil)
	h.seed(t, pair(20, domain.StandardERC721, 1, 2))
	path := nftsPath(addr(1).Checksum()) + "?token_ids=true"
	etag := h.get(t, path).Header().Get("ETag")

	h.seedNFTs(t, headBlock+1, headBlock+1)
	if rec := h.get(t, path, [2]string{"If-None-Match", etag}); rec.Code != http.StatusNotModified {
		t.Errorf("only the view height moved: status %d, want 304", rec.Code)
	}

	h.seedNFTs(t, headBlock+2, headBlock+2, mintTo(20, big.NewInt(7), headBlock+2, 0))
	if rec := h.get(t, path, [2]string{"If-None-Match", etag}); rec.Code != http.StatusOK {
		t.Errorf("a new known token ID: status %d, want 200", rec.Code)
	}
}
