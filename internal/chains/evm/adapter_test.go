package evm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/rpc"
)

// fakeCaller records the calls the adapter makes and replays canned results, so
// the request *shape* — which is what a provider charges for and rejects on —
// is asserted rather than assumed.
type fakeCaller struct {
	mu    sync.Mutex
	calls []call

	// headers answers eth_getBlockByNumber by number-or-tag.
	headers map[string]any
	// logs answers eth_getLogs.
	logs []Log
	// err, when set, fails every call.
	err error
}

type call struct {
	method string
	params []any
}

func (f *fakeCaller) Call(ctx context.Context, out any, method string, params ...any) error {
	f.mu.Lock()
	f.calls = append(f.calls, call{method: method, params: params})
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return err
	}

	var result any
	switch method {
	case "eth_getBlockByNumber":
		key, _ := params[0].(string)
		got, ok := f.headers[key]
		if !ok {
			// A node that does not have the block answers with null, which the
			// adapter must not read as block 0.
			result = nil
		} else {
			result = got
		}
	case "eth_getLogs":
		result = f.logs
	default:
		return fmt.Errorf("fake: unexpected method %s", method)
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (f *fakeCaller) methods() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.method)
	}
	return out
}

func (f *fakeCaller) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeCaller) lastParams() []any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return nil
	}
	return f.calls[len(f.calls)-1].params
}

func hdr(number uint64, hash, parent string, ts uint64) map[string]any {
	return map[string]any{
		"number":     hexUint64(number),
		"hash":       hash,
		"parentHash": parent,
		"timestamp":  hexUint64(ts),
	}
}

const (
	hashA = "0x1111111111111111111111111111111111111111111111111111111111111111"
	hashB = "0x2222222222222222222222222222222222222222222222222222222222222222"
	hashC = "0x3333333333333333333333333333333333333333333333333333333333333333"
)

func testChainInfo(strategy domain.WatermarkStrategy) domain.ChainInfo {
	return domain.ChainInfo{
		Slug:       "ethereum",
		ChainID:    1,
		StartBlock: 100,
		Standards:  []domain.Standard{domain.StandardERC20, domain.StandardERC721},
		Watermarks: strategy,
	}
}

func newTestAdapter(t *testing.T, f *fakeCaller, strategy domain.WatermarkStrategy) *Adapter {
	t.Helper()
	a, err := NewAdapter(f, Config{
		Chain:                 testChainInfo(strategy),
		WatermarkPollInterval: time.Minute,
		FinalityStallBlocks:   100,
		ConfirmationDepth:     12,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	return a
}

func TestHeaderDecodesAndRejectsNull(t *testing.T) {
	f := &fakeCaller{headers: map[string]any{
		hexUint64(500): hdr(500, hashA, hashB, 1_700_000_000),
	}}
	a := newTestAdapter(t, f, domain.WatermarkConsensusTags)
	ctx := context.Background()

	ref, err := a.Header(ctx, 500)
	if err != nil {
		t.Fatalf("Header: %v", err)
	}
	if ref.Number != 500 || ref.Hash != domain.MustParseHash(hashA) || ref.ParentHash != domain.MustParseHash(hashB) {
		t.Fatalf("header = %+v", ref)
	}
	if ref.Time != time.Unix(1_700_000_000, 0).UTC() {
		t.Errorf("timestamp = %s", ref.Time)
	}
	// Full transaction bodies would multiply the response size for data the
	// service never reads.
	if params := f.lastParams(); len(params) != 2 || params[1] != false {
		t.Errorf("params = %v, want the block ref plus false", params)
	}

	// A missing block comes back as JSON null. Reading that as a zero-valued
	// header would silently mean "block 0", so it must be an error — and one
	// classified as beyond-head, because the right response is to clamp.
	_, err = a.Header(ctx, 999)
	if err == nil {
		t.Fatal("a null header was accepted")
	}
	if !errors.Is(err, rpc.ErrBeyondHead) {
		t.Errorf("null header error = %v, want it to carry ErrBeyondHead", err)
	}
}

func TestWatermarksFromConsensusTags(t *testing.T) {
	f := &fakeCaller{headers: map[string]any{
		tagLatest:    hdr(500, hashA, hashB, 1_700_000_000),
		tagSafe:      hdr(496, hashB, hashC, 1_699_999_900),
		tagFinalized: hdr(488, hashC, hashA, 1_699_999_800),
	}}
	a := newTestAdapter(t, f, domain.WatermarkConsensusTags)
	ctx := context.Background()

	wm, err := a.Watermarks(ctx)
	if err != nil {
		t.Fatalf("Watermarks: %v", err)
	}
	if wm.Latest.Number != 500 || wm.Safe.Number != 496 || wm.Finalized.Number != 488 {
		t.Fatalf("watermarks = %d/%d/%d, want 500/496/488", wm.Latest.Number, wm.Safe.Number, wm.Finalized.Number)
	}
	if wm.FinalityLag() != 12 {
		t.Errorf("finality lag = %d, want 12", wm.FinalityLag())
	}
	if n := f.count(); n != 3 {
		t.Errorf("made %d calls, want 3", n)
	}

	// safe and finalized only move on epoch boundaries, so a second poll must
	// cost one call, not three. At a 4-second tip poll that is the difference
	// between 21k and 64k requests a day.
	if _, err := a.Watermarks(ctx); err != nil {
		t.Fatalf("Watermarks: %v", err)
	}
	if n := f.count(); n != 4 {
		t.Errorf("second poll took the call count to %d, want 4 (latest only)", n)
	}

	// Once the cache expires they are refreshed again.
	a.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := a.Watermarks(ctx); err != nil {
		t.Fatalf("Watermarks: %v", err)
	}
	if n := f.count(); n != 7 {
		t.Errorf("expired cache took the call count to %d, want 7", n)
	}
}

func TestWatermarksImmediateFinality(t *testing.T) {
	// On a BFT chain every accepted block is final, so asking for the tags
	// would be two wasted requests per poll.
	f := &fakeCaller{headers: map[string]any{tagLatest: hdr(500, hashA, hashB, 1_700_000_000)}}
	a := newTestAdapter(t, f, domain.WatermarkImmediateFinality)

	wm, err := a.Watermarks(context.Background())
	if err != nil {
		t.Fatalf("Watermarks: %v", err)
	}
	if wm.Safe != wm.Latest || wm.Finalized != wm.Latest {
		t.Errorf("watermarks = %+v, want all three at the head", wm)
	}
	if got := f.methods(); len(got) != 1 {
		t.Errorf("made %d calls, want 1", len(got))
	}
}

func TestWatermarksBlockCount(t *testing.T) {
	f := &fakeCaller{headers: map[string]any{tagLatest: hdr(500, hashA, hashB, 1_700_000_000)}}
	a := newTestAdapter(t, f, domain.WatermarkBlockCount)

	wm, err := a.Watermarks(context.Background())
	if err != nil {
		t.Fatalf("Watermarks: %v", err)
	}
	if wm.Safe.Number != 488 || wm.Finalized.Number != 488 {
		t.Fatalf("safe/finalized = %d/%d, want 488 (head minus the depth)", wm.Safe.Number, wm.Finalized.Number)
	}
	// A derived watermark has no hash of its own without another request, and
	// nothing consumes one: the reorg detector only compares at the head.
	if !wm.Safe.Hash.IsZero() {
		t.Error("a derived watermark carries a hash it could not have read")
	}
	if len(f.methods()) != 1 {
		t.Errorf("made %d calls, want 1", len(f.methods()))
	}
}

func TestWatermarksFallBackWhenFinalityStalls(t *testing.T) {
	// If the chain stops finalising, digests never freeze and checkpoints never
	// advance. The service switches to a confirmation depth instead of waiting
	// for a finality that is not coming.
	f := &fakeCaller{headers: map[string]any{
		tagLatest:    hdr(1000, hashA, hashB, 1_700_000_000),
		tagSafe:      hdr(300, hashB, hashC, 1_699_000_000),
		tagFinalized: hdr(300, hashC, hashA, 1_699_000_000),
	}}
	a := newTestAdapter(t, f, domain.WatermarkConsensusTags)

	wm, err := a.Watermarks(context.Background())
	if err != nil {
		t.Fatalf("Watermarks: %v", err)
	}
	if wm.Safe.Number != 988 || wm.Finalized.Number != 988 {
		t.Fatalf("stalled watermarks = %d/%d, want 988 (head minus the depth)", wm.Safe.Number, wm.Finalized.Number)
	}
}

func TestWatermarksFallBackWhenTagsAreUnsupported(t *testing.T) {
	// Pre-Merge nodes and some L2s do not implement the tags at all. The
	// strategy enum exists so such a chain still works, and the fallback has
	// to trigger on a *permanent* rejection rather than on any failure.
	f := &fakeCaller{headers: map[string]any{tagLatest: hdr(500, hashA, hashB, 1_700_000_000)}}
	a, err := NewAdapter(&permanentOnTag{inner: f, tag: tagSafe}, Config{
		Chain:                 testChainInfo(domain.WatermarkConsensusTags),
		WatermarkPollInterval: time.Minute,
		FinalityStallBlocks:   100,
		ConfirmationDepth:     12,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	wm, err := a.Watermarks(context.Background())
	if err != nil {
		t.Fatalf("Watermarks: %v", err)
	}
	if wm.Safe.Number != 488 || wm.Finalized.Number != 488 {
		t.Fatalf("watermarks = %d/%d, want the confirmation-depth fallback at 488", wm.Safe.Number, wm.Finalized.Number)
	}
}

// permanentOnTag fails one tag permanently, the way a node that does not
// implement it does.
type permanentOnTag struct {
	inner *fakeCaller
	tag   string
}

func (p *permanentOnTag) Call(ctx context.Context, out any, method string, params ...any) error {
	if method == "eth_getBlockByNumber" && len(params) > 0 && params[0] == p.tag {
		return fmt.Errorf("rpc %s: unknown block: %w", method, rpc.ErrPermanent)
	}
	return p.inner.Call(ctx, out, method, params...)
}

func TestWatermarksPropagatesTransientFailures(t *testing.T) {
	// A transient failure must not be mistaken for an unsupported tag: one is
	// retried, the other permanently changes strategy.
	f := &fakeCaller{headers: map[string]any{tagLatest: hdr(500, hashA, hashB, 1)}}
	a, err := NewAdapter(&permanentOnTag{inner: f, tag: "never"}, Config{
		Chain:                 testChainInfo(domain.WatermarkConsensusTags),
		WatermarkPollInterval: time.Minute,
		ConfirmationDepth:     12,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	f.err = fmt.Errorf("rpc: boom: %w", rpc.ErrUnavailable)
	if _, err := a.Watermarks(context.Background()); !errors.Is(err, rpc.ErrUnavailable) {
		t.Fatalf("Watermarks = %v, want the transient class preserved", err)
	}
}

func TestScanAccountTokensFilterShape(t *testing.T) {
	// One request, filtered on topic0 only, serves every account at once. An
	// address filter here would make the cost scale with the user count, which
	// is the whole thing the design avoids.
	f := &fakeCaller{logs: []Log{{
		Address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Topics: []string{
			TransferTopic0.Hex(),
			"0x0000000000000000000000005aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
			"0x000000000000000000000000d1220a0cf47c7b9be7a2e6ba89f429762e7b9adb",
		},
		Data:        "0x0000000000000000000000000000000000000000000000000000000005f5e100",
		BlockNumber: "0x1f4",
		BlockHash:   hashA,
		TxHash:      hashB,
		LogIndex:    "0x1",
	}}}
	a := newTestAdapter(t, f, domain.WatermarkConsensusTags)

	batch, err := a.ScanAccountTokens(context.Background(), 500, 699)
	if err != nil {
		t.Fatalf("ScanAccountTokens: %v", err)
	}
	if batch.FromBlock != 500 || batch.ToBlock != 699 || batch.LogCount != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	// Both legs of the transfer are interactions, so one log yields two pairs.
	if len(batch.Pairs) != 2 {
		t.Fatalf("got %d pairs, want 2", len(batch.Pairs))
	}
	if len(batch.Blocks) != 1 || batch.Blocks[0].Hash != domain.MustParseHash(hashA) {
		t.Errorf("block provenance = %+v", batch.Blocks)
	}

	params := f.lastParams()
	if len(params) != 1 {
		t.Fatalf("eth_getLogs took %d params, want 1 filter object", len(params))
	}
	filter, ok := params[0].(map[string]any)
	if !ok {
		t.Fatalf("filter is a %T", params[0])
	}
	if filter["fromBlock"] != "0x1f4" || filter["toBlock"] != "0x2bb" {
		t.Errorf("filter range = %v..%v, want 0x1f4..0x2bb", filter["fromBlock"], filter["toBlock"])
	}
	topics, ok := filter["topics"].([]string)
	if !ok || len(topics) != 1 || topics[0] != TransferTopic0.Hex() {
		t.Errorf("topics = %v, want only topic0", filter["topics"])
	}
	if _, present := filter["address"]; present {
		t.Error("the filter names an address; the scan must serve every account at once")
	}
}

func TestScanAccountTokensPreservesTheErrorClass(t *testing.T) {
	// The indexer halves a range only if the class survives, so wrapping must
	// not flatten it.
	f := &fakeCaller{err: fmt.Errorf("rpc eth_getLogs: too big: %w", rpc.ErrRangeTooLarge)}
	a := newTestAdapter(t, f, domain.WatermarkConsensusTags)

	_, err := a.ScanAccountTokens(context.Background(), 500, 10_500)
	if !rpc.SplitRange(err) {
		t.Fatalf("error = %v, want it to stay splittable", err)
	}

	if _, err := a.ScanAccountTokens(context.Background(), 10, 1); err == nil ||
		!strings.Contains(err.Error(), "invalid range") {
		t.Errorf("inverted range = %v, want it rejected before the request", err)
	}
}

func TestNewAdapterValidates(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"zero chain id", Config{Chain: domain.ChainInfo{Watermarks: domain.WatermarkConsensusTags, Standards: []domain.Standard{domain.StandardERC20}}}},
		{"unknown strategy", Config{Chain: domain.ChainInfo{ChainID: 1, Watermarks: "nope", Standards: []domain.Standard{domain.StandardERC20}}}},
		{"no standards", Config{Chain: domain.ChainInfo{ChainID: 1, Watermarks: domain.WatermarkConsensusTags}}},
	} {
		if _, err := NewAdapter(&fakeCaller{}, tc.cfg); err == nil {
			t.Errorf("%s was accepted", tc.name)
		}
	}
	if _, err := NewAdapter(nil, Config{Chain: testChainInfo(domain.WatermarkConsensusTags)}); err == nil {
		t.Error("a nil caller was accepted")
	}
}
