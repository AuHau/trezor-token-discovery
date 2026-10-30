package evm

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/obs"
	"tokendiscovery/internal/rpc"
)

// fakeChain plays Multicall3: it parses the real calldata the adapter produced
// and answers each inner call, so the encoder and decoder are exercised on both
// sides of the wire rather than against canned bytes.
type fakeChain struct {
	t *testing.T

	mu     sync.Mutex
	blocks []string         // the block parameter of each eth_call
	sizes  []int            // inner calls per eth_call, excluding getBlockNumber
	head   uint64           // what getBlockNumber reports
	pinned map[string]error // block parameter → whole-call failure

	balances map[domain.Address]*big.Int
	reverts  map[domain.Address]bool // inner call fails
	empty    map[domain.Address]bool // inner call succeeds with no output
	burners  map[domain.Address]bool // the whole batch fails while this is in it

	symbols  map[domain.Address]string
	decimals map[domain.Address]uint64
}

func newFakeChain(t *testing.T) *fakeChain {
	return &fakeChain{
		t:        t,
		head:     1000,
		pinned:   map[string]error{},
		balances: map[domain.Address]*big.Int{},
		reverts:  map[domain.Address]bool{},
		empty:    map[domain.Address]bool{},
		burners:  map[domain.Address]bool{},
		symbols:  map[domain.Address]string{},
		decimals: map[domain.Address]uint64{},
	}
}

func (f *fakeChain) Call(ctx context.Context, out any, method string, params ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if method != "eth_call" {
		return fmt.Errorf("fake: unexpected method %s", method)
	}
	tx := params[0].(map[string]any)
	block := params[1].(string)
	if tx["to"] != Multicall3Address.Hex() {
		f.t.Errorf("eth_call to %v, want the Multicall3 deployment", tx["to"])
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(tx["data"].(string), "0x"))
	if err != nil {
		return err
	}
	calls := parseCalls(f.t, raw)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocks = append(f.blocks, block)
	f.sizes = append(f.sizes, len(calls)-1)
	if err := f.pinned[block]; err != nil {
		return err
	}

	results := make([]mcResult, len(calls))
	for i, c := range calls {
		if f.burners[c.target] {
			return fmt.Errorf("execution: %w", rpc.ErrExecutionFailed)
		}
		results[i] = f.answer(c)
	}
	*(out.(*string)) = "0x" + hex.EncodeToString(encodeResults(results))
	return nil
}

func (f *fakeChain) answer(c mcCall) mcResult {
	switch {
	case c.target == Multicall3Address:
		return mcResult{success: true, ret: appendUint(nil, f.head)}
	case f.reverts[c.target]:
		return mcResult{}
	case f.empty[c.target]:
		return mcResult{success: true}
	}
	switch string(c.data[:4]) {
	case string(selBalanceOf):
		raw := f.balances[c.target]
		if raw == nil {
			raw = new(big.Int)
		}
		return mcResult{success: true, ret: raw.FillBytes(make([]byte, 32))}
	case string(selSymbol), string(selName):
		s, ok := f.symbols[c.target]
		if !ok {
			return mcResult{}
		}
		ret := appendUint(appendUint(nil, abiWord), uint64(len(s)))
		ret = append(ret, s...)
		return mcResult{success: true, ret: append(ret, make([]byte, paddedLen(len(s))-len(s))...)}
	case string(selDecimals):
		d, ok := f.decimals[c.target]
		if !ok {
			return mcResult{}
		}
		return mcResult{success: true, ret: appendUint(nil, d)}
	}
	f.t.Errorf("unexpected selector %x", c.data[:4])
	return mcResult{}
}

func (f *fakeChain) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.blocks)
}

// parseCalls decodes aggregate3 calldata with the same bounds-checked helpers.
func parseCalls(t *testing.T, raw []byte) []mcCall {
	t.Helper()
	if !strings.HasPrefix(string(raw), string(selAggregate3)) {
		t.Fatalf("calldata does not start with the aggregate3 selector: %x", raw[:4])
	}
	body := raw[4:]
	root, err := abiOffset(body, 0)
	if err != nil {
		t.Fatal(err)
	}
	n, err := abiUint(body, root)
	if err != nil {
		t.Fatal(err)
	}
	base := root + abiWord
	out := make([]mcCall, n)
	for i := range out {
		rel, err := abiOffset(body, base+uint64(i)*abiWord)
		if err != nil {
			t.Fatal(err)
		}
		tuple := base + rel
		copy(out[i].target[:], body[tuple+abiWord-domain.AddressLen:tuple+abiWord])
		if allow, _ := abiUint(body, tuple+abiWord); allow != 1 {
			t.Errorf("call %d does not set allowFailure", i)
		}
		dataRel, err := abiOffset(body, tuple+2*abiWord)
		if err != nil {
			t.Fatal(err)
		}
		if out[i].data, err = abiBytes(body, tuple+dataRel); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func readsAdapter(t *testing.T, f *fakeChain, chunk int, m *obs.Metrics) *Adapter {
	t.Helper()
	a, err := NewAdapter(f, Config{
		Chain:          testChainInfo(domain.WatermarkConsensusTags),
		MulticallChunk: chunk,
		Metrics:        m,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	return a
}

func tokenAddr(n int) domain.Address {
	var a domain.Address
	a[0] = 0xEE
	a[domain.AddressLen-2] = byte(n >> 8)
	a[domain.AddressLen-1] = byte(n)
	return a
}

func refs(n int) []domain.TokenRef {
	out := make([]domain.TokenRef, n)
	for i := range out {
		out[i] = domain.TokenRef{ChainID: 1, Address: tokenAddr(i), Standard: domain.StandardERC20}
	}
	return out
}

func TestBalancesPinsTheBlockAndReportsWhereItRan(t *testing.T) {
	f := newFakeChain(t)
	huge, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
	f.balances[tokenAddr(0)] = huge
	f.balances[tokenAddr(1)] = big.NewInt(5)
	f.head = 997 // the node says it ran below the pin; that must be reported, not hidden

	m := obs.NewMetrics()
	got, err := readsAdapter(t, f, 300, m).Balances(context.Background(), tokenAddr(99), refs(3), 1000)
	if err != nil {
		t.Fatalf("Balances: %v", err)
	}
	if f.blocks[0] != "0x3e8" {
		t.Errorf("pinned at %s, want 0x3e8: never the latest label", f.blocks[0])
	}
	if got[0].Raw.Cmp(huge) != 0 || got[1].Raw.Int64() != 5 || !got[2].IsZero() {
		t.Errorf("balances = %v, %v, %v", got[0].Raw, got[1].Raw, got[2].Raw)
	}
	for i, b := range got {
		if b.Token != tokenAddr(i) || b.Block != 997 {
			t.Errorf("balance %d = %+v, want its token at the executed block 997", i, b)
		}
	}
	if n := testutil.ToFloat64(m.MulticallCalls); n != 1 {
		t.Errorf("multicall batches = %v, want 1", n)
	}
	if n := testutil.ToFloat64(m.MulticallSlots); n != 3 {
		t.Errorf("multicall slots = %v, want 3", n)
	}
}

func TestBalancesChunks(t *testing.T) {
	f := newFakeChain(t)
	got, err := readsAdapter(t, f, 2, nil).Balances(context.Background(), tokenAddr(99), refs(5), 1000)
	if err != nil {
		t.Fatalf("Balances: %v", err)
	}
	if f.callCount() != 3 {
		t.Errorf("%d eth_calls for 5 tokens at chunk 2, want 3", f.callCount())
	}
	for i, b := range got {
		if b.Token != tokenAddr(i) || !b.OK() {
			t.Errorf("balance %d = %+v: results must stay in request order", i, b)
		}
	}
}

func TestBalancesPerTokenFailures(t *testing.T) {
	f := newFakeChain(t)
	f.balances[tokenAddr(0)] = big.NewInt(1)
	f.reverts[tokenAddr(1)] = true
	f.empty[tokenAddr(2)] = true

	got, err := readsAdapter(t, f, 300, nil).Balances(context.Background(), tokenAddr(99), refs(3), 1000)
	if err != nil {
		t.Fatalf("one bad contract must not fail the call: %v", err)
	}
	if !got[0].OK() {
		t.Errorf("healthy token = %+v", got[0])
	}
	if got[1].OK() || !got[1].Reverted {
		t.Errorf("reverting token = %+v", got[1])
	}
	// No code at the address answers with success and no bytes. That is not a
	// balance of zero.
	if got[2].OK() || got[2].IsZero() || !got[2].Reverted {
		t.Errorf("token with no return data = %+v, want a failure rather than zero", got[2])
	}
}

func TestBalancesFallsBackWhenThePinIsGone(t *testing.T) {
	f := newFakeChain(t)
	f.head = 2000
	f.pinned["0x3e8"] = fmt.Errorf("missing trie node: %w", rpc.ErrBeyondHead)

	got, err := readsAdapter(t, f, 300, nil).Balances(context.Background(), tokenAddr(99), refs(1), 1000)
	if err != nil {
		t.Fatalf("Balances: %v", err)
	}
	if strings.Join(f.blocks, ",") != "0x3e8,latest" {
		t.Errorf("blocks = %v, want the pin then latest", f.blocks)
	}
	if !got[0].OK() || got[0].Block != 2000 {
		t.Errorf("balance = %+v, want the height that actually executed", got[0])
	}
}

func TestBalancesIsolatesAGasBurner(t *testing.T) {
	// allowFailure cannot contain a contract that burns the batch's gas, so the
	// whole eth_call fails. Everything else on the page must still resolve.
	f := newFakeChain(t)
	f.burners[tokenAddr(5)] = true
	for i := range 8 {
		f.balances[tokenAddr(i)] = big.NewInt(int64(i + 1))
	}

	got, err := readsAdapter(t, f, 300, nil).Balances(context.Background(), tokenAddr(99), refs(8), 1000)
	if err != nil {
		t.Fatalf("Balances: %v", err)
	}
	for i, b := range got {
		if i == 5 {
			if b.OK() || !b.Reverted {
				t.Errorf("burner = %+v, want a per-token failure", b)
			}
			continue
		}
		if !b.OK() || b.Raw.Int64() != int64(i+1) {
			t.Errorf("token %d = %+v, want its balance", i, b)
		}
	}
	if n := f.callCount(); n > 8 {
		t.Errorf("%d calls to isolate one contract among 8, want about log2", n)
	}
}

func TestBalancesTransportFailureIsNotARevert(t *testing.T) {
	// The difference decides whether a token is left alone for minutes, so an
	// outage must not read as a broken contract.
	f := newFakeChain(t)
	f.pinned["0x3e8"] = fmt.Errorf("boom: %w", rpc.ErrUnavailable)

	got, err := readsAdapter(t, f, 2, nil).Balances(context.Background(), tokenAddr(99), refs(3), 1000)
	if !errors.Is(err, rpc.ErrUnavailable) {
		t.Fatalf("err = %v, want the cause surfaced", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d entries, want one per token even on failure", len(got))
	}
	for _, b := range got {
		if b.OK() || b.Reverted || b.Err == "" {
			t.Errorf("balance = %+v, want an unread entry that is not a revert", b)
		}
	}
}

func TestBalancesRejectsAMissingDeployment(t *testing.T) {
	f := &noCodeCaller{}
	a, err := NewAdapter(f, Config{Chain: testChainInfo(domain.WatermarkConsensusTags)})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	got, err := a.Balances(context.Background(), tokenAddr(99), refs(1), 1000)
	if err == nil {
		t.Fatal("an empty multicall result must be an error")
	}
	if got[0].OK() {
		t.Errorf("balance = %+v, want no value", got[0])
	}
}

type noCodeCaller struct{}

func (*noCodeCaller) Call(_ context.Context, out any, _ string, _ ...any) error {
	*(out.(*string)) = "0x"
	return nil
}

func TestMetadata(t *testing.T) {
	f := newFakeChain(t)
	erc20, noDecimals, silent, nft, burner := tokenAddr(0), tokenAddr(1), tokenAddr(2), tokenAddr(3), tokenAddr(4)
	f.symbols[erc20], f.decimals[erc20] = "USDC", 6
	f.symbols[noDecimals] = "ODD"
	f.symbols[nft] = "BAYC"
	f.burners[burner] = true

	in := []domain.TokenRef{
		{Address: erc20, Standard: domain.StandardERC20},
		{Address: noDecimals, Standard: domain.StandardERC20},
		{Address: silent, Standard: domain.StandardERC20},
		{Address: nft, Standard: domain.StandardERC721},
		{Address: burner, Standard: domain.StandardERC20},
	}
	got, err := readsAdapter(t, f, 300, nil).Metadata(context.Background(), in)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	by := map[domain.Address]domain.TokenMeta{}
	for _, m := range got {
		by[m.Token] = m
	}
	if m := by[erc20]; !m.Resolved || m.Symbol != "USDC" || m.Decimals != 6 || m.Standard != domain.StandardERC20 {
		t.Errorf("erc20 = %+v", m)
	}
	// Without decimals the amount cannot be displayed; 0 would be a wrong answer.
	if m := by[noDecimals]; m.Resolved {
		t.Errorf("an ERC-20 with no decimals() = %+v, want unresolved", m)
	}
	if m, ok := by[silent]; !ok || m.Resolved {
		t.Errorf("a contract with no metadata = %+v (present %v), want asked and unresolved", m, ok)
	}
	if m := by[nft]; !m.Resolved || m.Symbol != "BAYC" || m.Standard != domain.StandardERC721 {
		t.Errorf("erc721 = %+v, want resolved without decimals", m)
	}
	if m, ok := by[burner]; !ok || m.Resolved {
		t.Errorf("burner = %+v (present %v), want asked and unresolved", m, ok)
	}
}

func TestMetadataOutageIsNotAnAnswer(t *testing.T) {
	// A token missing from the result gets asked about again. Reporting it as
	// "no metadata" would be cached for the lifetime of the store.
	f := newFakeChain(t)
	f.pinned[tagLatest] = fmt.Errorf("boom: %w", rpc.ErrUnavailable)

	got, err := readsAdapter(t, f, 300, nil).Metadata(context.Background(), refs(2))
	if err == nil {
		t.Fatal("expected the outage to surface")
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want nothing recorded", got)
	}
}
