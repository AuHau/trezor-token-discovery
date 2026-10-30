package evm

import (
	"context"
	"encoding/hex"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/rpc"
)

// TestIntegrationBalances checks the whole read against a real endpoint and
// against a direct balanceOf at the same block, which is the only comparison
// that says the batching changed nothing. Skipped without RPC_URL.
func TestIntegrationBalances(t *testing.T) {
	url := os.Getenv("RPC_URL")
	if url == "" {
		t.Skip("set RPC_URL to run integration tests")
	}
	c, err := rpc.New(rpc.Options{URL: url, Rate: 10, Burst: 5, Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("rpc.New: %v", err)
	}
	a, err := NewAdapter(c, Config{Chain: testChainInfo(domain.WatermarkConsensusTags)})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var head string
	if err := c.Call(ctx, &head, "eth_blockNumber"); err != nil {
		t.Fatalf("eth_blockNumber: %v", err)
	}
	block, err := parseHexUint64(head)
	if err != nil {
		t.Fatalf("head %q: %v", head, err)
	}
	block -= 4 // far enough from the tip that every load-balanced node has it

	usdc := domain.MustParseAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	mkr := domain.MustParseAddress("0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2") // symbol() is a bytes32
	noCode := domain.MustParseAddress("0x000000000000000000000000000000000000dEaD")
	holder := domain.MustParseAddress("0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503")

	tokens := []domain.TokenRef{
		{ChainID: 1, Address: usdc, Standard: domain.StandardERC20},
		{ChainID: 1, Address: mkr, Standard: domain.StandardERC20},
		{ChainID: 1, Address: noCode, Standard: domain.StandardERC20},
	}
	got, err := a.Balances(ctx, holder, tokens, block)
	if err != nil {
		t.Fatalf("Balances: %v", err)
	}
	if got[0].Block != block {
		t.Errorf("executed at %d, pinned %d", got[0].Block, block)
	}

	var direct string
	err = c.Call(ctx, &direct, "eth_call", map[string]any{
		"to":   usdc.Hex(),
		"data": "0x" + hex.EncodeToString(encodeBalanceOf(holder)),
	}, hexUint64(block))
	if err != nil {
		t.Fatalf("direct balanceOf: %v", err)
	}
	want, ok := new(big.Int).SetString(strings.TrimPrefix(direct, "0x"), 16)
	if !ok {
		t.Fatalf("direct result %q", direct)
	}
	if !got[0].OK() || got[0].Raw.Cmp(want) != 0 {
		t.Errorf("multicall balance = %v, direct balanceOf = %v", got[0].Raw, want)
	}
	if got[2].OK() {
		t.Errorf("an address with no code = %+v, want a failure rather than zero", got[2])
	}
	t.Logf("USDC balance %s at block %d", got[0].Raw, got[0].Block)

	meta, err := a.Metadata(ctx, tokens[:2])
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if m := meta[0]; !m.Resolved || m.Symbol != "USDC" || m.Decimals != 6 {
		t.Errorf("USDC metadata = %+v", m)
	}
	if m := meta[1]; !m.Resolved || m.Symbol != "MKR" || m.Decimals != 18 {
		t.Errorf("MKR metadata (bytes32 symbol) = %+v", m)
	}
}
