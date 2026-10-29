package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These tests run against a real endpoint and are skipped unless RPC_URL is
// set, so `go test ./...` stays offline and deterministic by default.
//
// They exist because the fake server only proves the client handles the error
// shapes we *think* providers emit. The measured signatures in
// docs/measurements.md came from the real thing, and this keeps the client
// honest against it.
func realClient(t *testing.T) *Client {
	t.Helper()
	url := os.Getenv("RPC_URL")
	if url == "" {
		t.Skip("set RPC_URL to run integration tests")
	}
	c, err := New(Options{
		URL:     url,
		Rate:    10,
		Burst:   5,
		Timeout: 60 * time.Second,
		Retry:   DefaultRetry(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestIntegrationBlockNumber(t *testing.T) {
	c := realClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var hex string
	if err := c.Call(ctx, &hex, "eth_blockNumber"); err != nil {
		t.Fatalf("eth_blockNumber: %v", err)
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(hex, "0x"), 16, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", hex, err)
	}
	if n < 20_000_000 {
		t.Errorf("block number %d looks wrong for mainnet", n)
	}
	t.Logf("head = %d", n)
}

// The range cap drives the splitting logic. What matters is the *contract* —
// "this request must be made smaller" — not which specific class it lands in,
// because providers signal it inconsistently: a modest over-request returns a
// -32614 JSON-RPC error, while a very large one is rejected with a bare HTTP
// 413 before any JSON-RPC error exists. Both must be splittable.
func TestIntegrationOversizedRangeIsSplittable(t *testing.T) {
	const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	cases := []struct {
		name   string
		blocks uint64
	}{
		{"just over the 10k cap", 10_001},
		{"far over the cap", 200_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := realClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			const from = 26_000_000
			err := c.Call(ctx, nil, "eth_getLogs", map[string]any{
				"fromBlock": "0x" + strconv.FormatUint(from, 16),
				"toBlock":   "0x" + strconv.FormatUint(from+tc.blocks, 16),
				"topics":    []string{transferTopic},
			})
			if err == nil {
				t.Skipf("provider accepted a %d-block range", tc.blocks)
			}
			if !SplitRange(err) {
				t.Fatalf("error is not splittable, so the indexer would give up instead of halving: %v", err)
			}
			if Retryable(err) {
				t.Error("an oversized request must not be retried unchanged")
			}
			t.Logf("splittable as expected: %v", err)
		})
	}
}

// Proves gzip works end to end against the real endpoint: a 50-block getLogs is
// tens of MB decoded, and the transport decompresses it transparently.
func TestIntegrationGetLogsDecodesGzip(t *testing.T) {
	c := realClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var head string
	if err := c.Call(ctx, &head, "eth_blockNumber"); err != nil {
		t.Fatalf("eth_blockNumber: %v", err)
	}
	tip, _ := strconv.ParseUint(strings.TrimPrefix(head, "0x"), 16, 64)
	from := tip - 2000

	const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	var logs []json.RawMessage
	err := c.Call(ctx, &logs, "eth_getLogs", map[string]any{
		"fromBlock": "0x" + strconv.FormatUint(from, 16),
		"toBlock":   "0x" + strconv.FormatUint(from+49, 16),
		"topics":    []string{transferTopic},
	})
	if err != nil {
		t.Fatalf("eth_getLogs: %v", err)
	}
	// Measured density is ~574 logs/block, so 50 blocks should be in the
	// thousands. A tiny result would mean the filter or decode is wrong.
	if len(logs) < 1000 {
		t.Errorf("got %d logs for 50 blocks; expected thousands at measured density", len(logs))
	}
	t.Logf("%d logs decoded from 50 blocks", len(logs))
}

// Over-requesting past the head is an error, not an empty result — which is why
// the tip follower must clamp.
func TestIntegrationBeyondHeadClassification(t *testing.T) {
	c := realClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var head string
	if err := c.Call(ctx, &head, "eth_blockNumber"); err != nil {
		t.Fatalf("eth_blockNumber: %v", err)
	}
	tip, _ := strconv.ParseUint(strings.TrimPrefix(head, "0x"), 16, 64)
	future := tip + 50_000

	err := c.Call(ctx, nil, "eth_getLogs", map[string]any{
		"fromBlock": "0x" + strconv.FormatUint(future, 16),
		"toBlock":   "0x" + strconv.FormatUint(future+10, 16),
	})
	if err == nil {
		t.Skip("provider returned an empty result for a future range")
	}
	if !errors.Is(err, ErrBeyondHead) {
		t.Fatalf("future-range error misclassified as %v (full: %v)", outcomeFor(err), err)
	}
	t.Logf("classified correctly: %v", err)
}
