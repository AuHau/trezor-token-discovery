package evm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"tokendiscovery/internal/core/domain"
)

// FuzzDecode asserts the decoder's central robustness invariant: for ANY
// well-formed JSON array of logs, Decode must neither panic nor return an
// error. Provider output is untrusted — it crosses a network from a third party
// — and a panic in the indexer would halt it, while an error would discard a
// whole block range over one bad log.
//
// Run longer with:
//
//	go test ./internal/chains/evm -run=Fuzz -fuzz=FuzzDecode -fuzztime=60s
func FuzzDecode(f *testing.F) {
	// Seed from the real captured block and the hand-written edge cases, so
	// the fuzzer starts from structurally valid input and mutates outward.
	if raw, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "logs_mainnet_block.json")); err == nil {
		var fx mainnetFixture
		if json.Unmarshal(raw, &fx) == nil && len(fx.Logs) > 0 {
			if seed, err := json.Marshal(fx.Logs[:min(20, len(fx.Logs))]); err == nil {
				f.Add(seed)
			}
		}
	}
	if raw, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "logs_edge_cases.json")); err == nil {
		var fx edgeFixture
		if json.Unmarshal(raw, &fx) == nil {
			logs := make([]Log, 0, len(fx.Cases))
			for _, c := range fx.Cases {
				logs = append(logs, c.Log)
			}
			if seed, err := json.Marshal(logs); err == nil {
				f.Add(seed)
			}
		}
	}

	f.Add([]byte(`[]`))
	f.Add([]byte(`[{}]`))
	f.Add([]byte(`[{"topics":[]}]`))
	f.Add([]byte(`[{"topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}]`))
	f.Add([]byte(`[{"address":"0x","topics":["0x"],"data":"0x","blockNumber":"0x","blockHash":"0x"}]`))
	f.Add([]byte(`[{"blockNumber":"0xffffffffffffffffff"}]`))

	d := NewDecoder(allStandards())

	f.Fuzz(func(t *testing.T, data []byte) {
		var logs []Log
		if err := json.Unmarshal(data, &logs); err != nil {
			return // not our contract: the client guarantees well-formed JSON
		}

		// Full range, so no log is skipped merely for being out of bounds.
		batch, err := d.Decode(0, ^uint64(0), logs)
		if err != nil {
			t.Fatalf("Decode returned an error for well-formed JSON (%d logs): %v", len(logs), err)
		}

		// Every log must be accounted for exactly once: it either yielded
		// pairs or was counted as skipped. A log that vanishes silently is the
		// failure mode that produces missing tokens.
		if batch.LogCount != len(logs) {
			t.Fatalf("LogCount = %d, want %d", batch.LogCount, len(logs))
		}
		if batch.Skipped.Total() > len(logs) {
			t.Fatalf("skipped %d of %d logs", batch.Skipped.Total(), len(logs))
		}

		for _, p := range batch.Pairs {
			if p.Account.IsZero() {
				t.Fatal("decoded a pair for the zero address")
			}
			if !p.Standard.Valid() {
				t.Fatalf("decoded a pair with an invalid standard %v", p.Standard)
			}
			if p.FirstBlock > p.LastBlock {
				t.Fatalf("pair has FirstBlock %d > LastBlock %d", p.FirstBlock, p.LastBlock)
			}
		}
		for _, b := range batch.Blocks {
			if b.Digest == (domain.Hash{}) {
				t.Fatal("block digest is zero")
			}
		}
	})
}
