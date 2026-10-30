package evm

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/crypto/sha3"

	"tokendiscovery/internal/core/domain"
)

var update = flag.Bool("update", false, "rewrite golden files")

func allStandards() []domain.Standard {
	return []domain.Standard{domain.StandardERC20, domain.StandardERC721}
}

// The topic0 literal is the foundation of the whole design, so derive it from
// the signature rather than trusting a pasted constant.
func TestTransferTopic0MatchesKeccak(t *testing.T) {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte("Transfer(address,address,uint256)"))
	want := "0x" + hex.EncodeToString(h.Sum(nil))
	if got := TransferTopic0.Hex(); got != want {
		t.Fatalf("TransferTopic0 = %s, keccak256 of the signature = %s", got, want)
	}
}

func TestShapeClassification(t *testing.T) {
	tests := []struct {
		name    string
		topics  int
		data    string
		want    domain.Standard
		wantErr bool
	}{
		{name: "erc20: 3 topics, 32B value", topics: 3, data: "0x" + rep("00", 32), want: domain.StandardERC20},
		{name: "erc721: 4 topics, empty data", topics: 4, data: "0x", want: domain.StandardERC721},
		{name: "erc721: provider omitted data entirely", topics: 4, data: "", want: domain.StandardERC721},
		{name: "4 topics with data", topics: 4, data: "0x" + rep("11", 32), want: domain.StandardUnknown},
		{name: "3 topics, 31B data", topics: 3, data: "0x" + rep("22", 31), want: domain.StandardUnknown},
		{name: "3 topics, no data", topics: 3, data: "0x", want: domain.StandardUnknown},
		{name: "2 topics", topics: 2, data: "0x" + rep("00", 32), want: domain.StandardUnknown},
		// The measured residue: Transfer emitted with nothing indexed.
		{name: "1 topic, 96B data", topics: 1, data: "0x" + rep("33", 96), want: domain.StandardUnknown},
		// Corrupt hex is a provider fault, not an unrecognised contract, and
		// must be reported separately so the benign case does not hide it.
		{name: "odd-length data is malformed", topics: 3, data: "0xabc", wantErr: true},
		{name: "non-hex-prefixed data is malformed", topics: 3, data: "garbage", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := &Log{Data: tc.data, Topics: make([]string, tc.topics)}
			got, err := shapeOf(l)
			if (err != nil) != tc.wantErr {
				t.Fatalf("shapeOf err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Fatalf("shapeOf = %v, want %v", got, tc.want)
			}
		})
	}
}

type mainnetFixture struct {
	Block uint64 `json:"block"`
	Logs  []Log  `json:"logs"`
}

func loadMainnetFixture(t *testing.T) mainnetFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "logs_mainnet_block.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f mainnetFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	return f
}

// Golden test over a real captured mainnet block. The digest is the thing worth
// pinning: it is what two independent runs must agree on, so a change here means
// the decoding semantics changed and ParserVersion must be bumped.
func TestDecodeMainnetBlockGolden(t *testing.T) {
	f := loadMainnetFixture(t)
	batch, err := NewDecoder(allStandards()).Decode(f.Block, f.Block, f.Logs)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if batch.LogCount != len(f.Logs) {
		t.Errorf("LogCount = %d, want %d", batch.LogCount, len(f.Logs))
	}
	if len(batch.Blocks) != 1 {
		t.Fatalf("got %d block digests, want 1", len(batch.Blocks))
	}

	var erc20, erc721 int
	for _, p := range batch.Pairs {
		switch p.Standard {
		case domain.StandardERC20:
			erc20++
		case domain.StandardERC721:
			erc721++
		}
	}

	got := map[string]any{
		"block":         f.Block,
		"logs":          batch.LogCount,
		"pairs":         len(batch.Pairs),
		"erc20_pairs":   erc20,
		"erc721_pairs":  erc721,
		"nft_transfers": len(batch.NFTTransfers),
		"zero_skipped":  batch.Skipped.ZeroAddress,
		"unknown_shape": batch.Skipped.UnknownShape,
		"malformed":     batch.Skipped.Malformed,
		"digest":        batch.Blocks[0].Digest.Hex(),
	}

	goldenPath := filepath.Join("..", "..", "..", "testdata", "logs_mainnet_block.golden.json")
	if *update {
		out, _ := json.MarshalIndent(got, "", "  ")
		if err := os.WriteFile(goldenPath, append(out, '\n'), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("golden updated: %s", out)
		return
	}

	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	var want map[string]any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("parse golden: %v", err)
	}

	// Round-trip the decoded values through JSON so numeric types match the
	// golden file's (encoding/json yields float64 for every number).
	normalised, _ := json.Marshal(got)
	var gotNorm map[string]any
	if err := json.Unmarshal(normalised, &gotNorm); err != nil {
		t.Fatalf("normalise: %v", err)
	}
	if !reflect.DeepEqual(gotNorm, want) {
		t.Errorf("decode output differs from golden.\n got: %v\nwant: %v\n"+
			"If the decoding semantics changed on purpose, bump ParserVersion "+
			"and re-run with -update.", gotNorm, want)
	}
}

// Determinism is what makes cross-run digest comparison meaningful. A provider
// is free to return logs in any order, so the digest must not depend on it.
func TestDigestIsStableUnderLogReordering(t *testing.T) {
	f := loadMainnetFixture(t)
	d := NewDecoder(allStandards())

	first, err := d.Decode(f.Block, f.Block, f.Logs)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	shuffled := make([]Log, len(f.Logs))
	copy(shuffled, f.Logs)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	second, err := d.Decode(f.Block, f.Block, shuffled)
	if err != nil {
		t.Fatalf("Decode shuffled: %v", err)
	}
	if first.Blocks[0].Digest != second.Blocks[0].Digest {
		t.Error("digest changed when logs were reordered; cross-run comparison would be useless")
	}
	if len(first.Pairs) != len(second.Pairs) {
		t.Errorf("pair count changed under reordering: %d vs %d", len(first.Pairs), len(second.Pairs))
	}
	for i := range first.Pairs {
		if first.Pairs[i] != second.Pairs[i] {
			t.Fatalf("pair order is not canonical at index %d", i)
		}
	}
}

// The digest exists to catch a provider that returns an incomplete block. If
// dropping a log left the digest unchanged, the consistency check would be
// decorative.
func TestDigestDetectsADroppedLog(t *testing.T) {
	f := loadMainnetFixture(t)
	d := NewDecoder(allStandards())

	full, _ := d.Decode(f.Block, f.Block, f.Logs)
	truncated, _ := d.Decode(f.Block, f.Block, f.Logs[:len(f.Logs)-1])

	if full.Blocks[0].Digest == truncated.Blocks[0].Digest {
		t.Error("digest did not change when a log was dropped")
	}
}

type edgeFixture struct {
	FromBlock uint64 `json:"fromBlock"`
	ToBlock   uint64 `json:"toBlock"`
	Cases     []struct {
		Why string `json:"why"`
		Log Log    `json:"log"`
	} `json:"cases"`
}

func loadEdgeFixture(t *testing.T) edgeFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "logs_edge_cases.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f edgeFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	return f
}

// One malformed log must never cost a whole 200-block range, so the decoder
// isolates faults per log and counts them instead of returning an error.
func TestDecodeEdgeCases(t *testing.T) {
	f := loadEdgeFixture(t)
	logs := make([]Log, 0, len(f.Cases))
	for _, c := range f.Cases {
		logs = append(logs, c.Log)
	}

	batch, err := NewDecoder(allStandards()).Decode(f.FromBlock, f.ToBlock, logs)
	if err != nil {
		t.Fatalf("Decode must not fail on malformed input: %v", err)
	}

	alice := domain.MustParseAddress("0x00000000000000000000000000000000000a11ce")
	bob := domain.MustParseAddress("0x0000000000000000000000000000000000000b0b")
	carol := domain.MustParseAddress("0x00000000000000000000000000000000000ca401")
	tok20 := domain.MustParseAddress("0x00000000000000000000000000000000dec0de20")
	tok721 := domain.MustParseAddress("0x0000000000000000000000000000000dec0de721")

	want := map[domain.AccountToken]bool{
		// plain transfer (block 100) and the duplicate in block 101 widen the
		// alice/bob range rather than producing two rows
		{Account: alice, Token: tok20, Standard: domain.StandardERC20, FirstBlock: 100, LastBlock: 101}:   false,
		{Account: bob, Token: tok20, Standard: domain.StandardERC20, FirstBlock: 100, LastBlock: 101}:     false,
		{Account: carol, Token: tok721, Standard: domain.StandardERC721, FirstBlock: 100, LastBlock: 100}: false,
		{Account: bob, Token: tok721, Standard: domain.StandardERC721, FirstBlock: 100, LastBlock: 100}:   false,
	}
	for _, p := range batch.Pairs {
		if _, ok := want[p]; !ok {
			t.Errorf("unexpected pair decoded: %+v", p)
			continue
		}
		want[p] = true
	}
	for p, seen := range want {
		if !seen {
			t.Errorf("expected pair not decoded: account=%s token=%s std=%s first=%d last=%d",
				p.Account, p.Token, p.Standard, p.FirstBlock, p.LastBlock)
		}
	}

	// The zero address is never an account: one mint leg + one burn leg.
	if batch.Skipped.ZeroAddress != 2 {
		t.Errorf("ZeroAddress skips = %d, want 2", batch.Skipped.ZeroAddress)
	}
	// ERC-1155 TransferSingle has a different signature entirely.
	if batch.Skipped.WrongTopic != 1 {
		t.Errorf("WrongTopic skips = %d, want 1 (the ERC-1155 TransferSingle)", batch.Skipped.WrongTopic)
	}
	if batch.Skipped.RemovedFlag != 1 {
		t.Errorf("RemovedFlag skips = %d, want 1", batch.Skipped.RemovedFlag)
	}
	if batch.Skipped.OutsideRange != 1 {
		t.Errorf("OutsideRange skips = %d, want 1", batch.Skipped.OutsideRange)
	}
	// unindexed Transfer, 4-topics-with-data, 2 topics, 31-byte data, no topics
	if batch.Skipped.UnknownShape != 5 {
		t.Errorf("UnknownShape skips = %d, want 5", batch.Skipped.UnknownShape)
	}
	// bad blockNumber, bad address, bad topic, odd-length data
	if batch.Skipped.Malformed != 4 {
		t.Errorf("Malformed skips = %d, want 4", batch.Skipped.Malformed)
	}

	// The ERC-721 tokenId and log index must survive decoding, since ownership
	// is "last transfer wins" and needs intra-block ordering.
	if len(batch.NFTTransfers) != 1 {
		t.Fatalf("NFTTransfers = %d, want 1", len(batch.NFTTransfers))
	}
	nft := batch.NFTTransfers[0]
	if nft.To != carol {
		t.Errorf("NFT new owner = %s, want carol", nft.To)
	}
	if nft.TokenID != domain.MustParseHash("0x000000000000000000000000000000000000000000000000000000000000beef") {
		t.Errorf("tokenId = %s, want 0x…beef", nft.TokenID)
	}
	if nft.LogIndex != 7 {
		t.Errorf("logIndex = %d, want 7", nft.LogIndex)
	}
}

func TestDecodeRespectsEnabledStandards(t *testing.T) {
	f := loadMainnetFixture(t)

	only20, err := NewDecoder([]domain.Standard{domain.StandardERC20}).Decode(f.Block, f.Block, f.Logs)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for _, p := range only20.Pairs {
		if p.Standard != domain.StandardERC20 {
			t.Fatalf("ERC-20-only decoder emitted %s", p.Standard)
		}
	}
	if len(only20.NFTTransfers) != 0 {
		t.Errorf("ERC-20-only decoder emitted %d NFT transfers", len(only20.NFTTransfers))
	}

	only721, err := NewDecoder([]domain.Standard{domain.StandardERC721}).Decode(f.Block, f.Block, f.Logs)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for _, p := range only721.Pairs {
		if p.Standard != domain.StandardERC721 {
			t.Fatalf("ERC-721-only decoder emitted %s", p.Standard)
		}
	}
	if len(only721.Pairs) == 0 {
		t.Error("expected some ERC-721 pairs in the fixture")
	}
}

func TestDecodeRejectsInvertedRange(t *testing.T) {
	if _, err := NewDecoder(allStandards()).Decode(10, 5, nil); err == nil {
		t.Fatal("expected an error for from > to")
	}
}

func TestDecodeEmptyInput(t *testing.T) {
	batch, err := NewDecoder(allStandards()).Decode(100, 200, nil)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(batch.Pairs) != 0 || len(batch.Blocks) != 0 || batch.LogCount != 0 {
		t.Errorf("empty input should produce an empty batch, got %+v", batch)
	}
	if batch.FromBlock != 100 || batch.ToBlock != 200 {
		t.Errorf("range not preserved: %d..%d", batch.FromBlock, batch.ToBlock)
	}
}

func TestParseHexUint64(t *testing.T) {
	tests := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{"0x0", 0, false},
		{"0x1914c06", 26299398, false},
		{"0xff", 255, false},
		{"", 0, true},
		{"0x", 0, true},
		{"1914c06", 0, true},
		{"0xzz", 0, true},
	}
	for _, tc := range tests {
		got, err := parseHexUint64(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseHexUint64(%q) err = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("parseHexUint64(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func rep(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for range n {
		out = append(out, s...)
	}
	return string(out)
}
