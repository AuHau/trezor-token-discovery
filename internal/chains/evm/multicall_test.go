package evm

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"golang.org/x/crypto/sha3"

	"tokendiscovery/internal/core/domain"
)

func TestSelectorsMatchKeccak(t *testing.T) {
	cases := []struct {
		sig string
		sel []byte
	}{
		{"aggregate3((address,bool,bytes)[])", selAggregate3},
		{"getBlockNumber()", selGetBlockNumber},
		{"balanceOf(address)", selBalanceOf},
		{"symbol()", selSymbol},
		{"name()", selName},
		{"decimals()", selDecimals},
	}
	for _, tc := range cases {
		h := sha3.NewLegacyKeccak256()
		h.Write([]byte(tc.sig))
		if want := h.Sum(nil)[:4]; !bytes.Equal(tc.sel, want) {
			t.Errorf("%s: selector %x, keccak says %x", tc.sig, tc.sel, want)
		}
	}
}

func TestEncodeAggregate3Layout(t *testing.T) {
	// Hand-derived from the ABI spec, because round-tripping through our own
	// decoder could not catch a misunderstanding of the format.
	target := domain.MustParseAddress("0x00000000000000000000000000000000000000aa")
	got := hex.EncodeToString(encodeAggregate3([]mcCall{{target: target, data: []byte{0xde, 0xad}}}))
	want := strings.Join([]string{
		"82ad56cb",
		word(0x20), // array offset
		word(1),    // array length
		word(0x20), // tuple 0 offset, relative to after the length
		word(0xaa), // target
		word(1),    // allowFailure
		word(0x60), // data offset within the tuple
		word(2),    // data length
		"dead" + strings.Repeat("00", 30),
	}, "")
	if got != want {
		t.Fatalf("calldata:\n got %s\nwant %s", got, want)
	}
}

func TestDecodeAggregate3(t *testing.T) {
	raw := encodeResults([]mcResult{
		{success: true, ret: []byte{1, 2, 3}},
		{success: false},
		{success: true, ret: bytes.Repeat([]byte{7}, 64)},
	})
	got, err := decodeAggregate3(raw, 3)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got[0].success || !bytes.Equal(got[0].ret, []byte{1, 2, 3}) {
		t.Errorf("result 0 = %+v", got[0])
	}
	if got[1].success || len(got[1].ret) != 0 {
		t.Errorf("result 1 = %+v", got[1])
	}
	if !got[2].success || len(got[2].ret) != 64 {
		t.Errorf("result 2 = %+v", got[2])
	}
	if _, err := decodeAggregate3(raw, 2); err == nil {
		t.Error("a result count that differs from the request must be an error")
	}
}

func TestDecodeAggregate3RejectsMalformed(t *testing.T) {
	// Each of these would index out of bounds in a decoder that trusts offsets.
	huge := strings.Repeat("ff", 32)
	cases := map[string]string{
		"empty":              "",
		"short":              "00",
		"root out of bounds": word(0x1000),
		"root huge":          huge,
		"length missing":     word(0x20),
		"tuple offset huge":  word(0x20) + word(1) + huge,
		"data offset huge":   word(0x20) + word(1) + word(0x20) + word(1) + huge,
		"data length huge":   word(0x20) + word(1) + word(0x20) + word(1) + word(0x40) + huge,
	}
	for name, h := range cases {
		raw, err := hex.DecodeString(h)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := decodeAggregate3(raw, 1); err == nil {
			t.Errorf("%s: decoded without error", name)
		}
	}
}

func TestABIString(t *testing.T) {
	dynamic := func(s string) []byte {
		out, _ := hex.DecodeString(word(0x20) + word(uint64(len(s))) + hex.EncodeToString([]byte(s)) +
			strings.Repeat("00", paddedLen(len(s))-len(s)))
		return out
	}
	bytes32 := make([]byte, 32)
	copy(bytes32, "MKR")

	cases := []struct {
		name string
		ret  []byte
		want string
	}{
		{"dynamic", dynamic("USDC"), "USDC"},
		{"legacy bytes32", bytes32, "MKR"},
		{"control characters stripped", dynamic("A\x00B\n"), "AB"},
		{"invalid utf-8 dropped", dynamic("\xff\xfe"), ""},
		{"empty return", nil, ""},
		{"truncated", dynamic("USDC")[:40], ""},
	}
	for _, tc := range cases {
		if got := abiString(tc.ret); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

// FuzzDecodeAggregate3 asserts that no provider output can panic the read path.
//
//	go test ./internal/chains/evm -run=Fuzz -fuzz=FuzzDecodeAggregate3 -fuzztime=60s
func FuzzDecodeAggregate3(f *testing.F) {
	f.Add(encodeResults([]mcResult{{success: true, ret: make([]byte, 32)}, {success: false}}), 2)
	f.Add([]byte{}, 0)
	f.Add(bytes.Repeat([]byte{0xff}, 96), 1)
	f.Fuzz(func(t *testing.T, data []byte, want int) {
		if want < 0 || want > 1024 {
			return
		}
		results, err := decodeAggregate3(data, want)
		if err == nil && len(results) != want {
			t.Fatalf("got %d results without error, want %d", len(results), want)
		}
		for _, r := range results {
			abiString(r.ret)
		}
	})
}

func word(v uint64) string {
	return hex.EncodeToString(appendUint(nil, v))
}

// encodeResults is the inverse of decodeAggregate3: what the contract returns.
func encodeResults(results []mcResult) []byte {
	out := appendUint(nil, abiWord)
	out = appendUint(out, uint64(len(results)))
	offset := uint64(len(results) * abiWord)
	for _, r := range results {
		out = appendUint(out, offset)
		offset += uint64(3*abiWord + paddedLen(len(r.ret)))
	}
	for _, r := range results {
		success := uint64(0)
		if r.success {
			success = 1
		}
		out = appendUint(out, success)
		out = appendUint(out, 2*abiWord)
		out = appendUint(out, uint64(len(r.ret)))
		out = append(out, r.ret...)
		out = append(out, make([]byte, paddedLen(len(r.ret))-len(r.ret))...)
	}
	return out
}
