package store

import (
	"math/big"
	"strings"
	"testing"

	"tokendiscovery/internal/core/domain"
)

func TestCursorRoundTrip(t *testing.T) {
	want := domain.Cursor{
		SeededFromBlock: 25_933_634,
		HeadBlock:       26_299_430,
		HeadHash:        domain.MustParseHash("0x11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff"),
		SafeBlock:       26_299_400,
		FinalizedBlock:  26_299_360,
		ParserVersion:   1,
	}
	got, err := decodeCursor(encodeCursor(want))
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if got != want {
		t.Fatalf("cursor round-trip = %+v, want %+v", got, want)
	}
}

func TestPairRangeRoundTrip(t *testing.T) {
	first, last, err := decodePairRange(encodePairRange(10, 4_000_000_000))
	if err != nil {
		t.Fatalf("decodePairRange: %v", err)
	}
	if first != 10 || last != 4_000_000_000 {
		t.Fatalf("pair range = %d..%d, want 10..4000000000", first, last)
	}
}

func TestBalanceRoundTrip(t *testing.T) {
	// A uint256 at full width: token balances routinely exceed 64 bits, so the
	// encoding must carry all 32 bytes.
	max256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

	for _, tc := range []struct {
		name string
		raw  *big.Int
	}{
		{"zero", big.NewInt(0)},
		{"small", big.NewInt(1)},
		{"uint256 max", max256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := encodeBalance(tc.raw, 100, 0)
			if err != nil {
				t.Fatalf("encodeBalance: %v", err)
			}
			rec, err := decodeBalance(b)
			if err != nil {
				t.Fatalf("decodeBalance: %v", err)
			}
			if !rec.value || rec.raw.Cmp(tc.raw) != 0 || rec.block != 100 || rec.dirtyAt != 0 {
				t.Fatalf("decoded %+v, want value %s at block 100, clean", rec, tc.raw)
			}
		})
	}

	// A bare invalidation stamp carries no value to serve.
	rec, err := decodeBalance(encodeDirty(500))
	if err != nil {
		t.Fatalf("decodeBalance(stamp): %v", err)
	}
	if rec.value || rec.dirtyAt != 500 {
		t.Fatalf("stamp decoded as %+v, want no value and dirtyAt 500", rec)
	}
}

func TestBalanceEncodeRejectsImpossibleValues(t *testing.T) {
	// A failed read has no value and must never be cached; a negative balance
	// cannot survive a magnitude encoding, so both are caller bugs rather than
	// silent corruption.
	if _, err := encodeBalance(nil, 1, 0); err == nil {
		t.Error("encodeBalance(nil) did not fail")
	}
	if _, err := encodeBalance(big.NewInt(-1), 1, 0); err == nil {
		t.Error("encodeBalance(negative) did not fail")
	}
	tooWide := new(big.Int).Lsh(big.NewInt(1), 256)
	if _, err := encodeBalance(tooWide, 1, 0); err == nil {
		t.Error("encodeBalance(>uint256) did not fail")
	}
}

func TestMetaRoundTripAndTruncation(t *testing.T) {
	token := domain.MustParseAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	want := domain.TokenMeta{
		Token:    token,
		Standard: domain.StandardERC20,
		Symbol:   "USDC",
		Name:     "USD Coin",
		Decimals: 6,
		Resolved: true,
		Version:  3,
	}
	got, err := decodeMeta(token, encodeMeta(want))
	if err != nil {
		t.Fatalf("decodeMeta: %v", err)
	}
	if got != want {
		t.Fatalf("metadata round-trip = %+v, want %+v", got, want)
	}

	// Symbol and name come from an untrusted contract and are only displayed,
	// so an absurd one is truncated rather than stored or rejected.
	long := domain.TokenMeta{Token: token, Symbol: strings.Repeat("A", 10_000), Resolved: true}
	got, err = decodeMeta(token, encodeMeta(long))
	if err != nil {
		t.Fatalf("decodeMeta(long): %v", err)
	}
	if len(got.Symbol) != maxMetaString {
		t.Fatalf("symbol length = %d, want %d", len(got.Symbol), maxMetaString)
	}
}

func TestDigestRoundTrip(t *testing.T) {
	want := domain.BlockDigest{
		Number:   26_299_430,
		Hash:     domain.MustParseHash("0x11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff"),
		Digest:   domain.MustParseHash("0xffeeddccbbaa00998877665544332211ffeeddccbbaa00998877665544332211"),
		LogCount: 574,
	}
	got, err := decodeDigest(want.Number, encodeDigest(want))
	if err != nil {
		t.Fatalf("decodeDigest: %v", err)
	}
	if got != want {
		t.Fatalf("digest round-trip = %+v, want %+v", got, want)
	}
}

func TestDecodersRejectCorruptRecords(t *testing.T) {
	// A truncated or mistyped record means a corrupt store. Every decoder must
	// say so rather than panic or return plausible-looking zeroes.
	t.Run("cursor", func(t *testing.T) {
		if _, err := decodeCursor(nil); err == nil {
			t.Error("empty cursor record accepted")
		}
		bad := encodeCursor(domain.Cursor{})
		bad[0] = 0xAA
		if _, err := decodeCursor(bad); err == nil {
			t.Error("cursor record with wrong kind accepted")
		}
	})
	t.Run("pair", func(t *testing.T) {
		if _, _, err := decodePairRange([]byte{1, 2, 3}); err == nil {
			t.Error("short pair record accepted")
		}
	})
	t.Run("balance", func(t *testing.T) {
		for _, b := range [][]byte{nil, {kindBalance}, {kindDirty, 1}, {0xAA, 1, 2}} {
			if _, err := decodeBalance(b); err == nil {
				t.Errorf("corrupt balance record %x accepted", b)
			}
		}
		// A length byte that overruns the record must not slice out of range.
		bad, err := encodeBalance(big.NewInt(1), 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		bad[balanceHeaderLen-1] = 32
		if _, err := decodeBalance(bad); err == nil {
			t.Error("balance record with a lying length accepted")
		}
	})
	t.Run("metadata", func(t *testing.T) {
		token := domain.Address{}
		if _, err := decodeMeta(token, []byte{kindMeta}); err == nil {
			t.Error("short metadata record accepted")
		}
		bad := encodeMeta(domain.TokenMeta{Symbol: "AA"})
		bad = append(bad, 0xFF) // trailing garbage
		if _, err := decodeMeta(token, bad); err == nil {
			t.Error("metadata record with trailing bytes accepted")
		}
	})
	t.Run("digest", func(t *testing.T) {
		if _, err := decodeDigest(1, []byte{kindDigest}); err == nil {
			t.Error("short digest record accepted")
		}
	})
}
