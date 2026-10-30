package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"tokendiscovery/internal/core/domain"
)

func TestKeyWidthsAreFixed(t *testing.T) {
	// Fixed widths are what make the keyspace unambiguous without
	// length-prefixing, and what let pairToken slice the token back out.
	acct := domain.MustParseAddress("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed")
	tok := domain.MustParseAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

	for _, tc := range []struct {
		name string
		key  []byte
		want int
	}{
		{"cursor", cursorKey(1), chainKeyLen},
		{"account prefix", accountPrefix(1, acct), accountKeyLen},
		{"pair", pairKey(1, domain.AccountToken{Account: acct, Token: tok, Standard: domain.StandardERC20}), pairKeyLen},
		{"balance", balanceKey(1, acct, tok), balanceKeyLen},
		{"meta", metaKey(1, tok), metaKeyLen},
		{"digest", digestKey(1, 99), digestKeyLen},
	} {
		if got := len(tc.key); got != tc.want {
			t.Errorf("%s key is %d bytes, want %d", tc.name, got, tc.want)
		}
	}
}

func TestPairKeyRoundTrip(t *testing.T) {
	acct := domain.MustParseAddress("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed")
	tok := domain.MustParseAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	key := pairKey(7, domain.AccountToken{Account: acct, Token: tok, Standard: domain.StandardERC721})

	gotToken, ok := pairToken(key)
	if !ok || gotToken != tok {
		t.Fatalf("pairToken() = %s, %v, want %s", gotToken, ok, tok)
	}
	gotStd, ok := pairStandard(key)
	if !ok || gotStd != domain.StandardERC721 {
		t.Fatalf("pairStandard() = %v, %v, want erc721", gotStd, ok)
	}
	if !bytes.HasPrefix(key, accountPrefix(7, acct)) {
		t.Error("pair key does not start with its account prefix")
	}
	if got := binary.BigEndian.Uint64(key[1:9]); got != 7 {
		t.Errorf("chain id in key = %d, want 7", got)
	}
}

func TestKeyOrderIsScanOrder(t *testing.T) {
	acct := domain.MustParseAddress("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed")
	low := domain.MustParseAddress("0x0000000000000000000000000000000000000001")
	high := domain.MustParseAddress("0xffffffffffffffffffffffffffffffffffffffff")

	// Pages are in token-address order, so the key comparison must agree.
	a := pairKey(1, domain.AccountToken{Account: acct, Token: low, Standard: domain.StandardERC20})
	b := pairKey(1, domain.AccountToken{Account: acct, Token: high, Standard: domain.StandardERC20})
	if bytes.Compare(a, b) >= 0 {
		t.Error("pair keys do not sort by token address")
	}

	// The standard byte is last, so both rows for one token stay adjacent and
	// strictly inside the cursor boundary for that token.
	erc20 := pairKey(1, domain.AccountToken{Account: acct, Token: low, Standard: domain.StandardERC20})
	erc721 := pairKey(1, domain.AccountToken{Account: acct, Token: low, Standard: domain.StandardERC721})
	after := afterTokenKey(1, acct, low)
	if bytes.Compare(erc20, after) >= 0 || bytes.Compare(erc721, after) >= 0 {
		t.Error("afterTokenKey does not sort above every standard of its token")
	}
	if bytes.Compare(after, b) >= 0 {
		t.Error("afterTokenKey does not sort below the next token")
	}

	// Big-endian block numbers mean lexicographic order is numeric order,
	// which is what makes the rewind range delete correct.
	if bytes.Compare(digestKey(1, 9), digestKey(1, 10)) >= 0 {
		t.Error("digest keys do not sort numerically")
	}

	// Chains must not interleave: every chain-1 key sorts below chain 2.
	if bytes.Compare(pairKey(1, domain.AccountToken{Account: high, Token: high}), pairKey(2, domain.AccountToken{Account: low, Token: low})) >= 0 {
		t.Error("chain ranges interleave")
	}
}

func TestPrefixEnd(t *testing.T) {
	for _, tc := range []struct {
		in, want []byte
	}{
		{[]byte{'p', 0x00}, []byte{'p', 0x01}},
		{[]byte{'p', 0xFF}, []byte{'q'}},
		{[]byte{0xFF, 0xFF}, nil},
	} {
		got := prefixEnd(tc.in)
		if !bytes.Equal(got, tc.want) {
			t.Errorf("prefixEnd(%x) = %x, want %x", tc.in, got, tc.want)
		}
	}

	// The upper bound must exclude the next account but include every token of
	// this one.
	acct := domain.MustParseAddress("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed")
	prefix := accountPrefix(1, acct)
	end := prefixEnd(prefix)
	maxTok := domain.MustParseAddress("0xffffffffffffffffffffffffffffffffffffffff")
	last := pairKey(1, domain.AccountToken{Account: acct, Token: maxTok, Standard: domain.StandardERC721})
	if bytes.Compare(last, end) >= 0 {
		t.Error("account upper bound excludes a token of the same account")
	}
}

func TestSchemaKeySortsFirst(t *testing.T) {
	// The schema stamp must be reachable regardless of what else is stored,
	// and must never fall inside a per-chain range delete.
	for _, k := range [][]byte{cursorKey(0), chainPrefix(prefixBalance, 0), chainPrefix(prefixDigest, 0)} {
		if bytes.Compare(schemaKey, k) >= 0 {
			t.Errorf("schema key does not sort below %x", k)
		}
	}
}
