// Package domain holds the chain-agnostic vocabulary of the service: accounts,
// tokens, the account-token pairs the index is built from, and the block
// watermarks the indexer tracks. Nothing here talks to a network or a database.
//
// Address and Hash are deliberately fixed-size arrays rather than byte slices:
// they are comparable, usable directly as map keys, and allocation-free, which
// matters when the backfill handles hundreds of millions of pairs. The cost is
// that they encode the 20-byte EVM address shape. A non-EVM chain with wider
// addresses (Solana and Sui both use 32 bytes) would need this to become an
// opaque variable-width type, which would ripple through the store key codec.
// That is an accepted trade for an EVM-only PoC; see docs/limits.md.
package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/sha3"
)

// AddressLen is the width of an EVM address in bytes.
const AddressLen = 20

// HashLen is the width of a block or transaction hash in bytes.
const HashLen = 32

// Address is a 20-byte account or contract address.
type Address [AddressLen]byte

// Hash is a 32-byte block, transaction or topic hash.
type Hash [HashLen]byte

var (
	// ErrBadAddressFormat means the input was not 0x-prefixed 20-byte hex.
	ErrBadAddressFormat = errors.New("address must be 0x followed by 40 hex characters")
	// ErrBadChecksum means a mixed-case input failed EIP-55 verification. This
	// is reported rather than ignored because a typo'd address would otherwise
	// return a perfectly valid empty token list — a confidently wrong answer.
	ErrBadChecksum = errors.New("address failed EIP-55 checksum verification")
)

// ParseAddress decodes a 0x-prefixed hex address.
//
// Input that is entirely lower- or upper-case carries no checksum information
// and is accepted as-is. Mixed-case input is treated as EIP-55 and verified.
func ParseAddress(s string) (Address, error) {
	var a Address
	body, ok := strings.CutPrefix(s, "0x")
	if !ok {
		body, ok = strings.CutPrefix(s, "0X")
		if !ok {
			return a, ErrBadAddressFormat
		}
	}
	if len(body) != AddressLen*2 {
		return a, ErrBadAddressFormat
	}
	if _, err := hex.Decode(a[:], []byte(strings.ToLower(body))); err != nil {
		return a, ErrBadAddressFormat
	}
	hasLower := body != strings.ToUpper(body)
	hasUpper := body != strings.ToLower(body)
	if hasLower && hasUpper && a.Checksum() != "0x"+body {
		return a, ErrBadChecksum
	}
	return a, nil
}

// MustParseAddress is ParseAddress for compile-time constants and tests.
func MustParseAddress(s string) Address {
	a, err := ParseAddress(s)
	if err != nil {
		panic(fmt.Sprintf("MustParseAddress(%q): %v", s, err))
	}
	return a
}

// AddressFromTopic takes the low 20 bytes of a 32-byte indexed log topic, which
// is how ERC-20 and ERC-721 encode the `from` and `to` parameters.
func AddressFromTopic(h Hash) Address {
	var a Address
	copy(a[:], h[HashLen-AddressLen:])
	return a
}

// Hex returns the lowercase 0x-prefixed form.
func (a Address) Hex() string { return "0x" + hex.EncodeToString(a[:]) }

// Checksum returns the EIP-55 mixed-case form, which is what the API emits.
func (a Address) Checksum() string {
	lower := hex.EncodeToString(a[:])
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(lower))
	sum := hex.EncodeToString(h.Sum(nil))

	out := []byte("0x" + lower)
	for i := 0; i < len(lower); i++ {
		if lower[i] >= 'a' && lower[i] <= 'f' && sum[i] >= '8' {
			out[2+i] = lower[i] - ('a' - 'A')
		}
	}
	return string(out)
}

// IsZero reports whether a is the zero address, which appears as the counterparty
// of every mint and burn and is never a real account.
func (a Address) IsZero() bool { return a == Address{} }

// String implements fmt.Stringer using the checksummed form.
func (a Address) String() string { return a.Checksum() }

// ParseHash decodes a 0x-prefixed 32-byte hex hash.
func ParseHash(s string) (Hash, error) {
	var h Hash
	body, ok := strings.CutPrefix(s, "0x")
	if !ok {
		return h, fmt.Errorf("hash must be 0x followed by %d hex characters", HashLen*2)
	}
	if len(body) != HashLen*2 {
		return h, fmt.Errorf("hash must be 0x followed by %d hex characters", HashLen*2)
	}
	if _, err := hex.Decode(h[:], []byte(strings.ToLower(body))); err != nil {
		return h, fmt.Errorf("hash is not valid hex: %w", err)
	}
	return h, nil
}

// MustParseHash is ParseHash for compile-time constants and tests.
func MustParseHash(s string) Hash {
	h, err := ParseHash(s)
	if err != nil {
		panic(fmt.Sprintf("MustParseHash(%q): %v", s, err))
	}
	return h
}

// Hex returns the lowercase 0x-prefixed form.
func (h Hash) Hex() string { return "0x" + hex.EncodeToString(h[:]) }

// String implements fmt.Stringer.
func (h Hash) String() string { return h.Hex() }

// IsZero reports whether h is the zero hash.
func (h Hash) IsZero() bool { return h == Hash{} }
