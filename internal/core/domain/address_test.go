package domain

import (
	"errors"
	"testing"
)

func TestChecksumVectors(t *testing.T) {
	// The canonical EIP-55 test vectors from the spec.
	for _, want := range []string{
		"0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
		"0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
		"0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
		"0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
	} {
		a, err := ParseAddress(want)
		if err != nil {
			t.Fatalf("ParseAddress(%s): %v", want, err)
		}
		if got := a.Checksum(); got != want {
			t.Errorf("Checksum() = %s, want %s", got, want)
		}
	}
}

func TestParseAddress(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  error
	}{
		// All-lowercase and all-uppercase carry no checksum information, so
		// they must be accepted rather than rejected as "bad checksum".
		{"all lowercase", "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed", nil},
		{"all uppercase body", "0x5AAEB6053F3E94C9B9A09F33669435E7EF1BEAED", nil},
		{"valid eip55", "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", nil},

		// A single flipped case bit is the typo this guard exists for: without
		// it the request would return a valid-looking empty token list.
		{"wrong checksum", "0x5AAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", ErrBadChecksum},

		{"no prefix", "5aaeb6053f3e94c9b9a09f33669435e7ef1beaed", ErrBadAddressFormat},
		{"too short", "0x5aaeb6053f3e94c9b9a09f33669435e7ef1bea", ErrBadAddressFormat},
		{"too long", "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed00", ErrBadAddressFormat},
		{"not hex", "0xzzaeb6053f3e94c9b9a09f33669435e7ef1beaed", ErrBadAddressFormat},
		{"empty", "", ErrBadAddressFormat},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseAddress(tc.input)
			if !errors.Is(err, tc.want) {
				t.Fatalf("ParseAddress(%q) error = %v, want %v", tc.input, err, tc.want)
			}
		})
	}
}

func TestAddressFromTopic(t *testing.T) {
	// Indexed address parameters are left-padded into a 32-byte topic.
	topic := MustParseHash("0x0000000000000000000000005aaeb6053f3e94c9b9a09f33669435e7ef1beaed")
	want := MustParseAddress("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed")
	if got := AddressFromTopic(topic); got != want {
		t.Fatalf("AddressFromTopic() = %s, want %s", got, want)
	}
}

func TestZeroAddressDetection(t *testing.T) {
	// The zero address is the counterparty of every mint and burn, so it must
	// be recognisable in order to be skipped rather than indexed as an account.
	zero := MustParseAddress("0x0000000000000000000000000000000000000000")
	if !zero.IsZero() {
		t.Error("zero address not detected")
	}
	if MustParseAddress("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed").IsZero() {
		t.Error("non-zero address reported as zero")
	}
}

func TestAddressRoundTrip(t *testing.T) {
	const lower = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" // USDC
	a := MustParseAddress(lower)
	if a.Hex() != lower {
		t.Errorf("Hex() = %s, want %s", a.Hex(), lower)
	}
	// Checksummed output must parse back to the same bytes.
	if MustParseAddress(a.Checksum()) != a {
		t.Error("checksummed form did not round-trip")
	}
}
