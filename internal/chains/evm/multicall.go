package evm

import (
	"encoding/binary"
	"errors"
	"fmt"

	"tokendiscovery/internal/core/domain"
)

// Multicall3Address is the canonical deployment, at the same address on
// Ethereum and most EVM chains.
var Multicall3Address = domain.MustParseAddress("0xcA11bde05977b3631167028862bE2a173976CA11")

// defaultMulticallChunk is how many calls go into one eth_call. Well inside the
// gas cap for ordinary balanceOf calls, and small enough that one bad batch
// does not cost much to split.
const defaultMulticallChunk = 300

// Four-byte function selectors. TestSelectorsMatchKeccak derives each from its
// signature rather than trusting the literal.
var (
	selAggregate3     = []byte{0x82, 0xad, 0x56, 0xcb} // aggregate3((address,bool,bytes)[])
	selGetBlockNumber = []byte{0x42, 0xcb, 0xb1, 0x5c} // getBlockNumber()
	selBalanceOf      = []byte{0x70, 0xa0, 0x82, 0x31} // balanceOf(address)
	selSymbol         = []byte{0x95, 0xd8, 0x9b, 0x41} // symbol()
	selName           = []byte{0x06, 0xfd, 0xde, 0x03} // name()
	selDecimals       = []byte{0x31, 0x3c, 0xe5, 0x67} // decimals()
)

const abiWord = 32

// mcCall is one call inside a Multicall3 batch.
type mcCall struct {
	target domain.Address
	data   []byte
}

// mcResult is one call's outcome.
type mcResult struct {
	success bool
	ret     []byte
}

// encodeBalanceOf is the calldata for balanceOf(account).
func encodeBalanceOf(account domain.Address) []byte {
	out := make([]byte, 0, len(selBalanceOf)+abiWord)
	out = append(out, selBalanceOf...)
	return appendAddress(out, account)
}

// encodeAggregate3 is the calldata for aggregate3 with allowFailure set on
// every call, so one reverting contract degrades to one failed slot instead of
// reverting the batch.
func encodeAggregate3(calls []mcCall) []byte {
	// Each tuple is address, bool, data offset, data length, then the data
	// padded to a word boundary.
	const tupleHead = 4 * abiWord
	size := len(selAggregate3) + 2*abiWord + len(calls)*abiWord
	for _, c := range calls {
		size += tupleHead + paddedLen(len(c.data))
	}

	out := make([]byte, 0, size)
	out = append(out, selAggregate3...)
	out = appendUint(out, abiWord) // offset of the array
	out = appendUint(out, uint64(len(calls)))

	// Tuple offsets are relative to the first word after the array length.
	offset := uint64(len(calls) * abiWord)
	for _, c := range calls {
		out = appendUint(out, offset)
		offset += uint64(tupleHead + paddedLen(len(c.data)))
	}
	for _, c := range calls {
		out = appendAddress(out, c.target)
		out = appendUint(out, 1)         // allowFailure
		out = appendUint(out, 3*abiWord) // offset of data within the tuple
		out = appendUint(out, uint64(len(c.data)))
		out = append(out, c.data...)
		out = append(out, make([]byte, paddedLen(len(c.data))-len(c.data))...)
	}
	return out
}

// decodeAggregate3 parses the (bool, bytes)[] that aggregate3 returns.
//
// The bytes come from a third party and are shaped by whatever contracts ran,
// so every offset is bounds-checked before use: a malformed response must be an
// error, never a panic in the read path. A result count that differs from the
// request is an error too, since results are matched to tokens by position.
func decodeAggregate3(data []byte, want int) ([]mcResult, error) {
	root, err := abiOffset(data, 0)
	if err != nil {
		return nil, fmt.Errorf("array offset: %w", err)
	}
	n, err := abiUint(data, root)
	if err != nil {
		return nil, fmt.Errorf("array length: %w", err)
	}
	if n != uint64(want) {
		return nil, fmt.Errorf("got %d results for %d calls", n, want)
	}

	base := root + abiWord
	out := make([]mcResult, want)
	for i := range out {
		rel, err := abiOffset(data, base+uint64(i)*abiWord)
		if err != nil {
			return nil, fmt.Errorf("result %d offset: %w", i, err)
		}
		tuple := base + rel
		success, err := abiUint(data, tuple)
		if err != nil {
			return nil, fmt.Errorf("result %d success: %w", i, err)
		}
		dataRel, err := abiOffset(data, tuple+abiWord)
		if err != nil {
			return nil, fmt.Errorf("result %d data offset: %w", i, err)
		}
		ret, err := abiBytes(data, tuple+dataRel)
		if err != nil {
			return nil, fmt.Errorf("result %d data: %w", i, err)
		}
		out[i] = mcResult{success: success != 0, ret: ret}
	}
	return out, nil
}

// abiString decodes a symbol() or name() return value.
//
// Besides the standard dynamic string it accepts a bare bytes32, which early
// tokens such as MKR return. The two cannot be confused: a dynamic string is at
// least two words long. The result ends up in API responses, so anything that
// is not clean text is dropped rather than passed along.
func abiString(ret []byte) string {
	var raw []byte
	if len(ret) == abiWord {
		raw = ret
		for len(raw) > 0 && raw[len(raw)-1] == 0 {
			raw = raw[:len(raw)-1]
		}
	} else {
		off, err := abiOffset(ret, 0)
		if err != nil {
			return ""
		}
		if raw, err = abiBytes(ret, off); err != nil {
			return ""
		}
	}
	return cleanText(raw)
}

// abiUint reads the word at off as a uint64, rejecting values that do not fit.
func abiUint(data []byte, off uint64) (uint64, error) {
	if off > uint64(len(data)) || uint64(len(data))-off < abiWord {
		return 0, errors.New("word out of bounds")
	}
	word := data[off : off+abiWord]
	for _, b := range word[:abiWord-8] {
		if b != 0 {
			return 0, errors.New("value exceeds 64 bits")
		}
	}
	return binary.BigEndian.Uint64(word[abiWord-8:]), nil
}

// abiOffset reads a word that is about to be added to a position. Bounding it
// by the buffer length is what keeps that addition from overflowing.
func abiOffset(data []byte, off uint64) (uint64, error) {
	v, err := abiUint(data, off)
	if err != nil {
		return 0, err
	}
	if v > uint64(len(data)) {
		return 0, errors.New("offset out of bounds")
	}
	return v, nil
}

// abiBytes reads a length-prefixed byte string starting at off.
func abiBytes(data []byte, off uint64) ([]byte, error) {
	n, err := abiUint(data, off)
	if err != nil {
		return nil, err
	}
	start := off + abiWord
	if n > uint64(len(data))-start {
		return nil, errors.New("bytes out of bounds")
	}
	return data[start : start+n], nil
}

func appendUint(b []byte, v uint64) []byte {
	b = append(b, make([]byte, abiWord-8)...)
	return binary.BigEndian.AppendUint64(b, v)
}

func appendAddress(b []byte, a domain.Address) []byte {
	b = append(b, make([]byte, abiWord-domain.AddressLen)...)
	return append(b, a[:]...)
}

func paddedLen(n int) int { return (n + abiWord - 1) / abiWord * abiWord }
