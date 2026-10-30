package store

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"tokendiscovery/internal/core/domain"
)

// SchemaVersion is the version of the keyspace and the record encodings in this
// file. Open refuses a store written by a different one.
//
// Refusing is the right default rather than migrating: the whole store is
// rebuildable from the chain in ~1.8k RPC calls (docs/measurements.md), so a
// schema change costs one backfill, while a silent misread would produce
// confidently wrong *empty* token lists — the one failure mode that genuinely
// misleads a user.
const SchemaVersion uint32 = 2

// Record encodings are hand-rolled fixed-layout bytes rather than JSON or gob.
// Three reasons: the store holds tens of millions of records, so 16 bytes
// versus ~60 is the difference between a 2 GB and a 7 GB store; decoding is on
// the read path of every request; and a fixed layout is what lets the merge
// operators below work on raw operands without materialising a struct.
//
// Every record starts with a one-byte kind so that a future layout change is
// detectable per record rather than only via SchemaVersion.
const (
	kindCursor  byte = 1
	kindBalance byte = 2 // a cached balance
	kindDirty   byte = 3 // an invalidation stamp with no value of its own
	kindMeta    byte = 4
	kindDigest  byte = 5
)

// errShortRecord is the shape of every decode failure here: a truncated or
// mistyped record means a corrupt store, which is reported rather than
// guessed at.
func errShortRecord(what string, got, want int) error {
	return fmt.Errorf("store: corrupt %s record: %d bytes, want %d", what, got, want)
}

// ---------------------------------------------------------------------------
// Pair range: first and last block a pair was observed in.
// ---------------------------------------------------------------------------

// pairRangeLen is 8+8: first block, last block. No kind byte — this is the one
// record written tens of millions of times, and its merge operator is keyed by
// the key prefix, so a per-record tag would be eight million wasted bytes per
// 1M pairs.
const pairRangeLen = 16

func encodePairRange(first, last uint64) []byte {
	var b [pairRangeLen]byte
	binary.BigEndian.PutUint64(b[0:8], first)
	binary.BigEndian.PutUint64(b[8:16], last)
	return b[:]
}

func decodePairRange(b []byte) (first, last uint64, err error) {
	if len(b) != pairRangeLen {
		return 0, 0, errShortRecord("pair", len(b), pairRangeLen)
	}
	return binary.BigEndian.Uint64(b[0:8]), binary.BigEndian.Uint64(b[8:16]), nil
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

const cursorLen = 1 + 8 + 8 + domain.HashLen + 8 + 8 + 4

func encodeCursor(c domain.Cursor) []byte {
	b := make([]byte, 0, cursorLen)
	b = append(b, kindCursor)
	b = binary.BigEndian.AppendUint64(b, c.SeededFromBlock)
	b = binary.BigEndian.AppendUint64(b, c.HeadBlock)
	b = append(b, c.HeadHash[:]...)
	b = binary.BigEndian.AppendUint64(b, c.SafeBlock)
	b = binary.BigEndian.AppendUint64(b, c.FinalizedBlock)
	return binary.BigEndian.AppendUint32(b, c.ParserVersion)
}

func decodeCursor(b []byte) (domain.Cursor, error) {
	var c domain.Cursor
	if len(b) != cursorLen {
		return c, errShortRecord("cursor", len(b), cursorLen)
	}
	if b[0] != kindCursor {
		return c, fmt.Errorf("store: cursor record has kind %d, want %d", b[0], kindCursor)
	}
	b = b[1:]
	c.SeededFromBlock = binary.BigEndian.Uint64(b[0:8])
	c.HeadBlock = binary.BigEndian.Uint64(b[8:16])
	copy(c.HeadHash[:], b[16:16+domain.HashLen])
	b = b[16+domain.HashLen:]
	c.SafeBlock = binary.BigEndian.Uint64(b[0:8])
	c.FinalizedBlock = binary.BigEndian.Uint64(b[8:16])
	c.ParserVersion = binary.BigEndian.Uint32(b[16:20])
	return c, nil
}

// ---------------------------------------------------------------------------
// Balance cache
// ---------------------------------------------------------------------------

// maxBalanceBytes is the width of a uint256, the widest value an ERC-20
// balanceOf can return. A longer value means the adapter is wrong.
const maxBalanceBytes = 32

// balanceHeaderLen is kind + block + dirtyAt + rawLen.
const balanceHeaderLen = 1 + 8 + 8 + 1

// encodeBalance stores a balance read at a block, plus the invalidation
// bookkeeping.
//
// dirtyAt is part of the *value* rather than a separate key so that a read and
// an invalidation of the same pair can both be blind merges: the merge operator
// resolves them without either side reading the other. See balanceMerger.
func encodeBalance(raw *big.Int, block, dirtyAt uint64) ([]byte, error) {
	// A failed read has a nil value and is never cached, so reaching here with
	// one is a caller bug; a negative balance is one too, since the encoding
	// stores a magnitude.
	if raw == nil {
		return nil, fmt.Errorf("store: cannot cache a balance with no value")
	}
	if raw.Sign() < 0 {
		return nil, fmt.Errorf("store: cannot cache a negative balance")
	}
	mag := raw.Bytes() // big-endian magnitude; empty for zero
	if len(mag) > maxBalanceBytes {
		return nil, fmt.Errorf("store: balance is %d bytes, wider than uint256", len(mag))
	}
	b := make([]byte, 0, balanceHeaderLen+len(mag))
	b = append(b, kindBalance)
	b = binary.BigEndian.AppendUint64(b, block)
	b = binary.BigEndian.AppendUint64(b, dirtyAt)
	b = append(b, byte(len(mag)))
	return append(b, mag...), nil
}

// encodeDirty is the invalidation stamp the indexer writes for every pair a
// transfer touched. It carries no balance, so it is nine bytes and needs no
// knowledge of whether the pair is even cached.
func encodeDirty(block uint64) []byte {
	b := make([]byte, 0, 9)
	b = append(b, kindDirty)
	return binary.BigEndian.AppendUint64(b, block)
}

// balanceRecord is a decoded balance-cache entry. A record with value=false is
// a bare invalidation stamp: a pair that was touched but never read, which
// carries no balance to serve.
type balanceRecord struct {
	value   bool
	raw     *big.Int
	block   uint64
	dirtyAt uint64
}

func decodeBalance(b []byte) (balanceRecord, error) {
	var r balanceRecord
	if len(b) == 0 {
		return r, errShortRecord("balance", 0, balanceHeaderLen)
	}
	switch b[0] {
	case kindDirty:
		if len(b) != 9 {
			return r, errShortRecord("invalidation", len(b), 9)
		}
		r.dirtyAt = binary.BigEndian.Uint64(b[1:9])
		return r, nil
	case kindBalance:
		if len(b) < balanceHeaderLen {
			return r, errShortRecord("balance", len(b), balanceHeaderLen)
		}
		n := int(b[balanceHeaderLen-1])
		if len(b) != balanceHeaderLen+n {
			return r, errShortRecord("balance", len(b), balanceHeaderLen+n)
		}
		r.value = true
		r.block = binary.BigEndian.Uint64(b[1:9])
		r.dirtyAt = binary.BigEndian.Uint64(b[9:17])
		r.raw = new(big.Int).SetBytes(b[balanceHeaderLen:])
		return r, nil
	default:
		return r, fmt.Errorf("store: balance record has unknown kind %d", b[0])
	}
}

// ---------------------------------------------------------------------------
// Token metadata
// ---------------------------------------------------------------------------

// maxMetaString bounds symbol and name. Both come from an untrusted contract
// and are only ever displayed, so a contract returning a megabyte of text is
// truncated rather than stored.
const maxMetaString = 128

const metaHeaderLen = 1 + 1 + 1 + 1 + 4 + 1 + 1 // kind, standard, decimals, resolved, version, two lengths

func encodeMeta(m domain.TokenMeta) []byte {
	symbol, name := truncate(m.Symbol), truncate(m.Name)
	b := make([]byte, 0, metaHeaderLen+len(symbol)+len(name))
	b = append(b, kindMeta, byte(m.Standard), m.Decimals, boolByte(m.Resolved))
	b = binary.BigEndian.AppendUint32(b, m.Version)
	b = append(b, byte(len(symbol)))
	b = append(b, symbol...)
	b = append(b, byte(len(name)))
	return append(b, name...)
}

func decodeMeta(token domain.Address, b []byte) (domain.TokenMeta, error) {
	var m domain.TokenMeta
	if len(b) < metaHeaderLen {
		return m, errShortRecord("token metadata", len(b), metaHeaderLen)
	}
	if b[0] != kindMeta {
		return m, fmt.Errorf("store: token metadata record has kind %d, want %d", b[0], kindMeta)
	}
	m.Token = token
	m.Standard = domain.Standard(b[1])
	m.Decimals = b[2]
	m.Resolved = b[3] == 1
	m.Version = binary.BigEndian.Uint32(b[4:8])

	rest := b[8:]
	symbol, rest, err := takeString(rest)
	if err != nil {
		return m, err
	}
	name, rest, err := takeString(rest)
	if err != nil {
		return m, err
	}
	if len(rest) != 0 {
		return m, fmt.Errorf("store: token metadata record has %d trailing bytes", len(rest))
	}
	m.Symbol, m.Name = symbol, name
	return m, nil
}

func takeString(b []byte) (string, []byte, error) {
	if len(b) < 1 {
		return "", nil, errShortRecord("token metadata", 0, 1)
	}
	n := int(b[0])
	if len(b) < 1+n {
		return "", nil, errShortRecord("token metadata string", len(b)-1, n)
	}
	return string(b[1 : 1+n]), b[1+n:], nil
}

func truncate(s string) string {
	if len(s) > maxMetaString {
		return s[:maxMetaString]
	}
	return s
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// Block digest
// ---------------------------------------------------------------------------

const digestLen = 1 + domain.HashLen*2 + 4

func encodeDigest(d domain.BlockDigest) []byte {
	b := make([]byte, 0, digestLen)
	b = append(b, kindDigest)
	b = append(b, d.Hash[:]...)
	b = append(b, d.Digest[:]...)
	return binary.BigEndian.AppendUint32(b, uint32(d.LogCount))
}

func decodeDigest(number uint64, b []byte) (domain.BlockDigest, error) {
	var d domain.BlockDigest
	if len(b) != digestLen {
		return d, errShortRecord("block digest", len(b), digestLen)
	}
	if b[0] != kindDigest {
		return d, fmt.Errorf("store: block digest record has kind %d, want %d", b[0], kindDigest)
	}
	d.Number = number
	copy(d.Hash[:], b[1:1+domain.HashLen])
	copy(d.Digest[:], b[1+domain.HashLen:1+domain.HashLen*2])
	d.LogCount = int(binary.BigEndian.Uint32(b[1+domain.HashLen*2:]))
	return d, nil
}
