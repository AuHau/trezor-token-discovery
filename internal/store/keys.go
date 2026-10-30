// Package store is the durable index: a Pebble-backed implementation of
// ports.Store.
//
// Why an embedded key-value store at all. The read path must answer "which
// tokens has this account touched?" without an RPC call, which makes it a
// prefix scan over a key ordered account-first. That is the one access pattern
// the whole design rests on, and it is the pattern an LSM does best. Embedding
// it also removes a network hop and a second deployable from the read path, so
// a replica is a process plus a directory — which is what makes the
// "checkpoint and scale out" story in docs/architecture.md cheap.
//
// Why Pebble specifically:
//
//   - Checkpoints are hardlinked, so snapshotting a multi-GB store to seed a
//     new instance is near-instant and costs no extra disk up front.
//   - Merge operators let the indexer write pairs and balance invalidations
//     blind, with no read-modify-write. That matters: a backfill applies tens
//     of millions of pair writes (~30–50M measured, docs/measurements.md), and
//     a read before each one would turn a sequential write stream into random
//     reads.
//   - Atomic batches with range deletes give ApplyBatch and Rewind their
//     all-or-nothing semantics for free.
//   - It is a Go library with no cgo, so the service stays a single static
//     binary.
//
// # Keyspace
//
// Every key is fixed-width and begins with a one-byte prefix and the chain ID,
// so each chain owns a contiguous, independently deletable range and nothing
// needs length-prefixing or escaping to stay unambiguous. All integers are
// big-endian so that lexicographic key order is numeric order, which is what
// makes the range scans below correct.
//
//	\x00schema                                              → uint32 schema version
//	c <chain:8>                                             → cursor
//	p <chain:8> <account:20> <token:20> <standard:1>         → first:8 last:8   (merged)
//	b <chain:8> <account:20> <token:20>                      → balance cache    (merged)
//	t <chain:8> <token:20>                                   → token metadata
//	d <chain:8> <block:8>                                    → block digest
//
// The pair key is account-major and token-minor, with the standard *last*
// rather than between the account and the token. That ordering is what makes
// pagination work: the API's cursor is a token address, so a page boundary has
// to be expressible as "resume after this token" in a single scan — which it
// is only if tokens are contiguous within an account regardless of standard. A
// standard filter is then a filter on the scan rather than a narrower prefix,
// which costs nothing because one account holds tens of pairs, not millions.
package store

import (
	"encoding/binary"

	"tokendiscovery/internal/core/domain"
)

// Key prefixes. One byte each, chosen so that the hot range (pairs) and the
// volatile range (the balance cache) never interleave in the LSM.
const (
	prefixCursor  byte = 'c'
	prefixPair    byte = 'p'
	prefixBalance byte = 'b'
	prefixMeta    byte = 't'
	prefixDigest  byte = 'd'
)

// schemaKey holds the keyspace version. It sorts before every other key, and
// is checked on open: a layout change must refuse an old store rather than
// misread it. See SchemaVersion.
var schemaKey = []byte("\x00schema")

// Fixed key widths, named so the slicing below is checkable by eye.
const (
	chainKeyLen   = 1 + 8                                 // prefix + chain
	accountKeyLen = chainKeyLen + domain.AddressLen       // + account
	pairKeyLen    = accountKeyLen + domain.AddressLen + 1 // + token + standard
	balanceKeyLen = chainKeyLen + domain.AddressLen*2     // + account + token
	metaKeyLen    = chainKeyLen + domain.AddressLen       // + token
	digestKeyLen  = chainKeyLen + 8                       // + block
)

// chainPrefix is the common head of every per-chain key: prefix || chainID.
func chainPrefix(prefix byte, chainID uint64) []byte {
	k := make([]byte, chainKeyLen, pairKeyLen)
	k[0] = prefix
	binary.BigEndian.PutUint64(k[1:], chainID)
	return k
}

func cursorKey(chainID uint64) []byte {
	return chainPrefix(prefixCursor, chainID)[:chainKeyLen]
}

// accountPrefix bounds one account's pairs. The scan in AccountTokens runs over
// exactly this prefix.
func accountPrefix(chainID uint64, account domain.Address) []byte {
	return append(chainPrefix(prefixPair, chainID), account[:]...)
}

func pairKey(chainID uint64, p domain.AccountToken) []byte {
	k := append(accountPrefix(chainID, p.Account), p.Token[:]...)
	return append(k, byte(p.Standard))
}

func balanceKey(chainID uint64, account, token domain.Address) []byte {
	k := append(chainPrefix(prefixBalance, chainID), account[:]...)
	return append(k, token[:]...)
}

func metaKey(chainID uint64, token domain.Address) []byte {
	return append(chainPrefix(prefixMeta, chainID), token[:]...)
}

func digestKey(chainID uint64, block uint64) []byte {
	k := chainPrefix(prefixDigest, chainID)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], block)
	return append(k, buf[:]...)
}

// pairToken reads the token address back out of a pair key.
func pairToken(key []byte) (domain.Address, bool) {
	var a domain.Address
	if len(key) != pairKeyLen {
		return a, false
	}
	copy(a[:], key[accountKeyLen:accountKeyLen+domain.AddressLen])
	return a, true
}

// pairStandard reads the token standard back out of a pair key.
func pairStandard(key []byte) (domain.Standard, bool) {
	if len(key) != pairKeyLen {
		return domain.StandardUnknown, false
	}
	return domain.Standard(key[pairKeyLen-1]), true
}

// afterTokenKey is the exclusive lower bound for resuming a page after a token.
//
// It is the account prefix plus the token plus 0xFF, which sorts above every
// real standard byte for that token and below the next token — so both an
// ERC-20 and an ERC-721 row for the cursor's token are skipped together. That
// is why AccountTokens never splits a token's rows across a page boundary: a
// cursor cannot express "half of this token".
func afterTokenKey(chainID uint64, account, after domain.Address) []byte {
	k := append(accountPrefix(chainID, account), after[:]...)
	return append(k, 0xFF)
}

// prefixEnd returns the smallest key greater than every key with this prefix,
// for use as an exclusive iterator upper bound. It returns nil when the prefix
// is all 0xFF, which means "no upper bound" and cannot arise for the prefixes
// used here (all start with an ASCII letter).
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
