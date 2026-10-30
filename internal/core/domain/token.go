package domain

import "math/big"

// Standard is the token standard a contract was classified as.
//
// Classification is derived from log shape alone, with no contract calls: an
// ERC-20 Transfer has 3 topics and a 32-byte data payload (the unindexed
// value), while an ERC-721 Transfer has 4 topics and empty data (the tokenId is
// indexed). Measured against 1,148,860 mainnet Transfer logs, only 3 did not
// match either shape — see docs/measurements.md.
type Standard uint8

const (
	// StandardUnknown is the zero value and never stored.
	StandardUnknown Standard = 0
	// StandardERC20 is a fungible token.
	StandardERC20 Standard = 1
	// StandardERC721 is a non-fungible token.
	StandardERC721 Standard = 2
)

// String implements fmt.Stringer and doubles as the API's wire form.
func (s Standard) String() string {
	switch s {
	case StandardERC20:
		return "erc20"
	case StandardERC721:
		return "erc721"
	default:
		return "unknown"
	}
}

// Valid reports whether s is a standard the service indexes.
func (s Standard) Valid() bool { return s == StandardERC20 || s == StandardERC721 }

// ParseStandard maps the API wire form back to a Standard.
func ParseStandard(s string) (Standard, bool) {
	switch s {
	case "erc20":
		return StandardERC20, true
	case "erc721":
		return StandardERC721, true
	default:
		return StandardUnknown, false
	}
}

// TokenRef identifies a token contract on a specific chain. It is the unit the
// balance and metadata readers take, so those calls are self-describing rather
// than depending on ambient chain context.
type TokenRef struct {
	ChainID uint64
	Address Address

	// Standard decides which metadata a contract owes us: ERC-721 has no
	// decimals, so asking for them would spend a call slot on a certain failure.
	Standard Standard
}

// AccountToken is the single fact the index stores: this account interacted
// with this token. It deliberately asserts nothing about ownership or balance —
// an entry can exist with a zero balance (spam airdrop, sold position, a
// reorged-out log), which is exactly why balances are read live and separately.
type AccountToken struct {
	Account  Address
	Token    Address
	Standard Standard

	// FirstBlock and LastBlock bound the observed interactions. LastBlock moves
	// forward as new transfers land; FirstBlock never changes once written.
	FirstBlock uint64
	LastBlock  uint64
}

// AccountTokenBatch is the result of scanning one block range. The set is
// add-only and order-independent, so applying a batch twice is a no-op and
// batches from overlapping ranges merge without conflict.
type AccountTokenBatch struct {
	FromBlock uint64
	ToBlock   uint64

	// Pairs are the account-token facts observed in the range, already
	// deduplicated within the batch.
	Pairs []AccountToken

	// Blocks carries per-block provenance: the hash each pair set came from
	// (so logs can be validated against the canonical chain) and the content
	// digest used for cross-run consistency checks.
	Blocks []BlockDigest

	// NFTTransfers are the ERC-721 ownership changes in the range, in the
	// order observed. Empty unless ERC-721 indexing is enabled.
	NFTTransfers []NFTTransfer

	// LogCount is the raw number of Transfer logs seen, for metrics and for
	// validating density assumptions against docs/measurements.md.
	LogCount int

	// Skipped counts logs that were decoded but produced no pair: mint and
	// burn legs against the zero address, and shapes that match neither
	// standard.
	Skipped SkipCounts
}

// SkipCounts explains why logs produced no pairs. Surfaced rather than silently
// dropped: a sudden rise in Malformed means the provider or the decoder is
// wrong, and the resulting harm would be a *missing* token — the only failure
// mode that actually misleads a user.
type SkipCounts struct {
	ZeroAddress  int
	UnknownShape int
	Malformed    int
	WrongTopic   int
	RemovedFlag  int
	OutsideRange int
}

// Total is the number of logs that yielded nothing.
func (s SkipCounts) Total() int {
	return s.ZeroAddress + s.UnknownShape + s.Malformed + s.WrongTopic + s.RemovedFlag + s.OutsideRange
}

// NFTTransfer is one ERC-721 ownership change, decoded from the indexed tokenId
// in topic[3]. Ownership is "last transfer wins", so LogIndex is carried to
// order transfers within a block — which is also why this view is only ever
// advanced to the safe watermark, never to an orphan-able head.
type NFTTransfer struct {
	Token    Address
	TokenID  Hash // uint256, kept as raw bytes: token IDs routinely exceed 64 bits
	To       Address
	Block    uint64
	LogIndex uint32
}

// BlockDigest is the per-block content hash frozen once a block is finalized.
// Two honest runs over the same finalized range must produce identical digests,
// which is what makes a mismatch unambiguously a bug or a lying provider.
type BlockDigest struct {
	Number   uint64
	Hash     Hash
	Digest   Hash
	LogCount int
}

// Balance is a token balance read live from the chain.
//
// Raw is nil when the read failed, in which case Err explains why; that is a
// per-token outcome rather than a request failure, because one reverting
// contract must not sink the whole response.
type Balance struct {
	Token Address
	Raw   *big.Int
	Block uint64
	Err   string

	// Reverted says the contract itself refused to answer, as opposed to the
	// read never reaching it. Only the second is worth retrying immediately.
	Reverted bool
}

// OK reports whether the balance was read successfully.
func (b Balance) OK() bool { return b.Raw != nil }

// IsZero reports whether the balance was read and is zero. Zero-balance rows
// are filtered from responses by default: they are the harmless residue of
// airdrop spam and reorged-out logs.
func (b Balance) IsZero() bool { return b.Raw != nil && b.Raw.Sign() == 0 }

// TokenMeta is a token's display metadata, fetched once and cached for the
// lifetime of the store.
type TokenMeta struct {
	Token    Address
	Standard Standard
	Symbol   string
	Name     string
	Decimals uint8

	// Resolved distinguishes "we asked and the contract has no metadata" from
	// "we have not asked yet".
	Resolved bool

	// Version increments whenever the resolved metadata changes. It is part of
	// the ETag input, so a symbol resolving from null correctly invalidates a
	// client's cached page.
	Version uint32
}
