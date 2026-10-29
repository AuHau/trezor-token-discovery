package domain

import "time"

// BlockRef identifies a block and carries what the reorg detector needs.
type BlockRef struct {
	Number     uint64
	Hash       Hash
	ParentHash Hash
	Time       time.Time
}

// WatermarkStrategy says how a chain derives its safety watermarks.
//
// It exists because finality is the one thing that does not generalise across
// chains: Ethereum publishes probabilistic `safe`/`finalized` tags, Avalanche
// publishes the same tags but every accepted block is already final, L2s define
// finality against L1, and some chains offer nothing but a block count.
type WatermarkStrategy string

const (
	// WatermarkConsensusTags reads the safe and finalized block tags. Standard
	// post-Merge Ethereum.
	WatermarkConsensusTags WatermarkStrategy = "consensus-tags"

	// WatermarkImmediateFinality treats the head as already final, so safe and
	// finalized both track latest. Correct for BFT chains.
	WatermarkImmediateFinality WatermarkStrategy = "immediate-finality"

	// WatermarkBlockCount falls back to a fixed confirmation depth, for chains
	// that publish no tags and as the escape hatch when finality stalls.
	WatermarkBlockCount WatermarkStrategy = "block-count"
)

// Valid reports whether the strategy is one the EVM adapter implements.
func (w WatermarkStrategy) Valid() bool {
	switch w {
	case WatermarkConsensusTags, WatermarkImmediateFinality, WatermarkBlockCount:
		return true
	}
	return false
}

// Watermarks are the three heights the indexer tracks, each used for a
// different purpose:
//
//   - Latest: discovery and balance pinning. User-facing freshness; failures
//     here are small and self-healing because the pair set is add-only.
//   - Safe: the `unconfirmed` flag and ERC-721 ownership ordering.
//   - Finalized: digests and checkpoints, which must be stable forever.
type Watermarks struct {
	Latest    BlockRef
	Safe      BlockRef
	Finalized BlockRef
}

// FinalityLag is how far finalized trails latest. A large and growing value
// means the chain has stopped finalising, which stalls digest freezing and
// checkpointing and triggers the block-count fallback.
func (w Watermarks) FinalityLag() uint64 {
	if w.Latest.Number < w.Finalized.Number {
		return 0
	}
	return w.Latest.Number - w.Finalized.Number
}

// ChainInfo describes a configured chain. Adding an EVM network means supplying
// one of these; adding a non-EVM network means a new adapter behind the same
// port, with storage and API untouched because both are already chain-keyed.
type ChainInfo struct {
	Slug       string
	ChainID    uint64
	StartBlock uint64
	Standards  []Standard
	Watermarks WatermarkStrategy
}

// Supports reports whether this chain indexes the given standard.
func (c ChainInfo) Supports(s Standard) bool {
	for _, got := range c.Standards {
		if got == s {
			return true
		}
	}
	return false
}

// Cursor is the indexer's durable position, written in the same atomic batch as
// the pairs it describes so a crash can never leave the two disagreeing.
type Cursor struct {
	// SeededFromBlock records the block a completed backfill started from.
	// Zero means the store has never been seeded, which makes `serve` refuse
	// to boot rather than serve confidently wrong empty token lists.
	SeededFromBlock uint64

	// HeadBlock and HeadHash are the live edge. The hash is what the boot-time
	// canonicality check verifies, catching a reorg that straddled a restart.
	HeadBlock uint64
	HeadHash  Hash

	SafeBlock      uint64
	FinalizedBlock uint64

	// ParserVersion is the decoder's version. A mismatch refuses the store,
	// forcing a reindex — affordable because a full backfill is ~1.8k calls,
	// so a decoder bug can never become permanent.
	ParserVersion uint32
}

// Seeded reports whether a backfill has completed against this store.
func (c Cursor) Seeded() bool { return c.SeededFromBlock > 0 }
