package indexer

import (
	"tokendiscovery/internal/core/domain"
)

// headerRing is a bounded window of the most recently indexed block headers,
// oldest first.
//
// It exists so that reorg detection costs nothing in the common case: the
// parent hash of each new block is compared against the hash of the block we
// already indexed, in memory, with no RPC call. Only once that comparison
// fails does the walk back to a common ancestor begin — and the ring is what
// that walk compares the canonical chain against.
//
// It is bounded because the alternative is unbounded memory for a window that
// is only useful near the tip: Ethereum finalises in about two epochs, so a
// reorg deeper than a hundred blocks is not a reorg, it is a catastrophe, and
// is handled as one rather than absorbed silently.
type headerRing struct {
	refs []domain.BlockRef
	max  int
}

func newHeaderRing(max int) *headerRing {
	if max < 2 {
		max = 2
	}
	// Preallocated at its final size: the window never grows, so neither
	// should its backing array.
	return &headerRing{max: max, refs: make([]domain.BlockRef, 0, max)}
}

// push appends a header, dropping the oldest once the window is full.
func (r *headerRing) push(ref domain.BlockRef) {
	if len(r.refs) < r.max {
		r.refs = append(r.refs, ref)
		return
	}
	// Shift in place rather than reslicing, so neither the length nor the
	// backing array grows over millions of blocks.
	copy(r.refs, r.refs[1:])
	r.refs[r.max-1] = ref
}

// tip returns the most recently pushed header.
func (r *headerRing) tip() (domain.BlockRef, bool) {
	if len(r.refs) == 0 {
		return domain.BlockRef{}, false
	}
	return r.refs[len(r.refs)-1], true
}

// at returns the header remembered for a block number.
func (r *headerRing) at(number uint64) (domain.BlockRef, bool) {
	for i := len(r.refs) - 1; i >= 0; i-- {
		if r.refs[i].Number == number {
			return r.refs[i], true
		}
		if r.refs[i].Number < number {
			break
		}
	}
	return domain.BlockRef{}, false
}

// oldest returns the lowest-numbered header still remembered, which bounds how
// deep a reorg can be resolved.
func (r *headerRing) oldest() (domain.BlockRef, bool) {
	if len(r.refs) == 0 {
		return domain.BlockRef{}, false
	}
	return r.refs[0], true
}

// truncateTo drops every header above a block number, which is what a rewind
// to that block means for the in-memory window.
func (r *headerRing) truncateTo(number uint64) {
	keep := 0
	for _, ref := range r.refs {
		if ref.Number > number {
			break
		}
		keep++
	}
	r.refs = r.refs[:keep]
}

// reset clears the window, for a rewind deeper than anything remembered.
func (r *headerRing) reset(ref domain.BlockRef) {
	r.refs = r.refs[:0]
	if ref.Number != 0 || !ref.Hash.IsZero() {
		r.refs = append(r.refs, ref)
	}
}

func (r *headerRing) len() int { return len(r.refs) }
