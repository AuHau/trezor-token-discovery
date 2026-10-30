package indexer

import (
	"testing"

	"tokendiscovery/internal/core/domain"
)

func ref(n uint64, tag byte) domain.BlockRef {
	return domain.BlockRef{Number: n, Hash: domain.Hash{tag, byte(n)}}
}

func TestHeaderRingIsBounded(t *testing.T) {
	// The window is only useful near the tip, so it must not grow with the
	// chain — this runs for the lifetime of the process.
	r := newHeaderRing(4)
	for n := uint64(1); n <= 100; n++ {
		r.push(ref(n, 0))
	}
	if r.len() != 4 {
		t.Fatalf("ring holds %d headers, want 4", r.len())
	}
	if cap(r.refs) > 4 {
		t.Errorf("backing array grew to %d; the window must not accumulate", cap(r.refs))
	}
	tip, ok := r.tip()
	if !ok || tip.Number != 100 {
		t.Errorf("tip = %d (ok=%v), want 100", tip.Number, ok)
	}
	oldest, ok := r.oldest()
	if !ok || oldest.Number != 97 {
		t.Errorf("oldest = %d (ok=%v), want 97", oldest.Number, ok)
	}
	if _, ok := r.at(96); ok {
		t.Error("a header outside the window was reported as remembered")
	}
	if got, ok := r.at(98); !ok || got.Hash != ref(98, 0).Hash {
		t.Errorf("at(98) = %v (ok=%v)", got, ok)
	}
}

func TestHeaderRingTruncate(t *testing.T) {
	r := newHeaderRing(8)
	for n := uint64(10); n <= 15; n++ {
		r.push(ref(n, 0))
	}

	// A rewind to 12 means the window must forget everything above it, or the
	// next parent-hash comparison would be made against an orphaned block.
	r.truncateTo(12)
	tip, _ := r.tip()
	if tip.Number != 12 {
		t.Fatalf("tip after truncate = %d, want 12", tip.Number)
	}
	if _, ok := r.at(13); ok {
		t.Error("block 13 survived a truncate to 12")
	}

	// A rewind below everything remembered leaves the ring anchored on the
	// block it rewound to, so the next comparison has something to use.
	r.reset(ref(5, 1))
	if r.len() != 1 {
		t.Fatalf("ring holds %d headers after reset, want 1", r.len())
	}
	got, ok := r.at(5)
	if !ok || got.Hash != ref(5, 1).Hash {
		t.Errorf("at(5) after reset = %v (ok=%v)", got, ok)
	}
}

func TestHeaderRingEmpty(t *testing.T) {
	r := newHeaderRing(4)
	if _, ok := r.tip(); ok {
		t.Error("an empty ring reported a tip")
	}
	if _, ok := r.oldest(); ok {
		t.Error("an empty ring reported an oldest header")
	}
	if _, ok := r.at(1); ok {
		t.Error("an empty ring remembered a header")
	}
	// Resetting with a zero ref must leave it empty rather than anchoring the
	// follower on block 0.
	r.reset(domain.BlockRef{})
	if r.len() != 0 {
		t.Errorf("reset with a zero ref left %d headers", r.len())
	}
}
