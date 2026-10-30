package store

import (
	"math/big"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"tokendiscovery/internal/core/domain"
)

var (
	testAccount = domain.MustParseAddress("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed")
	testToken   = domain.MustParseAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
)

func testPair() domain.AccountToken {
	return domain.AccountToken{Account: testAccount, Token: testToken, Standard: domain.StandardERC20}
}

// mergeChain replays a merge chain the way Pebble does, and asserts that both
// replay directions agree.
//
// Pebble feeds operands either forward from the oldest (MergeNewer) or backward
// from the newest (MergeOlder) depending on which iterator drives the merge, so
// an operator that is not order-independent would be correct in tests and wrong
// in production. Running both directions here is what rules that out.
func mergeChain(t *testing.T, key []byte, operands ...[]byte) (value []byte, deleted bool) {
	t.Helper()
	if len(operands) == 0 {
		t.Fatal("no operands")
	}

	run := func(newest bool) ([]byte, bool) {
		base := operands[0]
		rest := operands[1:]
		if !newest {
			base = operands[len(operands)-1]
			rest = nil
			for i := len(operands) - 2; i >= 0; i-- {
				rest = append(rest, operands[i])
			}
		}
		m, err := newMerger().Merge(key, base)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		for _, op := range rest {
			if newest {
				err = m.MergeNewer(op)
			} else {
				err = m.MergeOlder(op)
			}
			if err != nil {
				t.Fatalf("merge operand: %v", err)
			}
		}
		// includesBase is true here: these chains start at the stored record,
		// which is what normal iteration sees.
		if dm, ok := m.(pebble.DeletableValueMerger); ok {
			value, del, _, err := dm.DeletableFinish(true)
			if err != nil {
				t.Fatalf("DeletableFinish: %v", err)
			}
			return value, del
		}
		value, _, err := m.Finish(true)
		if err != nil {
			t.Fatalf("Finish: %v", err)
		}
		return value, false
	}

	forward, fdel := run(true)
	backward, bdel := run(false)
	if string(forward) != string(backward) || fdel != bdel {
		t.Fatalf("merge is order-dependent: MergeNewer gave %x (delete %v), MergeOlder gave %x (delete %v)",
			forward, fdel, backward, bdel)
	}
	return forward, fdel
}

func TestPairMergerWidensRange(t *testing.T) {
	// FirstBlock is a min and LastBlock a max, so batches applied in any order
	// — which happens whenever a backfill range is retried or overlaps —
	// converge on the same widest range.
	merged, _ := mergeChain(t, pairKey(1, testPair()),
		encodePairRange(100, 100),
		encodePairRange(50, 50),
		encodePairRange(200, 200),
	)
	first, last, err := decodePairRange(merged)
	if err != nil {
		t.Fatal(err)
	}
	if first != 50 || last != 200 {
		t.Fatalf("merged range = %d..%d, want 50..200", first, last)
	}
}

func TestPairMergerIsIdempotent(t *testing.T) {
	// Applying the same batch twice must be a no-op; that is what makes a
	// crash mid-backfill recoverable by simply re-scanning the range.
	merged, _ := mergeChain(t, pairKey(1, testPair()), encodePairRange(10, 20), encodePairRange(10, 20))
	first, last, err := decodePairRange(merged)
	if err != nil {
		t.Fatal(err)
	}
	if first != 10 || last != 20 {
		t.Fatalf("merged range = %d..%d, want 10..20", first, last)
	}
}

func TestBalanceMergerInvalidation(t *testing.T) {
	value := func(raw int64, block uint64) []byte {
		b, err := encodeBalance(big.NewInt(raw), block, 0)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	tests := []struct {
		name        string
		operands    [][]byte
		wantValue   bool
		wantRaw     int64
		wantBlock   uint64
		wantDirtyAt uint64
		wantDeleted bool
	}{
		{
			// A transfer after the read invalidates it.
			name:        "stamp above the read",
			operands:    [][]byte{value(5, 100), encodeDirty(150)},
			wantValue:   true,
			wantRaw:     5,
			wantBlock:   100,
			wantDirtyAt: 150,
		},
		{
			// A transfer at or below the read is already reflected in it, so
			// the entry stays clean — this is what lets a clean entry be served
			// forever without an RPC call.
			name:      "stamp below the read",
			operands:  [][]byte{value(5, 100), encodeDirty(100)},
			wantValue: true,
			wantRaw:   5,
			wantBlock: 100,
		},
		{
			// A fresh read above the invalidation clears it.
			name:      "read above the stamp",
			operands:  [][]byte{value(5, 100), encodeDirty(150), value(7, 200)},
			wantValue: true,
			wantRaw:   7,
			wantBlock: 200,
		},
		{
			// An in-flight read that lands late, pinned to an older block, must
			// not clear an invalidation that still applies. Resolving by block
			// number rather than write order is what guarantees this.
			name:        "late read below the stamp",
			operands:    [][]byte{value(5, 100), encodeDirty(150), value(6, 120)},
			wantValue:   true,
			wantRaw:     6,
			wantBlock:   120,
			wantDirtyAt: 150,
		},
		{
			// Nothing is cached, so there is nothing to invalidate: the entry
			// disappears instead of leaving a stamp behind for an account that
			// may never ask about its balance.
			name:        "stamps only, nothing cached",
			operands:    [][]byte{encodeDirty(10), encodeDirty(300), encodeDirty(50)},
			wantDeleted: true,
		},
	}

	key := balanceKey(1, testAccount, testToken)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			merged, deleted := mergeChain(t, key, tc.operands...)
			if deleted != tc.wantDeleted {
				t.Fatalf("merge deleted the entry = %v, want %v", deleted, tc.wantDeleted)
			}
			if deleted {
				return
			}
			rec, err := decodeBalance(merged)
			if err != nil {
				t.Fatalf("decodeBalance: %v", err)
			}
			if rec.value != tc.wantValue {
				t.Fatalf("value present = %v, want %v", rec.value, tc.wantValue)
			}
			if tc.wantValue && (rec.raw.Int64() != tc.wantRaw || rec.block != tc.wantBlock) {
				t.Errorf("value = %s at block %d, want %d at %d", rec.raw, rec.block, tc.wantRaw, tc.wantBlock)
			}
			if rec.dirtyAt != tc.wantDirtyAt {
				t.Errorf("dirtyAt = %d, want %d", rec.dirtyAt, tc.wantDirtyAt)
			}
		})
	}
}

func TestBalanceMergerDropsStampsWithNoCachedValue(t *testing.T) {
	// Most pairs a transfer touches are never read. Their stamps must not
	// accumulate forever — but only when the whole merge chain was present,
	// since otherwise an older cached value may still be hiding in a lower
	// level of the LSM and would be wrongly resurrected as clean.
	m, err := newMerger().Merge(balanceKey(1, testAccount, testToken), encodeDirty(10))
	if err != nil {
		t.Fatal(err)
	}
	bm, ok := m.(*balanceMerger)
	if !ok {
		t.Fatalf("balance key produced a %T", m)
	}

	if _, del, _, err := bm.DeletableFinish(true); err != nil || !del {
		t.Errorf("DeletableFinish(includesBase=true) = delete %v, err %v; want delete", del, err)
	}
	if value, del, _, err := bm.DeletableFinish(false); err != nil || del || len(value) == 0 {
		t.Errorf("DeletableFinish(includesBase=false) = %x, delete %v, err %v; want the stamp kept", value, del, err)
	}
}

func TestMergerRejectsUnmergeableKeys(t *testing.T) {
	// Cursors, digests and metadata are written with Set. A merge against one
	// would silently produce nonsense, so it is refused.
	if _, err := newMerger().Merge(cursorKey(1), encodePairRange(1, 1)); err == nil {
		t.Error("merge on a cursor key was accepted")
	}
	if _, err := newMerger().Merge(nil, encodePairRange(1, 1)); err == nil {
		t.Error("merge on an empty key was accepted")
	}
	// A corrupt operand must fail the merge rather than be guessed at.
	if _, err := newMerger().Merge(pairKey(1, testPair()), []byte{1, 2, 3}); err == nil {
		t.Error("merge of a corrupt pair operand was accepted")
	}
}
