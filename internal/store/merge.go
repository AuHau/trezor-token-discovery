package store

import (
	"fmt"
	"io"
	"math/big"

	"github.com/cockroachdb/pebble/v2"
)

// mergerName is persisted in the store's OPTIONS file. Pebble refuses to open a
// store whose merger name differs from the configured one, which is a second
// line of defence behind SchemaVersion: changing merge *semantics* without
// changing the layout still has to refuse the old data.
const mergerName = "tokendiscovery.v1"

// newMerger builds the store's merge operator.
//
// Merging is what keeps the write path blind. The indexer never reads before
// writing: it merges a pair's observed block range and an invalidation stamp
// for every touched pair, and the operators below resolve those against
// whatever is already stored, at read or compaction time. For a backfill of
// ~30–50M pairs that is the difference between a sequential write stream and
// tens of millions of random point reads.
//
// Both operators are commutative and idempotent — min, max and argmax — so
// applying the same batch twice is a no-op and batches from overlapping ranges
// merge without conflict. That is exactly the property the add-only pair set in
// domain.AccountTokenBatch claims, and it is what makes a crash mid-backfill
// recoverable by simply re-scanning the range.
//
// One merger is registered per store, so it dispatches on the key prefix.
func newMerger() *pebble.Merger {
	return &pebble.Merger{
		Name: mergerName,
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			if len(key) == 0 {
				return nil, fmt.Errorf("store: merge on an empty key")
			}
			switch key[0] {
			case prefixPair:
				return newPairMerger(value)
			case prefixBalance:
				return newBalanceMerger(value)
			default:
				// Reached only by a bug: everything else is written with Set.
				return nil, fmt.Errorf("store: no merge operator for key prefix %q", key[0])
			}
		},
	}
}

// pairMerger keeps the widest observed block range for an account-token pair:
// FirstBlock is a running min and LastBlock a running max, so the order
// operands arrive in does not matter.
type pairMerger struct {
	first, last uint64
}

func newPairMerger(value []byte) (pebble.ValueMerger, error) {
	first, last, err := decodePairRange(value)
	if err != nil {
		return nil, err
	}
	return &pairMerger{first: first, last: last}, nil
}

func (m *pairMerger) add(value []byte) error {
	first, last, err := decodePairRange(value)
	if err != nil {
		return err
	}
	if first < m.first {
		m.first = first
	}
	if last > m.last {
		m.last = last
	}
	return nil
}

func (m *pairMerger) MergeNewer(value []byte) error { return m.add(value) }
func (m *pairMerger) MergeOlder(value []byte) error { return m.add(value) }

func (m *pairMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return encodePairRange(m.first, m.last), nil, nil
}

// balanceMerger resolves cached balances against invalidation stamps.
//
// The rule is the one that makes event-driven invalidation honest: a balance
// read at block B is stale only if a transfer touching that pair landed at a
// block *after* B. So the merger keeps the newest balance it has seen and the
// highest invalidation block, and reports dirty only when the latter is ahead
// of the former.
//
// That resolution has to live here rather than in the caller because the two
// writers race by design: the indexer stamps pairs as it advances while a
// request is reading balances at an older block. Comparing block numbers at
// merge time makes the outcome independent of which write lands first, so a
// late-arriving read can never clear an invalidation that still applies.
type balanceMerger struct {
	haveValue bool
	raw       *big.Int
	block     uint64
	dirtyAt   uint64
}

func newBalanceMerger(value []byte) (pebble.ValueMerger, error) {
	m := &balanceMerger{}
	if err := m.add(value); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *balanceMerger) add(value []byte) error {
	r, err := decodeBalance(value)
	if err != nil {
		return err
	}
	if r.dirtyAt > m.dirtyAt {
		m.dirtyAt = r.dirtyAt
	}
	// Keep the value read at the highest block. Two reads at the same block
	// cannot disagree, so ties need no tie-break.
	if r.value && (!m.haveValue || r.block > m.block) {
		m.haveValue, m.raw, m.block = true, r.raw, r.block
	}
	return nil
}

func (m *balanceMerger) MergeNewer(value []byte) error { return m.add(value) }
func (m *balanceMerger) MergeOlder(value []byte) error { return m.add(value) }

func (m *balanceMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	value, _, closer, err := m.DeletableFinish(includesBase)
	return value, closer, err
}

// DeletableFinish lets a pair that was invalidated but never cached disappear
// instead of accumulating a stamp forever. Without it, every transfer the
// indexer sees would leave a permanent nine-byte record for an account that
// may never ask about its balance — which is most of them.
//
// Deleting is only safe when the whole merge chain was present: includesBase is
// conservative, so a false value means an older balance may exist in a lower
// level of the LSM and the stamp must be kept to invalidate it.
func (m *balanceMerger) DeletableFinish(includesBase bool) ([]byte, bool, io.Closer, error) {
	if !m.haveValue {
		if includesBase {
			return nil, true, nil, nil
		}
		return encodeDirty(m.dirtyAt), false, nil, nil
	}
	// A stamp at or below the value's block describes a transfer the read
	// already accounts for, so it is dropped rather than carried.
	dirtyAt := m.dirtyAt
	if dirtyAt <= m.block {
		dirtyAt = 0
	}
	value, err := encodeBalance(m.raw, m.block, dirtyAt)
	return value, false, nil, err
}

var (
	_ pebble.ValueMerger          = (*pairMerger)(nil)
	_ pebble.DeletableValueMerger = (*balanceMerger)(nil)
)
