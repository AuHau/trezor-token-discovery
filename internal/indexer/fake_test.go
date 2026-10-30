package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/rpc"
)

// fakeChain is a deterministic synthetic chain: block N contains exactly one
// transfer, of token addr(N), involving account addr(1).
//
// That one property is what makes the tests precise rather than approximate:
// the set of tokens the store holds for addr(1) *is* the set of blocks that
// were indexed, so a gap, an overlap or a failure to rewind is visible directly
// in the data instead of having to be inferred from call counts.
//
// Block hashes are derived from the number plus a per-block fork tag, so a
// reorg is "retag these blocks" and every hash downstream of it changes exactly
// as it would on a real chain.
type fakeChain struct {
	mu   sync.Mutex
	info domain.ChainInfo

	head uint64

	// safeDepth and finalDepth place the watermarks behind the head.
	safeDepth, finalDepth uint64

	// tags[n] is block n's fork tag; absent means the original chain.
	tags map[uint64]byte

	// maxRange, when non-zero, makes any wider scan fail the way a provider
	// with a smaller range cap does.
	maxRange uint64

	// failScan is consulted before every scan, so a test can inject a provider
	// failure, or mutate the chain mid-sweep. Returning nil lets the scan
	// proceed.
	failScan func(fc *fakeChain, from, to uint64) error

	// lie makes the logs report a block hash the header does not have, which
	// is how a branch being replaced reaches a caller through eth_getLogs.
	lie bool

	// genesisTime anchors block timestamps so lag figures are meaningful.
	genesisTime time.Time

	scans      int
	headers    int
	watermarks int
	maxScanTo  uint64
	minScanFro uint64
}

func newFakeChain(start, head uint64) *fakeChain {
	return &fakeChain{
		info: domain.ChainInfo{
			Slug:       "fake",
			ChainID:    1,
			StartBlock: start,
			Standards:  []domain.Standard{domain.StandardERC20, domain.StandardERC721},
			Watermarks: domain.WatermarkConsensusTags,
		},
		head:        head,
		safeDepth:   2,
		finalDepth:  4,
		tags:        map[uint64]byte{},
		genesisTime: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC),
		minScanFro:  ^uint64(0),
	}
}

func (fc *fakeChain) Chain() domain.ChainInfo { return fc.info }

// hashOf is sha256(number, fork tag), so it changes when a block is retagged by
// a reorg and never otherwise.
func (fc *fakeChain) hashOf(n uint64) domain.Hash {
	var buf [9]byte
	binary.BigEndian.PutUint64(buf[:8], n)
	buf[8] = fc.tags[n]
	return domain.Hash(sha256.Sum256(buf[:]))
}

func (fc *fakeChain) refOf(n uint64) domain.BlockRef {
	ref := domain.BlockRef{
		Number: n,
		Hash:   fc.hashOf(n),
		Time:   fc.genesisTime.Add(time.Duration(n) * 12 * time.Second),
	}
	if n > 0 {
		ref.ParentHash = fc.hashOf(n - 1)
	}
	return ref
}

func (fc *fakeChain) Header(ctx context.Context, number uint64) (domain.BlockRef, error) {
	if err := ctx.Err(); err != nil {
		return domain.BlockRef{}, err
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.headers++
	if number > fc.head {
		return domain.BlockRef{}, fmt.Errorf("fake: block %d is beyond head %d: %w", number, fc.head, rpc.ErrBeyondHead)
	}
	return fc.refOf(number), nil
}

func (fc *fakeChain) Watermarks(ctx context.Context) (domain.Watermarks, error) {
	if err := ctx.Err(); err != nil {
		return domain.Watermarks{}, err
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.watermarks++
	behind := func(depth uint64) domain.BlockRef {
		n := fc.info.StartBlock
		if fc.head > depth && fc.head-depth > n {
			n = fc.head - depth
		}
		return fc.refOf(n)
	}
	return domain.Watermarks{
		Latest:    fc.refOf(fc.head),
		Safe:      behind(fc.safeDepth),
		Finalized: behind(fc.finalDepth),
	}, nil
}

func (fc *fakeChain) ScanAccountTokens(ctx context.Context, from, to uint64) (domain.AccountTokenBatch, error) {
	if err := ctx.Err(); err != nil {
		return domain.AccountTokenBatch{}, err
	}
	fc.mu.Lock()
	hook := fc.failScan
	fc.mu.Unlock()
	if hook != nil {
		if err := hook(fc, from, to); err != nil {
			return domain.AccountTokenBatch{}, err
		}
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.scans++
	if to > fc.maxScanTo {
		fc.maxScanTo = to
	}
	if from < fc.minScanFro {
		fc.minScanFro = from
	}
	if fc.maxRange > 0 && to-from+1 > fc.maxRange {
		return domain.AccountTokenBatch{}, fmt.Errorf("fake: range %d exceeds %d: %w", to-from+1, fc.maxRange, rpc.ErrRangeTooLarge)
	}
	if to > fc.head {
		return domain.AccountTokenBatch{}, fmt.Errorf("fake: range %d..%d is beyond head %d: %w", from, to, fc.head, rpc.ErrBeyondHead)
	}

	batch := domain.AccountTokenBatch{FromBlock: from, ToBlock: to}
	for n := from; n <= to; n++ {
		batch.Pairs = append(batch.Pairs, domain.AccountToken{
			Account:    addr(1),
			Token:      addr(n),
			Standard:   domain.StandardERC20,
			FirstBlock: n,
			LastBlock:  n,
		})
		hash := fc.hashOf(n)
		if fc.lie {
			hash = domain.Hash{0xBA, 0xD0}
		}
		batch.Blocks = append(batch.Blocks, domain.BlockDigest{
			Number:   n,
			Hash:     hash,
			Digest:   domain.Hash{byte(n)},
			LogCount: 1,
		})
		batch.LogCount++
	}
	return batch, nil
}

// advance moves the head forward.
func (fc *fakeChain) advance(blocks uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.head += blocks
}

// reorg replaces the chain from fromBlock onwards with a different branch and
// sets the new head.
func (fc *fakeChain) reorg(fromBlock, newHead uint64, tag byte) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for n := fromBlock; n <= newHead || n <= fc.head; n++ {
		fc.tags[n] = tag
	}
	fc.head = newHead
}

func (fc *fakeChain) setHead(n uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.head = n
}

func (fc *fakeChain) onScan(f func(fc *fakeChain, from, to uint64) error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.failScan = f
}

func (fc *fakeChain) setLie(v bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.lie = v
}

func (fc *fakeChain) counts() (scans, headers, watermarks int) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.scans, fc.headers, fc.watermarks
}

func (fc *fakeChain) scanBounds() (lowestFrom, highestTo uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.minScanFro, fc.maxScanTo
}

// addr builds a distinct address from a counter.
func addr(n uint64) domain.Address {
	var a domain.Address
	binary.BigEndian.PutUint64(a[domain.AddressLen-8:], n)
	return a
}
