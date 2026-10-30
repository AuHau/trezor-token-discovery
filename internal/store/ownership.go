package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
)

// The ERC-721 ownership view is the one table a reorg can falsify. Pairs are
// add-only, so an orphaned row is merely an extra token; ownership is "last
// transfer wins", so an orphaned transfer would leave a token with the wrong
// owner and nothing to correct it.
//
// So the view only ever moves to the finalized watermark. Transfers above that
// wait in a per-block journal, and a reorg just deletes journal entries; the
// view itself never has to be undone. The price is that the ID list trails the
// head by about two epochs, which the API states rather than hides, and the live
// balanceOf count is there for anyone who needs the current number.
//
// Applying a transfer never reads the view. Its From says which row to remove,
// so this stays as much a write stream as the pair index.

// pendingValueLen is from + to + token + tokenId.
const pendingValueLen = domain.AddressLen*3 + domain.HashLen

func encodePending(t domain.NFTTransfer) []byte {
	b := make([]byte, 0, pendingValueLen)
	b = append(b, t.From[:]...)
	b = append(b, t.To[:]...)
	b = append(b, t.Token[:]...)
	return append(b, t.TokenID[:]...)
}

func decodePending(key, value []byte) (domain.NFTTransfer, error) {
	var t domain.NFTTransfer
	if len(key) != pendingKeyLen {
		return t, fmt.Errorf("store: pending transfer key is %d bytes, want %d", len(key), pendingKeyLen)
	}
	if len(value) != pendingValueLen {
		return t, errShortRecord("pending transfer", len(value), pendingValueLen)
	}
	t.Block = binary.BigEndian.Uint64(key[chainKeyLen:])
	t.LogIndex = binary.BigEndian.Uint32(key[digestKeyLen:])
	rest := value
	rest = rest[copy(t.From[:], rest):]
	rest = rest[copy(t.To[:], rest):]
	rest = rest[copy(t.Token[:], rest):]
	copy(t.TokenID[:], rest)
	return t, nil
}

// ownedAt reads the height the view has reached. Zero means nothing applied.
func (s *Store) ownedAt(chainID uint64) (uint64, error) {
	value, closer, err := s.db.Get(ownedAtKey(chainID))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read ownership height: %w", err)
	}
	defer closer.Close()
	if len(value) != 8 {
		return 0, errShortRecord("ownership height", len(value), 8)
	}
	return binary.BigEndian.Uint64(value), nil
}

// applyOwnership adds a batch's transfers to the journal and moves whatever has
// become final into the view, in the same atomic write as the rest of the batch.
func (s *Store) applyOwnership(b *pebble.Batch, chainID uint64, batch domain.AccountTokenBatch, cursor domain.Cursor) error {
	applied, err := s.ownedAt(chainID)
	if err != nil {
		return err
	}
	// Backfill reports the chain's finalized height, far above the block it has
	// reached, and the view must not claim blocks that were not scanned.
	limit := min(cursor.FinalizedBlock, batch.ToBlock)

	var final []domain.NFTTransfer
	if limit > applied {
		final, err = s.pendingThrough(chainID, applied, limit)
		if err != nil {
			return err
		}
	}
	for _, t := range batch.NFTTransfers {
		switch {
		case t.Block <= applied:
			// Already in the view, and replaying it would resurrect an old owner.
		case t.Block <= limit:
			final = append(final, t)
		default:
			if err := b.Set(pendingKey(chainID, t.Block, t.LogIndex), encodePending(t), nil); err != nil {
				return fmt.Errorf("apply batch: journal transfer: %w", err)
			}
		}
	}
	if limit <= applied {
		return nil
	}

	// Last transfer wins, and a provider's response order is no guarantee.
	slices.SortFunc(final, func(x, y domain.NFTTransfer) int {
		if x.Block != y.Block {
			return cmpUint64(x.Block, y.Block)
		}
		return cmpUint64(uint64(x.LogIndex), uint64(y.LogIndex))
	})
	for _, t := range final {
		// Mints have no previous owner and burns have no next one.
		if t.From != (domain.Address{}) {
			if err := b.Delete(ownedKey(chainID, t.From, t.Token, t.TokenID), nil); err != nil {
				return fmt.Errorf("apply batch: clear owner: %w", err)
			}
		}
		if t.To != (domain.Address{}) {
			if err := b.Set(ownedKey(chainID, t.To, t.Token, t.TokenID), nil, nil); err != nil {
				return fmt.Errorf("apply batch: set owner: %w", err)
			}
		}
	}

	if err := b.DeleteRange(pendingKey(chainID, applied+1, 0), pendingKey(chainID, limit+1, 0), nil); err != nil {
		return fmt.Errorf("apply batch: trim journal: %w", err)
	}
	if err := b.Set(ownedAtKey(chainID), binary.BigEndian.AppendUint64(nil, limit), nil); err != nil {
		return fmt.Errorf("apply batch: write ownership height: %w", err)
	}
	return nil
}

// pendingThrough reads the journal over (after, through].
func (s *Store) pendingThrough(chainID uint64, after, through uint64) ([]domain.NFTTransfer, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: pendingKey(chainID, after+1, 0),
		UpperBound: pendingKey(chainID, through+1, 0),
	})
	if err != nil {
		return nil, fmt.Errorf("read journal: %w", err)
	}
	defer iter.Close()

	var out []domain.NFTTransfer
	for iter.First(); iter.Valid(); iter.Next() {
		t, err := decodePending(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("read journal: %w", err)
	}
	return out, nil
}

// OwnedTokenIDs implements ports.Store.
func (s *Store) OwnedTokenIDs(ctx context.Context, chainID uint64, account domain.Address, tokens []domain.Address, limit int) (map[domain.Address]ports.OwnedTokens, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	// Read before the rows, so that a view advancing mid-request can only make
	// the stated height an understatement.
	at, err := s.ownedAt(chainID)
	if err != nil {
		return nil, 0, err
	}

	out := make(map[domain.Address]ports.OwnedTokens, len(tokens))
	for _, token := range tokens {
		owned, err := s.ownedIDs(chainID, account, token, limit)
		if err != nil {
			return nil, 0, err
		}
		out[token] = owned
	}
	return out, at, nil
}

func (s *Store) ownedIDs(chainID uint64, account, token domain.Address, limit int) (ports.OwnedTokens, error) {
	prefix := ownedPrefix(chainID, account, token)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return ports.OwnedTokens{}, fmt.Errorf("owned token ids: open iterator: %w", err)
	}
	defer iter.Close()

	var owned ports.OwnedTokens
	for iter.First(); iter.Valid(); iter.Next() {
		if len(owned.IDs) == limit {
			owned.Truncated = true
			break
		}
		var id domain.Hash
		copy(id[:], iter.Key()[len(prefix):])
		owned.IDs = append(owned.IDs, id)
	}
	if err := iter.Error(); err != nil {
		return ports.OwnedTokens{}, fmt.Errorf("owned token ids: %w", err)
	}
	return owned, nil
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
