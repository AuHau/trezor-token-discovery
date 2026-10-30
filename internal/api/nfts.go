package api

import (
	"math/big"
	"net/http"
	"slices"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
)

// maxKnownTokenIDs bounds the IDs listed per collection. With the page limit it
// also bounds the response, which an account full of airdropped NFTs would
// otherwise control.
const maxKnownTokenIDs = 100

// nftsResponse is the ERC-721 contract.
//
// It holds two answers that can legitimately disagree, and keeps them apart
// instead of reconciling them: ownedCount is balanceOf, which is live truth, and
// knownTokenIds is what the indexed transfers show, which stops at finality and
// knows nothing from before the seeded window.
type nftsResponse struct {
	Chain   string `json:"chain"`
	ChainID uint64 `json:"chainId"`
	Account string `json:"account"`

	Block     blockInfo     `json:"block"`
	Freshness freshnessInfo `json:"freshness"`

	// TokenIDsThroughBlock is how far knownTokenIds has been applied. Present
	// only when token IDs were asked for.
	TokenIDsThroughBlock uint64 `json:"tokenIdsThroughBlock,omitempty"`

	Collections []nftEntry `json:"collections"`

	// NextCursor is absent on the last page.
	NextCursor string `json:"nextCursor,omitempty"`

	// OmittedEmpty counts collections dropped because the account provably owns
	// none, so a short page is explicable.
	OmittedEmpty int `json:"omittedEmpty,omitempty"`
}

type nftEntry struct {
	Address string `json:"address"`

	// FirstBlock and LastBlock bound the observed interactions, as for tokens.
	FirstBlock uint64 `json:"firstBlock"`
	LastBlock  uint64 `json:"lastBlock"`

	// Unconfirmed is true when FirstBlock is above the safe watermark, as for
	// tokens. It is about this collection being listed, not the known IDs,
	// which stop at finality regardless.
	Unconfirmed bool `json:"unconfirmed"`

	Symbol string `json:"symbol,omitempty"`
	Name   string `json:"name,omitempty"`

	OwnedCount *ownedCount `json:"ownedCount,omitempty"`

	// KnownTokenIDs are decimal strings: token IDs routinely exceed 2^53, and
	// many are hashes that use the whole uint256.
	KnownTokenIDs []string `json:"knownTokenIds,omitzero"`

	// TokenIDsTruncated says the account has more IDs than are listed here.
	TokenIDsTruncated bool `json:"tokenIdsTruncated,omitempty"`
}

// ownedCount is balanceEntry under the name it has for a collection.
type ownedCount struct {
	Count  string `json:"count,omitempty"`
	Block  uint64 `json:"block,omitempty"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`

	// Unconfirmed is as for balanceEntry.
	Unconfirmed bool `json:"unconfirmed"`
}

// handleAccountNFTs serves ERC-721 discovery.
//
// Like the token endpoint it costs no RPC by default. token_ids is a local read
// too, so counts is the only parameter that can spend any.
func (d Deps) handleAccountNFTs(w http.ResponseWriter, r *http.Request) {
	withIDs, err := parseBool(r.URL.Query().Get("token_ids"), false)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid-parameter", "Invalid token_ids flag", err.Error())
		return
	}

	x, ok := d.discover(w, r, domain.StandardERC721, "counts")
	if !ok {
		return
	}

	resp := nftsResponse{
		Chain:       x.info.Slug,
		ChainID:     x.info.ChainID,
		Account:     x.account.Checksum(),
		Block:       x.block(),
		Freshness:   d.freshness(),
		Collections: make([]nftEntry, 0, len(x.page.Pairs)),
		NextCursor:  x.nextCursor(),
	}

	var owned map[domain.Address]ports.OwnedTokens
	if withIDs {
		owned, resp.TokenIDsThroughBlock, err = d.Store.OwnedTokenIDs(r.Context(), x.info.ChainID, x.account, x.tokens, maxKnownTokenIDs)
		if err != nil {
			d.serverError(w, r, "read owned token ids", err)
			return
		}
	}

	for _, p := range x.page.Pairs {
		entry := nftEntry{
			Address:     p.Token.Checksum(),
			FirstBlock:  p.FirstBlock,
			LastBlock:   p.LastBlock,
			Unconfirmed: x.unconfirmed(p),
		}
		if m, ok := x.meta[p.Token]; ok && m.Resolved {
			entry.Symbol, entry.Name = m.Symbol, m.Name
		}
		if b, ok := x.resolved[p.Token]; ok {
			if x.omit(b) {
				resp.OmittedEmpty++
				continue
			}
			entry.OwnedCount = &ownedCount{Block: b.Block, Status: string(b.Status), Error: b.Err, Unconfirmed: x.balanceUnconfirmed(p)}
			if b.OK() {
				entry.OwnedCount.Count = b.Raw.String()
			}
		}
		if row, ok := owned[p.Token]; ok {
			entry.KnownTokenIDs = make([]string, len(row.IDs))
			for i, id := range row.IDs {
				entry.KnownTokenIDs[i] = new(big.Int).SetBytes(id[:]).String()
			}
			entry.TokenIDsTruncated = row.Truncated
		}
		resp.Collections = append(resp.Collections, entry)
	}

	d.writeDiscovery(w, r, resp, resp.validator(), resp.Block)
}

// validator is the page without what moves on its own. See
// tokensResponse.validator.
func (n nftsResponse) validator() nftsResponse {
	n.Block = blockInfo{}
	n.Freshness = freshnessInfo{Ready: n.Freshness.Ready}
	n.TokenIDsThroughBlock = 0
	n.Collections = slices.Clone(n.Collections)
	for i, entry := range n.Collections {
		if entry.OwnedCount != nil {
			c := *entry.OwnedCount
			c.Block = 0
			n.Collections[i].OwnedCount = &c
		}
	}
	return n
}
