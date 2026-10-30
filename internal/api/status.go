package api

import (
	"net/http"

	"tokendiscovery/internal/core/domain"
)

// chainDescription is the static configuration of one chain: what a client
// needs to know before it asks anything.
type chainDescription struct {
	Chain      string   `json:"chain"`
	ChainID    uint64   `json:"chainId"`
	StartBlock uint64   `json:"startBlock"`
	Standards  []string `json:"standards"`

	// Watermarks names how this chain derives safety heights, because it
	// changes what `unconfirmed` means: on a BFT chain the head is already
	// final, on Ethereum it is not.
	Watermarks string `json:"watermarks"`
}

// handleChains lists the configured chains.
//
// It exists so a client never has to hardcode a slug, which is the client-side
// half of "adding a network is a config block".
func (d Deps) handleChains(w http.ResponseWriter, r *http.Request) {
	infos := d.Chains.Chains()
	out := make([]chainDescription, 0, len(infos))
	for _, info := range infos {
		out = append(out, describe(info))
	}
	d.writeCacheable(w, r, map[string]any{"chains": out})
}

func describe(info domain.ChainInfo) chainDescription {
	standards := make([]string, 0, len(info.Standards))
	for _, s := range info.Standards {
		standards = append(standards, s.String())
	}
	return chainDescription{
		Chain:      info.Slug,
		ChainID:    info.ChainID,
		StartBlock: info.StartBlock,
		Standards:  standards,
		Watermarks: string(info.Watermarks),
	}
}

// chainStatus is one chain's index position.
type chainStatus struct {
	chainDescription

	SeededFromBlock uint64 `json:"seededFromBlock"`
	IndexedHead     uint64 `json:"indexedHead"`
	HeadHash        string `json:"headHash,omitempty"`
	SafeBlock       uint64 `json:"safeBlock"`
	FinalizedBlock  uint64 `json:"finalizedBlock"`
	ParserVersion   uint32 `json:"parserVersion"`

	// Error is set when this chain's position could not be read, so one broken
	// chain degrades its own entry rather than the whole response.
	Error string `json:"error,omitempty"`
}

// handleStatus reports what this instance has indexed.
//
// Separate from /readyz on purpose: /readyz answers a load balancer with a
// status code, this answers an operator — or a reviewer — with the numbers
// behind that decision.
func (d Deps) handleStatus(w http.ResponseWriter, r *http.Request) {
	infos := d.Chains.Chains()
	out := make([]chainStatus, 0, len(infos))
	for _, info := range infos {
		entry := chainStatus{chainDescription: describe(info)}
		cursor, err := d.Store.Cursor(r.Context(), info.ChainID)
		if err != nil {
			entry.Error = "index position could not be read"
			d.Logger.Error("status: read cursor", "chain", info.Slug, "error", err)
		} else {
			entry.SeededFromBlock = cursor.SeededFromBlock
			entry.IndexedHead = cursor.HeadBlock
			entry.SafeBlock = cursor.SafeBlock
			entry.FinalizedBlock = cursor.FinalizedBlock
			entry.ParserVersion = cursor.ParserVersion
			if !cursor.HeadHash.IsZero() {
				entry.HeadHash = cursor.HeadHash.Hex()
			}
		}
		out = append(out, entry)
	}

	d.writeCacheable(w, r, map[string]any{
		"version":   d.Version,
		"freshness": d.freshness(),
		"chains":    out,
	})
}
