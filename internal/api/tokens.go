package api

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-chi/chi/v5"

	"tokendiscovery/internal/balances"
	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
)

// Pagination bounds. The store clamps as well; these exist so the API can
// reject an unparseable or negative limit with a 400 rather than silently
// substituting one.
const (
	defaultLimit = 100
	maxLimit     = 1000
)

// tokensResponse is the discovery contract.
//
// Every field the assignment asks for is here, plus the two the design insists
// on: the block height the answer is valid for, and an honest account of how
// fresh that height is.
type tokensResponse struct {
	Chain    string `json:"chain"`
	ChainID  uint64 `json:"chainId"`
	Account  string `json:"account"`
	Standard string `json:"standard"`

	Block     blockInfo     `json:"block"`
	Freshness freshnessInfo `json:"freshness"`

	Tokens []tokenEntry `json:"tokens"`

	// NextCursor is absent on the last page.
	NextCursor string `json:"nextCursor,omitempty"`

	// OmittedZeroBalances counts tokens dropped from this page because their
	// balance was read as zero. Reported so a short page is explicable rather
	// than looking like a bug.
	OmittedZeroBalances int `json:"omittedZeroBalances,omitempty"`
}

// blockInfo is the height these data are valid for.
type blockInfo struct {
	// Height is the indexed head: the block up to which transfers have been
	// applied to the index. It is the answer to "as of when?".
	Height uint64 `json:"height"`
	Hash   string `json:"hash,omitempty"`

	Safe      uint64 `json:"safe"`
	Finalized uint64 `json:"finalized"`

	// Unconfirmed is true when Height is above the safe watermark, so some of
	// these tokens could still be reorged out. The pair set is add-only, so
	// the risk is an extra token, never a missing one.
	Unconfirmed bool `json:"unconfirmed"`
}

// freshnessInfo exposes how far behind the chain this instance is, so a client
// can tell "no tokens" from "not caught up".
type freshnessInfo struct {
	Ready           bool   `json:"ready"`
	Reason          string `json:"reason,omitempty"`
	LagBlocks       uint64 `json:"lagBlocks"`
	LatestSeenBlock uint64 `json:"latestSeenBlock,omitempty"`
}

type tokenEntry struct {
	Address  string `json:"address"`
	Standard string `json:"standard"`

	// FirstBlock and LastBlock bound the observed interactions. They are the
	// only claim the index makes: this account touched this token. Ownership
	// and balance are separate questions, answered separately.
	FirstBlock uint64 `json:"firstBlock"`
	LastBlock  uint64 `json:"lastBlock"`

	Symbol   string `json:"symbol,omitempty"`
	Name     string `json:"name,omitempty"`
	Decimals *uint8 `json:"decimals,omitempty"`

	Balance *balanceEntry `json:"balance,omitempty"`
}

type balanceEntry struct {
	// Raw is a decimal string, not a JSON number. A uint256 balance does not
	// fit in a float64, so a number here would silently lose precision in
	// every JavaScript client.
	Raw string `json:"raw,omitempty"`

	// Block is the height the balance was read at. It can trail the page's
	// block height, which is precisely what Status explains.
	Block  uint64 `json:"block,omitempty"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// handleAccountTokens serves the discovery endpoint.
//
// This is the request the whole service exists for, and in the steady state it
// costs: one Pebble prefix scan, one metadata lookup, one cached-balance
// lookup. Zero RPC calls. Only balances=true can spend any, and only for
// entries the cache cannot prove current.
func (d Deps) handleAccountTokens(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	info, ok := d.chainInfo(chi.URLParam(r, "chain"))
	if !ok {
		writeProblem(w, r, http.StatusNotFound, "unknown-chain",
			"Unknown chain", fmt.Sprintf("%q is not a configured chain; see GET /v1/chains", chi.URLParam(r, "chain")))
		return
	}

	account, err := domain.ParseAddress(chi.URLParam(r, "address"))
	if err != nil {
		// A bad checksum is reported rather than ignored: a typo'd address
		// would otherwise return a perfectly valid empty token list, which is
		// the one failure mode that genuinely misleads a user.
		kind := "invalid-address"
		if err == domain.ErrBadChecksum {
			kind = "address-checksum"
		}
		writeProblem(w, r, http.StatusBadRequest, kind, "Invalid account address", err.Error())
		return
	}

	q := r.URL.Query()

	standard := domain.StandardERC20
	if raw := q.Get("standard"); raw != "" {
		parsed, ok := domain.ParseStandard(raw)
		if !ok {
			writeProblem(w, r, http.StatusBadRequest, "invalid-standard",
				"Unknown token standard", fmt.Sprintf("%q is not one of erc20, erc721", raw))
			return
		}
		standard = parsed
	}
	if !info.Supports(standard) {
		writeProblem(w, r, http.StatusBadRequest, "standard-not-indexed",
			"Standard not indexed on this chain",
			fmt.Sprintf("%s does not index %s", info.Slug, standard))
		return
	}

	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid-limit", "Invalid limit", err.Error())
		return
	}

	var cursor *domain.Address
	if raw := q.Get("cursor"); raw != "" {
		after, err := domain.ParseAddress(raw)
		if err != nil {
			writeProblem(w, r, http.StatusBadRequest, "invalid-cursor",
				"Invalid cursor", "a cursor is the token address returned as nextCursor: "+err.Error())
			return
		}
		cursor = &after
	}

	withBalances, err := parseBool(q.Get("balances"), false)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid-parameter", "Invalid balances flag", err.Error())
		return
	}
	includeZero, err := parseBool(q.Get("include_zero"), false)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid-parameter", "Invalid include_zero flag", err.Error())
		return
	}

	// The cursor is the authority on what the index contains, so the block
	// height in the response comes from it rather than from the indexer's
	// in-memory view.
	indexCursor, err := d.Store.Cursor(ctx, info.ChainID)
	if err != nil {
		d.serverError(w, r, "read cursor", err)
		return
	}
	if !indexCursor.Seeded() {
		// Answering here would mean returning an empty list that looks like a
		// real answer. `serve` refuses to boot in this state; this is the
		// second line of defence.
		writeProblem(w, r, http.StatusServiceUnavailable, "not-seeded",
			"Index is not seeded", "the backfill has not completed, so discovery would return empty results")
		return
	}

	page, err := d.Store.AccountTokens(ctx, ports.AccountTokensQuery{
		ChainID:  info.ChainID,
		Account:  account,
		Standard: standard,
		After:    cursor,
		Limit:    limit,
	})
	if err != nil {
		d.serverError(w, r, "read account tokens", err)
		return
	}

	tokens := make([]domain.Address, 0, len(page.Pairs))
	for _, p := range page.Pairs {
		tokens = append(tokens, p.Token)
	}

	meta, err := d.Store.TokenMeta(ctx, info.ChainID, tokens)
	if err != nil {
		d.serverError(w, r, "read token metadata", err)
		return
	}

	var resolved map[domain.Address]balances.Resolved
	if withBalances && d.Balances != nil {
		// Metadata is resolved here rather than on every discovery call so that
		// balances stays the one parameter that can cost RPC. The two run side
		// by side so that a slow provider costs one budget, not two.
		var metaDone sync.WaitGroup
		if d.Metadata != nil {
			metaDone.Go(func() {
				meta = d.Metadata.Fill(ctx, info.ChainID, standard, tokens, meta)
			})
		}
		resolved, err = d.Balances.Resolve(ctx, info.ChainID, account, tokens, indexCursor.HeadBlock)
		metaDone.Wait()
		if err != nil {
			d.serverError(w, r, "resolve balances", err)
			return
		}
	}

	resp := tokensResponse{
		Chain:    info.Slug,
		ChainID:  info.ChainID,
		Account:  account.Checksum(),
		Standard: standard.String(),
		Block: blockInfo{
			Height:      indexCursor.HeadBlock,
			Safe:        indexCursor.SafeBlock,
			Finalized:   indexCursor.FinalizedBlock,
			Unconfirmed: indexCursor.HeadBlock > indexCursor.SafeBlock,
		},
		Freshness: d.freshness(),
		Tokens:    make([]tokenEntry, 0, len(page.Pairs)),
	}
	if !indexCursor.HeadHash.IsZero() {
		resp.Block.Hash = indexCursor.HeadHash.Hex()
	}
	if page.Next != nil {
		resp.NextCursor = page.Next.Hex()
	}

	for _, p := range page.Pairs {
		entry := tokenEntry{
			Address:    p.Token.Checksum(),
			Standard:   p.Standard.String(),
			FirstBlock: p.FirstBlock,
			LastBlock:  p.LastBlock,
		}
		if m, ok := meta[p.Token]; ok && m.Resolved {
			entry.Symbol, entry.Name = m.Symbol, m.Name
			// ERC-721 has no decimals, and reporting 0 would claim it does.
			if p.Standard == domain.StandardERC20 {
				decimals := m.Decimals
				entry.Decimals = &decimals
			}
		}
		if b, ok := resolved[p.Token]; ok {
			// Zero balances are the harmless residue of airdrop spam and
			// reorged-out logs, so they are dropped by default — but only when
			// the value is provably zero. A stale zero is the exact state of an
			// account that just received this token, so hiding it would hide
			// the funds.
			if !includeZero && b.Status == balances.StatusOK && b.IsZero() {
				resp.OmittedZeroBalances++
				continue
			}
			entry.Balance = &balanceEntry{
				Block:  b.Block,
				Status: string(b.Status),
				Error:  b.Err,
			}
			if b.OK() {
				entry.Balance.Raw = b.Raw.String()
			}
		}
		resp.Tokens = append(resp.Tokens, entry)
	}

	d.writeCacheable(w, r, resp)
}

// parseLimit rejects what it cannot understand instead of substituting a
// default, so a client with a bug hears about it.
func parseLimit(raw string) (int, error) {
	if raw == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("limit must be an integer")
	}
	if n < 1 || n > maxLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", maxLimit)
	}
	return n, nil
}

func parseBool(raw string, def bool) (bool, error) {
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("must be true or false")
	}
	return v, nil
}
