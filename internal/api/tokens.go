package api

import (
	"fmt"
	"net/http"
	"slices"
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

	// Unconfirmed is true when FirstBlock is above the safe watermark, so this
	// token could still be reorged out. The pair set is add-only, so the risk
	// is an extra token, never a missing one.
	Unconfirmed bool `json:"unconfirmed"`

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

	// Unconfirmed is true when the token's LastBlock is above the safe
	// watermark, so a transfer behind this balance could still be reorged out.
	// Status does not cover this: an ok balance can be current and provisional.
	Unconfirmed bool `json:"unconfirmed"`
}

// discovery is one validated page of the index, with whatever the request asked
// to have resolved for it. It is the part the per-standard endpoints share.
type discovery struct {
	info    domain.ChainInfo
	account domain.Address

	// live is the request's opt-in to spending RPC; includeZero only means
	// anything alongside it.
	live        bool
	includeZero bool

	cursor   domain.Cursor
	page     ports.AccountTokensPage
	tokens   []domain.Address
	meta     map[domain.Address]domain.TokenMeta
	resolved map[domain.Address]balances.Resolved
}

// discover validates a discovery request and reads its page. On false it has
// already written the error response.
//
// liveParam names the flag that opts in to live reads, which is the one thing
// the endpoints call differently: an ERC-721 balanceOf is a count.
func (d Deps) discover(w http.ResponseWriter, r *http.Request, standard domain.Standard, liveParam string) (*discovery, bool) {
	ctx := r.Context()

	info, ok := d.chainInfo(chi.URLParam(r, "chain"))
	if !ok {
		writeProblem(w, r, http.StatusNotFound, "unknown-chain",
			"Unknown chain", fmt.Sprintf("%q is not a configured chain; see GET /v1/chains", chi.URLParam(r, "chain")))
		return nil, false
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
		return nil, false
	}

	if !info.Supports(standard) {
		writeProblem(w, r, http.StatusBadRequest, "standard-not-indexed",
			"Standard not indexed on this chain",
			fmt.Sprintf("%s does not index %s", info.Slug, standard))
		return nil, false
	}

	q := r.URL.Query()

	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid-limit", "Invalid limit", err.Error())
		return nil, false
	}

	var after *domain.Address
	if raw := q.Get("cursor"); raw != "" {
		parsed, err := domain.ParseAddress(raw)
		if err != nil {
			writeProblem(w, r, http.StatusBadRequest, "invalid-cursor",
				"Invalid cursor", "a cursor is the token address returned as nextCursor: "+err.Error())
			return nil, false
		}
		after = &parsed
	}

	live, err := parseBool(q.Get(liveParam), false)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid-parameter", "Invalid "+liveParam+" flag", err.Error())
		return nil, false
	}
	includeZero, err := parseBool(q.Get("include_zero"), false)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid-parameter", "Invalid include_zero flag", err.Error())
		return nil, false
	}

	// The cursor is the authority on what the index contains, so the block
	// height in the response comes from it rather than from the indexer's
	// in-memory view.
	cursor, err := d.Store.Cursor(ctx, info.ChainID)
	if err != nil {
		d.serverError(w, r, "read cursor", err)
		return nil, false
	}
	if !cursor.Seeded() {
		// Answering here would mean returning an empty list that looks like a
		// real answer. `serve` refuses to boot in this state; this is the
		// second line of defence.
		writeProblem(w, r, http.StatusServiceUnavailable, "not-seeded",
			"Index is not seeded", "the backfill has not completed, so discovery would return empty results")
		return nil, false
	}

	page, err := d.Store.AccountTokens(ctx, ports.AccountTokensQuery{
		ChainID:  info.ChainID,
		Account:  account,
		Standard: standard,
		After:    after,
		Limit:    limit,
	})
	if err != nil {
		d.serverError(w, r, "read account tokens", err)
		return nil, false
	}

	tokens := make([]domain.Address, 0, len(page.Pairs))
	for _, p := range page.Pairs {
		tokens = append(tokens, p.Token)
	}

	meta, err := d.Store.TokenMeta(ctx, info.ChainID, tokens)
	if err != nil {
		d.serverError(w, r, "read token metadata", err)
		return nil, false
	}

	var resolved map[domain.Address]balances.Resolved
	if live && d.Balances != nil {
		// Metadata is resolved here rather than on every discovery call so that
		// this flag stays the one parameter that can cost RPC. The two run side
		// by side so that a slow provider costs one budget, not two.
		var metaDone sync.WaitGroup
		if d.Metadata != nil {
			metaDone.Go(func() {
				meta = d.Metadata.Fill(ctx, info.ChainID, standard, tokens, meta)
			})
		}
		resolved, err = d.Balances.Resolve(ctx, info.ChainID, account, tokens, cursor.HeadBlock)
		metaDone.Wait()
		if err != nil {
			d.serverError(w, r, "resolve balances", err)
			return nil, false
		}
	}

	return &discovery{
		info:        info,
		account:     account,
		live:        live,
		includeZero: includeZero,
		cursor:      cursor,
		page:        page,
		tokens:      tokens,
		meta:        meta,
		resolved:    resolved,
	}, true
}

// block describes the height this page is valid for.
func (x *discovery) block() blockInfo {
	b := blockInfo{
		Height:    x.cursor.HeadBlock,
		Safe:      x.cursor.SafeBlock,
		Finalized: x.cursor.FinalizedBlock,
	}
	if !x.cursor.HeadHash.IsZero() {
		b.Hash = x.cursor.HeadHash.Hex()
	}
	return b
}

// unconfirmed reports whether a pair could still be reorged out of the list.
//
// It goes by FirstBlock because that is what puts a pair in the list. A later
// transfer above the watermark can only move LastBlock, so a long-held token
// that was touched a minute ago is not in doubt.
func (x *discovery) unconfirmed(p domain.AccountToken) bool {
	return p.FirstBlock > x.cursor.SafeBlock
}

// balanceUnconfirmed reports whether a transfer behind a pair's balance could
// still be reorged out.
//
// It goes by LastBlock rather than the block the balance was read at: a first
// read of a long-quiet token happens at the head, but nothing a reorg could undo
// went into it. LastBlock is not rolled back by a reorg, so it can overstate the
// risk until the watermark passes it, never understate it.
func (x *discovery) balanceUnconfirmed(p domain.AccountToken) bool {
	return p.LastBlock > x.cursor.SafeBlock
}

func (x *discovery) nextCursor() string {
	if x.page.Next == nil {
		return ""
	}
	return x.page.Next.Hex()
}

// omit reports whether a resolved row is dropped as empty.
//
// Zero balances are the harmless residue of airdrop spam and reorged-out logs,
// so they are dropped by default — but only when the value is provably zero. A
// stale zero is the exact state of an account that just received this token, so
// hiding it would hide the funds.
func (x *discovery) omit(b balances.Resolved) bool {
	return !x.includeZero && b.Status == balances.StatusOK && b.IsZero()
}

// handleAccountTokens serves ERC-20 discovery.
//
// This is the request the whole service exists for, and in the steady state it
// costs: one Pebble prefix scan, one metadata lookup, one cached-balance
// lookup. Zero RPC calls. Only balances=true can spend any, and only for
// entries the cache cannot prove current.
func (d Deps) handleAccountTokens(w http.ResponseWriter, r *http.Request) {
	x, ok := d.discover(w, r, domain.StandardERC20, "balances")
	if !ok {
		return
	}

	resp := tokensResponse{
		Chain:      x.info.Slug,
		ChainID:    x.info.ChainID,
		Account:    x.account.Checksum(),
		Standard:   domain.StandardERC20.String(),
		Block:      x.block(),
		Freshness:  d.freshness(),
		Tokens:     make([]tokenEntry, 0, len(x.page.Pairs)),
		NextCursor: x.nextCursor(),
	}

	for _, p := range x.page.Pairs {
		entry := tokenEntry{
			Address:     p.Token.Checksum(),
			Standard:    p.Standard.String(),
			FirstBlock:  p.FirstBlock,
			LastBlock:   p.LastBlock,
			Unconfirmed: x.unconfirmed(p),
		}
		if m, ok := x.meta[p.Token]; ok && m.Resolved {
			decimals := m.Decimals
			entry.Symbol, entry.Name, entry.Decimals = m.Symbol, m.Name, &decimals
		}
		if b, ok := x.resolved[p.Token]; ok {
			if x.omit(b) {
				resp.OmittedZeroBalances++
				continue
			}
			entry.Balance = &balanceEntry{
				Block:       b.Block,
				Status:      string(b.Status),
				Error:       b.Err,
				Unconfirmed: x.balanceUnconfirmed(p),
			}
			if b.OK() {
				entry.Balance.Raw = b.Raw.String()
			}
		}
		resp.Tokens = append(resp.Tokens, entry)
	}

	d.writeDiscovery(w, r, resp, resp.validator(), resp.Block)
}

// validator is the page without what moves on its own: the heights, and the
// lag. Readiness stays, because "not caught up" must not revalidate as unchanged.
// So does each token's unconfirmed flag: it clears only once per token, and a
// client revalidating into a 304 would otherwise hold on to it forever.
func (t tokensResponse) validator() tokensResponse {
	t.Block = blockInfo{}
	t.Freshness = freshnessInfo{Ready: t.Freshness.Ready}
	t.Tokens = slices.Clone(t.Tokens)
	for i, entry := range t.Tokens {
		if entry.Balance != nil {
			b := *entry.Balance
			b.Block = 0
			t.Tokens[i].Balance = &b
		}
	}
	return t
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
