# Presentation outline

About 30 minutes, then Q&A. The assignment asks for key decisions: what was done,
how, and why. Numbers marked _pending_ come from the full-span run.

## 1. The problem, and the one idea (3 min)

A plain node cannot answer "which tokens does this account have?". There is no such
RPC, and asking per user costs 14.4M calls a day at 5k users, before reading a single
balance.

**ERC-20 and ERC-721 transfers share one `topic0`. So one `eth_getLogs` serves every
user at once.** Keep one fact from each log: this account touched this token.

## 2. Measure first (3 min)

Phase 1 ran before any design was fixed, and it changed the design:

- gzip: **14.5× smaller, 12× faster**. The biggest single win, and easy to lose by
  setting the header by hand.
- Concurrency is **flat** from 1 to 16 workers, so the backfill is one loop and the
  ordered-commit machinery was never built.
- **3 of 1.15M** logs do not classify by shape, so there is no ERC-165 probe.
- The cap is 10,000 *blocks*, not results, and a very large request gets a bare 413.

## 3. Why these technologies (3 min)

Go, embedded Pebble, no Postgres or Redis, Multicall3, REST. The argument to make:
the index and the cache share one store, so invalidation is a local write in the same
atomic batch as the data, not a distributed-cache problem.

## 4. Balances: three honest answers (5 min)

- `ok`, `stale`, `unavailable`, and never a zero standing in for "don't know".
- Invalidation by event: the indexer already sees every transfer.
- Show a poll going from cold to warm: `/metrics` and the demo below.
- Rejected: deriving balances from transfer amounts.

## 5. An unreliable RPC, in layers (4 min)

The assignment's explicit question. Slow, erroring, rate-limiting, behind the tip, and
lying each have an answer at the cheapest place (table in the README). Then the failure
that matters: **discovery keeps answering when the provider is gone.**

## 6. Reorgs, and why two tables need opposite treatment (3 min)

- Pairs are add-only: a reorg leaves at worst an extra row, so nothing is deleted.
- Ownership is last-transfer-wins: an orphaned transfer would leave a wrong owner. So it
  stops at finality, and a reorg just drops journal entries.

## 7. Proof (4 min)

The load test table from [rpc-budget](rpc-budget.md): _pending_. Read two separate
numbers: RPC per request (the cache's win) and the 304 ratio (the ETag's win).

## 8. What testing found (2 min)

Worth showing, because the assignment says the process is what counts:

- **A false finality stall.** Healthy mainnet routinely crossed the threshold, silently
  replacing `finalized` with head−12. One log line during a mainnet run gave it away.
- **stETH.** Event-driven invalidation is not a proof: rebasing tokens change with no
  `Transfer`. Fixed with an age limit.
- **An ETag that never matched,** because it hashed a block height that moves every 12
  seconds. Polling clients got almost no 304s until the load test was being prepared.
- **A leaked API key path.** Transport errors quoted the provider URL, which could
  reach `/readyz`.

## 9. Limits, and what comes next (2 min)

Be direct: [limits](limits.md). The window, one provider, `checkpoint` needing a stop.
Next: a provider pool, which also makes a lagging node detectable. Then a second network:
Linea, because Suite can already sign for it and it would test the watermark strategy
for real.

## Demo

Uses the full-span store in `./data-ethereum`, so nothing needs indexing first.

```sh
set -a; . ./.env; set +a
./bin/tokendiscovery serve

A=0x111111125421cA6dc452d289314280a0f8842A65
B=localhost:8080/v1/chains/ethereum/accounts/$A
curl -s "$B/tokens" | jq '.block, (.tokens | length)'              # no RPC
curl -s "$B/tokens?balances=true" | jq '.tokens[0]'                # cold: live read
curl -s "$B/tokens?balances=true" | jq '.tokens[0]'                # warm: no RPC
curl -s localhost:8080/metrics | grep -E 'td_multicall_calls|td_balance_cache'
curl -s "$B/nfts?counts=true&token_ids=true" | jq '.collections[0]'
curl -si "$B/tokens" -H 'If-None-Match: <etag from a previous response>' | head -5   # 304
```
