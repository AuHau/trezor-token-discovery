# Presentation outline

About 30 minutes, then Q&A. The assignment asks for key decisions: what was done,
how, and why.

## Why these technologies (3 min)

Go, embedded Pebble, no Postgres or Redis, Multicall3, REST. The argument to make:
the index and the cache share one store, so invalidation is a local write in the same
atomic batch as the data, not a distributed-cache problem.

## Scale-out

Checkpointing for fresh instances

## Balances: three honest answers (5 min)

- `ok`, `stale`, `unavailable`, and never a zero standing in for "don't know".
- Invalidation by event: the indexer already sees every transfer.
- Show a poll going from cold to warm: `/metrics` and the demo below.
- Rejected: deriving balances from transfer amounts.
- TTL for rebasing tokens

## 5. An unreliable RPC, in layers (4 min)

The assignment's explicit question. Slow, erroring, rate-limiting, behind the tip, and
lying each have an answer at the cheapest place (table in the README). Then the failure
that matters: **discovery keeps answering when the provider is gone.**

## 6. Reorgs (3 min)

- Reorgs detected by per-block parent-hash check, rewound to the common ancestor and re-indexed.
- Potentially dropped tokens are stored in 


## 7. Proof (4 min)

The load test table from [rpc-budget](rpc-budget.md): 5,000 users at 83 requests/s,
0 failures, p95 37.5 ms. Read two separate numbers: 0.073 `eth_call` per request (the
cache's win) and 96.9% answered with 304 (the ETag's win).

## 8. What testing found (2 min)

Worth showing, because the assignment says the process is what counts:

- **A false finality stall.** Healthy mainnet routinely crossed the threshold, silently
  replacing `finalized` with head−12. One log line during a mainnet run gave it away.
- **stETH.** Event-driven invalidation is not a proof: rebasing tokens change with no
  `Transfer`. Fixed with an age limit.
- **An ETag that never matched,** because it hashed a block height that moves every 12
  seconds. Polling clients got almost no 304s until the load test was being prepared.

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
