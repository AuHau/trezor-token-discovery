# RPC budget

The assignment says every request costs money, so this is the service's bill,
against the design it replaces. "Measured" means taken from `/metrics` against
mainnet; everything else is arithmetic from those numbers and is labelled.

## The bill

| | Calls | Basis |
|---|---|---|
| **Backfill**, one-off, 371,574 blocks | ~1,860 `eth_getLogs` | Arithmetic at 200 blocks per call. The run took 1h23m48s and logged no warnings, so no range was halved |
| **Tip following** | ~29,300 / day | One head poll per 4 s (21,600), one `eth_getLogs` per block (7,200), safe and finalized tags every 6 min (480). Measured at about 22 per minute, or ~31,700 / day |
| **Discovery request** | 0 | A local prefix scan |
| **Balance request, steady state** | 0.073 `eth_call` per request, measured | Only entries a transfer touched, or that reached the age limit |
| **Age limit** | at most 5,000 batches / day at 5k users | Arithmetic: one batch per active account per day. This is what re-reading rebasing tokens costs |

On **Linea** the same arithmetic gives ~250 `eth_getLogs` for the whole 501k-block
backfill (2,000 blocks per call, at ~1.7 logs per block) and ~31,000 calls / day
to follow the tip: 21,600 head polls, ~9,600 single-block `eth_getLogs` at ~9 s
blocks, and 480 tag reads. Neither has been measured from `/metrics` yet. See
[measurements](measurements.md#second-network-linea).

Tip following does not depend on how many users there are. Only balance refreshes
do, and those follow on-chain activity in the accounts being polled.

## Against the naive design

Without an index, each poll has to ask the node about the account directly: one
`eth_getLogs` filtered on `from`, one on `to`, then a `balanceOf` per token.

| At 5,000 users polling once a minute | Calls / day |
|---|---|
| Naive: 2 `eth_getLogs` per poll, before any balance read | **14,400,000** |
| This service: tip following | ~31,700 |
| This service: balance refreshes | ~526,000 (arithmetic from the measured 365 `eth_call` per minute) |

## Why it is this cheap

1. **One scan serves every user.** `eth_getLogs` is filtered on `topic0` only, so
   its cost does not grow with users.
2. **gzip.** 14.5× fewer bytes and 12× faster on the same payload, which also makes
   the backfill affordable. See [measurements](measurements.md).
3. **Invalidation by event, not by clock.** A cached balance is re-read because a
   transfer touched it, not because a timer ran out.
4. **Multicall3.** Up to 300 `balanceOf` in one `eth_call`.
5. **Metadata once.** Fetched once per token and kept for the life of the store.
6. **ETag/304.** This saves bandwidth, not RPC, and is reported separately so the
   two are not confused.

## Load test

`make loadtest` runs 5,000 users polling once a minute, about 83 requests/s, against
a running `serve`, on real accounts taken from recent transfers. Those accounts lean
active, since dormant wallets do not appear in recent logs, so a real population
should be cheaper than this one.

Results from the full-span store on mainnet, 20 minutes (`-duration 20m -warmup 10`),
starting with a cold balance cache. Steady state is the last 10 periods:

| Period | Requests | p50 | p95 | p99 | Failed | 304 | `eth_call` / period | `eth_getLogs` / period |
|---|---|---|---|---|---|---|---|---|
| 1 to 4, cache filling | 19,968 | 1.6 to 2 ms | ~805 ms | ~807 ms | 0 | 0.3% to 62.9% | ~1,480 | 4 to 6 |
| 5 to 10, settling | 30,005 | 1.1 to 1.3 ms | 6.2 to 42.7 ms | 41.1 to 69.1 ms | 0 | 66.0% to 96.6% | 189 to 746 | 5 |
| **11 to 20, steady state** | **49,976** | **1.2 ms** | **37.5 ms** | **48 ms** | **0** | **96.9%** | **365 on average** | **~5** |

- **Cost:** 0.073 `eth_call` per request in steady state, and about 0.11 over the whole
  run. The provider rate-limited nothing, and the log has no warnings or errors.
- **A cold cache costs about five minutes.** Requests wait out the 800 ms budget and most
  tokens come back `stale` or `unavailable`, but nothing fails.
- **Degraded tokens:** `td_balance_degraded_total` rose by 43,691 in steady state, about
  4% of ~1.1M balance lookups. Response bodies showed 20,909 non-`ok` tokens out of
  42,431, but that 49% describes only the ~3% of responses that were not 304, so it
  is not the rate users see.
- **Latency is not flat in steady state.** p95 sat near 40 ms in periods 11 to 15, where
  `eth_call` rose to 372 to 668 per period, and near 6 ms otherwise. The cause is unknown.
- **Misses are unexplained.** 436,571 steady-state lookups, about 40%, missed the cache,
  against only 2,332 marked dirty by a transfer. Until that is explained, 0.073 is what
  was measured and not necessarily the floor.

## What it does not measure

- The harness and the service share a machine, so latency includes their contention.
- Ten minutes of steady state does not reach the one-day age limit, so those
  re-reads are in the arithmetic above and not in the measured rows.
- One provider on one plan. A different provider's limits change where a cold cache
  starts to degrade.
