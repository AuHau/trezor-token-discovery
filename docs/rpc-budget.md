# RPC budget

The assignment says every request costs money, so this is the service's bill,
against the design it replaces. "Measured" means taken from `/metrics` against
mainnet; everything else is arithmetic from those numbers and is labelled.

## The bill

| | Calls | Basis |
|---|---|---|
| **Backfill**, one-off, 365,796 blocks | ~1,830 `eth_getLogs` | 200 blocks per call. Measured: _pending_ |
| **Tip following** | ~29,300 / day | One head poll per 4 s (21,600), one `eth_getLogs` per block (7,200), safe and finalized tags every 6 min (480). Measured at about 22 per minute, or ~31,700 / day |
| **Discovery request** | 0 | A local prefix scan |
| **Balance request, steady state** | _pending_ per request | Only entries a transfer touched, or that reached the age limit |
| **Age limit** | at most 5,000 batches / day at 5k users | Arithmetic: one batch per active account per day. This is what re-reading rebasing tokens costs |

Tip following does not depend on how many users there are. Only balance refreshes
do, and those follow on-chain activity in the accounts being polled.

## Against the naive design

Without an index, each poll has to ask the node about the account directly: one
`eth_getLogs` filtered on `from`, one on `to`, then a `balanceOf` per token.

| At 5,000 users polling once a minute | Calls / day |
|---|---|
| Naive: 2 `eth_getLogs` per poll, before any balance read | **14,400,000** |
| This service: tip following | ~31,700 |
| This service: balance refreshes | _pending_ |

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

_Results pending: they need the full-span store, because on a short index accounts
hold only a few tokens and the numbers would flatter the design._

## What it does not measure

- The harness and the service share a machine, so latency includes their contention.
- A few minutes of steady state does not reach the one-day age limit, so those
  re-reads are in the arithmetic above and not in the measured rows.
- One provider on one plan. A different provider's limits change where a cold cache
  starts to degrade.
