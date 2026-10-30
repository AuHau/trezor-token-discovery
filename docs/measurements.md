# Phase 1 — measurements

Every sizing number in [the plan](architecture.md) started as an estimate. This document replaces
them with measurements taken against a QuickNode Ethereum mainnet endpoint (Geth v1.17.8) on
**2026-10-29**, tip **26,299,430**, assignment start block **25,933,634** → span **365,796 blocks
(50.8 days)**.

Reproduce with:

```sh
cp .env.example .env    # add your RPC_URL
set -a; . ./.env; set +a
go run ./tools/measure -windows 10 -window-size 200   # density + split + decode cost
go run ./tools/measure -curve 12000 -curve-chunk 300  # unique-pair growth
```

## Headline: estimate vs measured

| Quantity | Plan estimate | **Measured** | Impact |
|---|---|---|---|
| Transfer logs per block | ~300 | **574** | 1.9× more data than planned |
| Transfer logs, full window | ~110M | **~210M** | — |
| ERC-20 : ERC-721 split | unstated | **98.4% : 1.6%** | NFT path is cheap |
| Logs unclassifiable by shape | "exotic contracts may be misclassified" | **3 of 1,148,860 (0.0003%)** | dropping the ERC-165 probe is justified |
| Decoded JSON per block | ~170 KB implied | **365 KB** | — |
| Wire bytes | not considered | **14.5× smaller with gzip** | the single biggest win |
| `encoding/json` throughput | ~50 MB/s | **243 MB/s** (24% of request time) | stdlib json confirmed |
| `eth_getLogs` limit | assumed ~10k results ⇒ ~30 blocks | **10,000-block range cap, no result cap** | range is bounded by response size, not results |
| Backfill call count | ~12k | **~1,800** at 200 blocks/call | — |
| Backfill wall time | ~20 min at 10 rps | **~22–26 min, bandwidth-bound** | concurrency buys ~15%, not 8× |
| Unique account-token pairs | 30–60M | **~30–50M** (growth curve) | store stays low single-digit GB |

## Log density and standard split

2,000 blocks sampled as 10 × 200-block windows spread evenly across the 50-day span (not clustered at
the tip, so the figures aren't biased by recent activity):

```
Transfer logs           1148860  (574.4 per block)
ERC-20   (3 topics+32B) 1129979  (98.4%)
ERC-721  (4 topics+0B)    18878  (1.6%)
unclassified                  3  (0.00%)

log shapes seen:
  topics=3 data=32B       1129979  (98.36%)   → ERC-20
  topics=4 data=0B          18878  (1.64%)    → ERC-721
  topics=1 data=96B             3  (0.00%)    → neither (fully unindexed Transfer)
```

**Only three shapes exist in 1.15M logs.** Shape-based classification resolves 99.9997% of them with
zero contract calls, which is the evidence for not implementing an ERC-165 `supportsInterface` probe.
The three stragglers emit `Transfer` with no indexed parameters at all, so they carry no account
topics and are correctly skipped rather than misclassified.

## Account-token pairs

```
pair emissions          2235607  (1117.8 per block)   ~1.95 per log
zero-address skipped      62107                       mint/burn legs
unique pairs in sample   846432  (dedup 2.64x within 2000 blocks)
unique tokens              9873
unique accounts           607493
```

Pair emissions are a loose upper bound on store size because popular pairs recur. The growth curve
over 12,000 contiguous blocks is the honest input:

| blocks scanned | emissions | unique pairs | dedup | unique/block |
|---|---|---|---|---|
| 300 | 244,956 | 82,117 | 2.98× | 273.7 |
| 1,200 | 1,060,399 | 280,595 | 3.78× | 233.8 |
| 3,000 | 2,649,238 | 515,701 | 5.14× | 171.9 |
| 6,000 | 5,591,648 | 952,230 | 5.87× | 158.7 |
| 12,000 | 11,078,970 | 1,730,299 | 6.40× | 144.2 |

The marginal rate decays from 274 to ~145 unique pairs/block and is still falling slowly. Linear
extrapolation at the current margin gives ~53M pairs for the full window; with continued decay
**~30–50M** is the working figure. At a 46-byte key plus ~8-byte value that is ~2.2 GB raw, so the
plan's "low single-digit GB" stands.

## gzip is the most important implementation detail

Identical 400-block request, with and without compression:

| | wire bytes | time | decoded bytes | logs |
|---|---|---|---|---|
| plain | 119,270,171 | 21.01 s | 119,270,171 | 187,587 |
| gzip | **8,206,112** | **1.74 s** | 119,270,171 | 187,587 |

**14.5× smaller, 12× faster, byte-identical payload.** This turns the full backfill from ~134 GB into
~9.2 GB on the wire.

Go's `http.Transport` adds `Accept-Encoding: gzip` and decompresses transparently **only if the
request does not set that header itself**. Setting it manually silently transfers responsibility for
decompression to the caller. The measurement tool documents this at the call site; the implementation
must not "helpfully" set the header.

## `eth_getLogs` limits (sets the range and the error handling)

| Probe | Result |
|---|---|
| Range 1,000 / 10,000 blocks | OK |
| Range 100,000 blocks | `-32614` · `eth_getLogs is limited to a 10,000 range` |
| 800 blocks / 365,284 logs / 232 MB | **OK — no result cap hit** |
| Request beyond the tip | `-32602` · `block range extends beyond current head block` |
| Request fully in the future | `-32602` · `requested block range is in the future` |
| Range 200,000 blocks | **bare HTTP 413**, no JSON-RPC error body (found in phase 3) |

Two corrections to the plan:

1. There is **no 10,000-*result* cap** on this endpoint — 365k logs came back in a single response. So
   the "halve on *more than 10000 results*" trigger is the wrong primary guard. The real constraint is
   a **10,000-block range cap** (`-32614`) plus response size: at 365 KB/block, a 10,000-block request
   would be a 3.6 GB response. The range must be chosen for a sane response size, not for a result
   limit. **200 blocks ≈ 73 MB decoded / ~4 MB wire** is a good default.
2. The tip follower must clamp to the known head: over-requesting returns `-32602`, not an empty result.
3. **An oversized range is signalled two different ways.** A modest over-request (10,001 blocks) returns
   a `-32614` JSON-RPC error, but a large one (200,000 blocks) is rejected with a bare **HTTP 413**
   before any JSON-RPC error exists. Both mean "make the request smaller", so the client classifies 413
   as splittable rather than permanent — otherwise the indexer would give up instead of halving. This
   was found by the phase-3 integration test, not the phase-1 probe, because the probe only went up to
   100,000 blocks.

## Transfer and decode cost

```
decoded JSON            730.5 MB  (365 KB per block)
request wall time        12.29s   (59.5 MB/s decoded-equivalent)
encoding/json decode      3.01s   (243 MB/s, 24% of request time)
```

Decoding is **not** the bottleneck: 243 MB/s is ~5× the planned assumption, and at 24% of request time
it costs ~0.24 of a core during backfill. stdlib `encoding/json` is the right choice and no
third-party decoder is warranted.

## Concurrency does not help

Eight 200-block requests at varying parallelism, distinct ranges so nothing is served from cache:

| workers | wall | wire | blocks/s | implied full backfill |
|---|---|---|---|---|
| 1 | 0.84 s | 3.9 MB | 238 | 25.6 min |
| 2 | 2.17 s | 10.1 MB | 185 | 33.0 min |
| 4 | 3.82 s | 19.1 MB | 209 | 29.1 min |
| 8 | 6.12 s | 33.3 MB | 261 | 23.3 min |
| 16 | 11.51 s | 62.5 MB | 278 | 21.9 min |

Throughput is flat at ~200–280 blocks/s across a 16× range of parallelism — the endpoint is
bandwidth/server-shaped, and a single connection already saturates it. Measurements are noisy (2
workers came out slower than 1), so treat these as a band, not a curve.

**Consequence for the plan:** the "parallel backfill over disjoint ranges with ordered commit" is not
worth building. A single sequential loop lands within ~15% of the best observed rate and removes the
ordered-commit machinery entirely. Revisit only if a provider is found that scales with concurrency.

## What this changes in the plan

- **Add gzip to the RPC client contract** (and a note never to set `Accept-Encoding` by hand).
- **Replace the result-cap guard** with a 10,000-block range cap plus a response-size-driven range;
  keep halving as the fallback for providers that do impose result caps.
- **Drop concurrent backfill** in favour of a sequential loop.
- **Backfill is ~25 min**, so the "pay once, checkpoint, never backfill again" story holds.
- **Revise density figures**: 574 logs/block, ~210M logs, ~30–50M pairs.
- **Clamp tip requests to the known head** to avoid `-32602`.
- Confirmed as-is: shape-based classification, stdlib `encoding/json`, low single-digit GB store.

## Second network: Linea

The same tool, pointed at Linea's public endpoint (`https://rpc.linea.build`) on **2026-10-30**,
tip **32,473,388**. The start block **31,972,391** is the Linea block with the same timestamp as
Ethereum's 25,933,634, so both chains cover the same ~52-day window: **501,009 blocks**, ~9 s apart on
average.

```sh
RPC_URL=https://rpc.linea.build go run ./tools/measure -start-block 31972391 -windows 10 -window-size 1000
```

| | Ethereum | Linea |
|---|---|---|
| Transfer logs per block | 574 | **1.7** |
| ERC-20 / ERC-721 split | | 99.4% / 0.6% |
| Unclassified log shapes | 0 | 0 |
| Decoded JSON per block | ~365 KB | **~1 KB** |
| `latest − finalized`, healthy | ≤ ~126 blocks (~25 min) | **~1,720 blocks (~3.7 h)** |
| `safe` | Its own tag, one epoch behind | **Equal to `finalized`** |
| `eth_getLogs` caps (public endpoint) | 10,000-block range, no result cap (QuickNode) | 10,000-block range **and** 10,000 results (`-32005 query returned more than…`, already classified as `ErrResultTooLarge` and halved) |
| Multicall3 at `0xcA11…CA11` | yes | yes (`eth_getCode` non-empty) |

What this sets in the `linea` preset (`internal/config`):

- **`LogRangeBlocks = 2000`.** About 2 MB and ~3.4k logs per call, well under the result cap. The
  whole window is ~250 calls, against ~1,830 on Ethereum.
- **`FinalityStallBlocks = 21,600`.** Linea finalises when a proof lands on L1, so a ~4 h gap is
  healthy. Ethereum's 256 would declare a stall on every poll and replace L1 finality with a
  12-block guess. 21,600 is about two days at the measured block time. The 1,720 figure is a single
  sample, so treat the threshold as provisional until it has been watched for longer.
- Everything else, including the watermark strategy, the decoder, the store and the API, is
  unchanged from Ethereum.

End to end: a 20,001-block `backfill` took **14 s** and ~10 `eth_getLogs` calls (52k logs, 32k
pairs, 1 MB store). `serve` then answered `/tokens?balances=true` with live Multicall3 balances
(USDC, USDT, Aave aTokens, EURe) and logged no finality stall.
