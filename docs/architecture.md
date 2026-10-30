# Architecture

This document layouts the solution architecture for task specified in [assignment](assignment.md).

[How each part works](#how-each-part-works) is below, and [limits](limits.md)
explains where it stops. This section records where the design moved away from
the notes, and answers their open question.

```
                 ┌─────────── RPC client ───────────┐
   provider  ──▶ │ rate limit · retry + backoff ·   │
                 │ deadlines · error classification │
                 └───────┬──────────────────┬───────┘
                         │                  │
            ┌────────────▼─────┐   ┌────────▼──────────┐
            │ Indexer          │   │ Balance resolver  │
            │ backfill + tip   │   │ Multicall3 ·      │
            │ decode → pairs   │   │ budget · shared   │
            │                  │   │ in-flight reads   │
            └────────┬─────────┘   └────────┬──────────┘
                     │ atomic batch         │ read / write-back
                 ┌───▼──────────────────────▼────┐
                 │ Pebble: pairs · balance cache │
                 │ metadata · digests · cursor · │
                 │ NFT journal + ownership view  │
                 └───┬───────────────────────────┘
                     │
            ┌────────▼────────┐
            │ REST API (chi)  │  ETag / 304 · /v1/status · /metrics
            └─────────────────┘
```

## How each part works

### ERC-721

`/nfts` is the same pipeline with one more table. Collections come from the same
pairs, and `counts=true` is the same balance path, since an ERC-721 `balanceOf`
is a count: one request answers both, with no RPC beyond what a dirty entry
needs.

**The count and the list are two different answers, and they can disagree.**
`ownedCount` is `balanceOf`, so it is live truth. `knownTokenIds` comes from
indexed transfers, so it covers only the seeded window (an NFT acquired earlier
and untouched since is counted but not listed) and only up to
`tokenIdsThroughBlock`. There is no `tokenIdsComplete` flag, because a matching
count and list do not prove the list is right: a pre-window token and a recent
sale can cancel out. Token IDs are decimal strings, since many use the full
uint256.

**The list stops at finality, because it cannot be undone.** Pairs are add-only,
so a reorg leaves at worst an extra row. Ownership is last-transfer-wins, so an
orphaned transfer would leave a token with the wrong owner and nothing to
correct it. Transfers above the finalized height wait in a per-block journal,
written atomically with the batch, and a reorg just deletes journal entries. The
price is that the list trails the head by about two epochs (~13 minutes), and up
to six minutes more while the finalized tag is cached. A reorg below the view
stops the service, like one too deep to resolve: it means a lying provider or
broken finality, and the store has to be rebuilt. If the chain really stops
finalising for 256 blocks, the watermark falls back to a 12-block depth so the
view keeps moving, and for that stretch this guarantee is only as good as the
depth.

Applying a transfer needs no read, because its `from` names the row to remove,
so the view stays a write stream like the pair index.

Known limits: IDs beyond 100 per collection cannot be paged, only flagged with
`tokenIdsTruncated`. Ownership comes from events, so a contract that emits a
non-compliant `Transfer` gets a wrong list for its own collection only. ERC-1155
is not indexed.

Against mainnet (2026-10-30, 1,500 blocks seeded), `ownerOf` at
`tokenIdsThroughBlock` agreed with all 29 sampled token IDs across the 8 busiest
recipients in the window.

### Balances

`balances=true` is the only parameter that can spend RPC, and it only spends it
on what the cache cannot vouch for. Each token on the page ends up in one of
three places:

| Cache says | What happens | Cost |
|---|---|---|
| clean | served as-is, `ok` | none, however often it is polled |
| dirty, missing, or a day old | read live, written back, `ok` | its slot in one multicall |
| read failed | the old value as `stale`, or `unavailable` if there is none | bounded by the budget |

The reads are one `eth_call` to Multicall3's `aggregate3` per 300 tokens,
pinned to the page's block by number. A dirty refresh re-reads only the tokens a
transfer actually touched. Metadata (`symbol`, `name`, `decimals`) is fetched
once per token alongside the first balance read and kept for the life of the
store, so plain discovery stays at zero RPC. It is a separate `eth_call` rather
than extra slots in the balance batch: it happens at most once per token, so
the saving would not pay for a second port method.

The cases that shaped the design:

- **A clean entry expires after a day.** Rebasing tokens such as stETH change every
  holder's balance with no `Transfer`, so invalidation alone would leave `ok` on a
  wrong value forever. Re-reading past 7,200 blocks bounds that error at one extra
  read per active account per day, with no list of which tokens rebase.
- **The block is reported, not assumed.** The batch leads with Multicall3's
  `getBlockNumber()`, so `balance.block` is what the EVM says it ran at. If the
  node has lost the pinned state, the read retries at `latest`; a result below
  the page's height is `stale`, because a transfer could sit in the gap.
- **No output is not zero.** Calling an address with no code succeeds with empty
  output. Decoding that as `0` would report a confident zero, so anything but
  exactly 32 bytes is a failure.
- **`allowFailure` does not stop a gas burner.** A contract can take 63/64 of the
  gas and sink the whole batch, so a batch that fails in execution is bisected
  until the culprit is alone: a few extra calls once, not every balance on the
  page.
- **Broken contracts are left alone for 10 minutes.** Nothing stamps a pair when
  a reverting contract recovers, so it needs a clock, unlike a value. Without
  it, one broken spam token would cost a call on every poll, indefinitely.
  Outages are not remembered this way, so they retry immediately.
- **A read in flight during a reorg is not cached.** It may have run on the
  dropped branch, and the canonical transfer replacing it can be stamped at or
  below its block, so it would look clean forever. Reads above the index head
  are served but never written, and that check is atomic against the flush.
- **The budget bounds the wait, not the read.** A request waits 800 ms; past
  that, the response goes out with whatever the cache has, while the read carries
  on for up to 15 s and writes back. Ending the read at the budget is what the load
  test ruled out: with demand above the rate limit, calls sat in the limiter for
  nearly the whole budget and reached the provider with no time left, so the rate
  limit was spent on reads that could not finish and a cold cache never filled.
  A read is also shared between concurrent requests for the same page, so a client
  hanging up neither aborts it nor wastes it.
- **Reads in progress are capped.** At most about one second of the RPC rate (25
  reads) run at once. Past that, a request is answered from the cache immediately
  and `td_balance_reads_shed_total` counts it, because demand above the provider's
  rate cannot be queued away and the indexer shares the same bucket.

Known limits: a gas burner that does *not* sink its batch can leave later tokens
in it failing, which recovers after the 10-minute window for balances but is
kept for metadata. Coalescing is per page rather than per token, so overlapping
pages of different sizes read separately.

#### Measured against mainnet

2026-10-30, with an index seeded over the last 400 blocks, on an account with 95
tokens:

| | |
|---|---|
| Cold page, balances and metadata | **0.40 s**, 2 `eth_call`s |
| Warm page | **~2 ms**, 0 `eth_call`s |
| Multicall balance vs direct `balanceOf` at the same block | 12 of 12 identical |
| Non-standard metadata | MKR's `bytes32` symbol decodes correctly |
| 75 polls at 1/s across 7 new blocks | ~7,180 clean hits, **6 dirty refreshes**, 2 new tokens, 0 degraded |

### Keeping up with the chain

[`internal/indexer`](../internal/indexer) has two modes, and the split is the
design:

**Backfill** sweeps from the start block to the tip in 200-block ranges — one
sequential loop, because concurrency was [measured](measurements.md#concurrency-does-not-help) and buys ~15%, so
the ordered-commit machinery a parallel sweep needs would be paid for nothing.
It commits the cursor with every range, so an interrupted sweep resumes instead
of restarting, and `SeededFromBlock` is written only on completion so a
half-indexed store can never be served from.

**Follow** tracks the tip block by block and compares each new block's parent
hash against the hash it already indexed. That comparison is in memory and
free, which is what makes reorg detection affordable on *every* block rather
than on a timer. When it fails, the walk back to the common ancestor consults
two oracles — the in-memory header window first, then the store's per-block
digests — so a reorg that happened while the service was *down* is still
resolved exactly, with the store rewound to the real ancestor rather than a
guessed distance. A reorg deeper than the window can be resolved is fatal on
purpose: on a chain that finalises in two epochs it means a broken or lying
provider, and indexing an unverified branch is worse than stopping.

If the follower falls a long way behind — an outage, a restart after hours —
it catches up in ranged scans up to the **safe** watermark, where there is
nothing to reorg and so nothing for the per-block walk to protect, then
switches back to block-by-block at the tip.

#### Measured end to end

One run against mainnet on 2026-10-30, seeding blocks 26,290,000 → 26,304,142
(`backfill -from-block 26290000`) and then following the tip:

| | |
|---|---|
| Backfill | 14,143 blocks in **2m52s** — **82 blocks/s**, 71 `eth_getLogs` calls |
| Logs seen | 7,965,057 — **563 per block**, against the 574 phase 1 predicted |
| Pairs written | 7,361,858 |
| Store | **330 MB** for 14k blocks, with pair dedup still improving across the span |
| Steady state | 7 blocks indexed in 70s on **26 requests** (~22/min), head lag ~8s |

Two corrections to the phase-1 projections, both worth stating plainly:

1. **82 blocks/s, not 238.** The phase-1 figure measured RPC alone; decoding
   and writing the pairs is now in the loop. The full span then measured
   **74 blocks/s and 1h23m48s** for 371,574 blocks, not ~25 minutes — still a
   one-off cost, and still one a checkpoint saves every later replica from paying.
2. **The store looks bigger than the "low single-digit GB" estimate at this
   span, and is not.** 330 MB per 14k blocks does not extrapolate linearly, because
   dedup improves the further the sweep runs, as the growth curve in
   [measurements](measurements.md) shows. The full span measured **3.5 GB**, with
   205,395,819 logs seen and 167,482,969 pair writes.

### The store

[`internal/store`](../internal/store) is the durable index, on embedded Pebble.
Three decisions carry it:

**One key answers the question.** Pairs are stored as
`p <chain> <account> <token> <standard>`, so discovery is a bounded prefix scan
over one account with **zero RPC calls**, and the page cursor is just a token
address — key order *is* page order, so pagination needs no snapshot pinning.

**The write path never reads.** Pair ranges and balance invalidations are
Pebble *merge* operands, not read-modify-writes: `FirstBlock` is a running min,
`LastBlock` a running max. A backfill of ~30–50M pairs stays a sequential write
stream, and because min/max are commutative and idempotent, replaying a batch
is a no-op — which is what makes a crash mid-backfill recoverable by simply
re-scanning the range.

**A cached balance expires on an event, not a clock.** Each entry carries the
block it was read at; every transfer stamps the pair with its block. The merge
operator reports dirty only when the stamp is *ahead* of the read, so a clean
entry is not "fresh for 30 seconds": no transfer has touched it, so it costs no
RPC however often it is read. Resolving by block number rather than write order
is also what stops a late-landing in-flight read from clearing an invalidation
that still applies.

A reorg takes the opposite treatment per table: per-block digests and pending
ERC-721 transfers above the common ancestor are dropped and the balance cache is
flushed wholesale (every cached value is a claim about a height), while
account-token pairs are kept —
re-scanning re-adds the canonical ones, and an orphaned pair is at worst an
extra token whose live balance reads zero.

`checkpoint` is the scale-out story: Pebble hardlinks the sstables, so cloning a
multi-GB store costs no extra disk up front and a new replica starts warm
instead of paying for its own backfill.

The ownership view added schema version 2. A store from an earlier build is
refused at startup with a message saying to delete it and re-run `backfill`,
because it never recorded the transfers the view needs.
