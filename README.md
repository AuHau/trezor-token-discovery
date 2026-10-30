# trezor-token-discovery

ERC-20 and ERC-721 **token discovery over plain JSON-RPC** — the question a
non-rich node cannot answer: *which tokens has this account interacted with, and
what are the balances?*

A PoC backend that watches Ethereum mainnet from a configured start block, keeps
the minimum state needed to answer that question, and serves it over REST —
without being a full indexer and without burning paid RPC credits.

## The idea in one paragraph

ERC-20 and ERC-721 transfers are both logs with the same
`topic0 = keccak256("Transfer(address,address,uint256)")`, with `from` and `to`
as indexed topics. So **one `eth_getLogs` per block range, filtered only on
`topic0`, serves every user at once**. From each log the service keeps a single
fact — the `AccountToken` pair `(account, token, standard)` — and nothing else.
Discovery is then a local prefix scan with **zero RPC**; balances are read live
via Multicall3 and cached with **event-driven invalidation**, so a cached
balance stays valid until a transfer actually touches it. The two standards are
told apart by log *shape* alone, at no RPC cost: ERC-20 has 3 topics and 32
bytes of data, ERC-721 has 4 topics and empty data.

## Status

Built incrementally against the implementation plan.

| Phase | | |
|---|---|---|
| 1 | Measurement | ✅ [`docs/measurements.md`](docs/measurements.md) |
| 2 | Skeleton: domain, ports, config, logging, metrics, CLI | ✅ |
| 3 | RPC client: limiter, retry, deadlines, error classification | ✅ |
| 4 | Log decoder: shape classification, digests, golden + fuzz tests | ✅ |
| 5 | Pebble store: keyspace, merge-based writes, checkpoints | ✅ |
| 6 | Indexer: backfill, tip follower, reorg handling | ✅ |
| 7 | REST API: discovery, pagination, ETags, status | ✅ |
| 8 | Balances (Multicall3) | ✅ |
| 9 | ERC-721 endpoint | ✅ |
| 10 | Docs and load test | ⬜ |

## Quick start

```sh
cp .env.example .env     # add your RPC_URL (it carries an API key — .env is gitignored)
make                     # fmt, vet, test, build

./bin/tokendiscovery version
./bin/tokendiscovery serve          # the API, /healthz, /readyz, /metrics
```

`/readyz` is backed by the indexer's own position: an instance that is up but
behind the tip reports **not ready** and is taken out of rotation rather than
answering with a stale head.

`backfill` must complete before `serve` will start. Serving a half-indexed store
would return confidently wrong *empty* token lists — the one failure mode that
genuinely misleads a user — so it is made impossible rather than described in
the API.

```sh
./bin/tokendiscovery backfill                # seed the store (~75 min, ~1.8k requests)
./bin/tokendiscovery serve                   # follow the tip and answer queries
./bin/tokendiscovery checkpoint -out ./snap  # clone the store for a new replica
```

`backfill` is interruptible: the cursor is committed with every range, so
re-running it resumes rather than restarting. For a fast demo,
`backfill -from-block N` seeds a short span instead of the full 50 days.
`checkpoint` takes the store's directory lock, so it needs the service stopped.
Checkpointing a live instance would need an admin endpoint, which does not exist
yet.

## Measured, not assumed

Phase 1 exists so the design rests on numbers rather than guesses. Against
mainnet (2026-10-29, full detail in [`docs/measurements.md`](docs/measurements.md)):

| | |
|---|---|
| Transfer logs per block | **574** (the plan had guessed ~300) |
| ERC-20 : ERC-721 split | **98.4% : 1.6%** |
| Logs unclassifiable by shape | **3 in 1,148,860** — which is why there is no ERC-165 probe |
| `Accept-Encoding: gzip` | **14.5× smaller, 12× faster** — the single most important detail in the RPC client |
| `eth_getLogs` limits | a 10,000-**block** range cap, *no* result cap — and a bare HTTP 413 past ~200k blocks |
| Full 50-day backfill | ~1.8k calls, ~9 GB gzipped, **~25 min** of RPC (see below for the measured end-to-end figure) |
| Concurrency | **flat** (238 blocks/s at 1 worker, 278 at 16) — so the backfill is a sequential loop |

## The API

```
GET /v1/chains                                           configured networks
GET /v1/chains/{chain}/accounts/{address}/tokens          ERC-20 discovery
GET /v1/chains/{chain}/accounts/{address}/nfts            ERC-721 discovery
GET /v1/status                                            what this instance has indexed
GET /healthz · /readyz · /metrics                         operational
```

The discovery call is the one the assignment asks for, and in steady state it
costs **one Pebble prefix scan and zero RPC calls**:

```sh
curl "localhost:8080/v1/chains/ethereum/accounts/0x9De9…Ce33/tokens"
```

```json
{
  "chain": "ethereum", "chainId": 1,
  "account": "0x9De929252c9017FE8A4Cd5986475a658aD87Ce33",
  "standard": "erc20",
  "block": {
    "height": 26304253,
    "hash": "0x715e4d46…",
    "safe": 26304190,
    "finalized": 26304158,
    "unconfirmed": true
  },
  "freshness": { "ready": true, "lagBlocks": 0, "latestSeenBlock": 26304253 },
  "tokens": [
    { "address": "0x4857DB576eE0e45F10525eC59527BfD10beeC60b",
      "standard": "erc20", "firstBlock": 26290000, "lastBlock": 26304240 }
  ]
}
```

| Parameter | |
|---|---|
| `limit` | 1–1000, default 100 |
| `cursor` | the `nextCursor` from the previous page (a token address) |
| `balances` | include the current balance per token |
| `include_zero` | keep tokens whose balance reads zero (dropped by default) |

Four things in that response are deliberate:

- **`block.height` is the answer to "as of when?"** — the assignment's
  requirement, taken from the index cursor rather than from a live `eth_blockNumber`,
  so it describes the data actually returned.
- **`block.unconfirmed`** says the height is above the safe watermark, so a
  token could still be reorged out. The pair set is add-only, so the risk is an
  extra token, never a missing one.
- **`freshness`** is in *every* response, not just `/readyz`, because "this
  account holds no tokens" and "this instance is 500 blocks behind" must never
  look the same.
- **Balances are a decimal string**, not a JSON number. A uint256 does not fit
  in a float64, so a number would silently lose precision in every JavaScript
  client.

A balance carries a `status`, because there are three honest answers and not
two: `ok` is current (no transfer has touched the pair since it was read, and it
was read within the last day), `stale` is a real value a later transfer
invalidated, and `unavailable` means no value — never a zero standing in for
one. See [Balances](#balances) for how each one comes about.

Errors are RFC 9457 problem documents with a stable `type`, so a client can
branch on the kind rather than parse prose — and a mixed-case address that
fails its EIP-55 checksum is **rejected**, because the alternative is answering
a typo with a perfectly valid empty token list:

```json
{ "type": "https://trezor.io/errors/address-checksum",
  "title": "Invalid account address", "status": 400,
  "detail": "address failed EIP-55 checksum verification",
  "requestId": "Deving/a3uWtaLyJm-000008" }
```

Every response carries an `ETag` and honours `If-None-Match`, which is a
*bandwidth* saving rather than an RPC one — the RPC saving is the cache's job —
and `td_http_bytes_saved_by_304_total` is there to show which of the two
actually happened. On discovery routes it is a weak validator that leaves out the
heights: the head moves every 12 seconds, so hashing the whole body would mean a
client polling once a minute never gets a 304. The current head still travels in
`X-Block-Number` and `X-Block-Hash`, including on the 304.

## ERC-721

`/nfts` is the same pipeline with one more table. Collections come from the same
pairs, and `counts=true` is the same balance path, since an ERC-721 `balanceOf`
is a count: one request answers both, with no RPC beyond what a dirty entry
needs.

```sh
curl "localhost:8080/v1/chains/ethereum/accounts/0xDa02…B7F6/nfts?counts=true&token_ids=true"
```

```json
{
  "chain": "ethereum", "chainId": 1,
  "account": "0xDa021A2C1C2F99574D861Ef8c6fA745851BEB7F6",
  "block": { "height": 26304670, "finalized": 26304572, "…": "…" },
  "tokenIdsThroughBlock": 26304572,
  "collections": [
    { "address": "0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85",
      "firstBlock": 26303190, "lastBlock": 26304666,
      "ownedCount": { "count": "18163", "block": 26304669, "status": "ok" },
      "knownTokenIds": ["215693765813587502…", "…"],
      "tokenIdsTruncated": true }
  ]
}
```

| Parameter | |
|---|---|
| `counts` | include the live owned count per collection; the only one that can cost RPC |
| `token_ids` | include the token IDs the index knows of, at most 100 per collection |
| `include_zero` | keep collections whose count reads zero (dropped by default) |
| `limit`, `cursor` | as for `/tokens` |

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

## Balances

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
- **The budget bounds the request.** Live reads get 800 ms; past that, the
  response goes out with whatever the cache has. A read is shared between
  concurrent requests for the same page and outlives the caller that started
  it, so a client hanging up neither aborts it nor wastes it.

Known limits: a gas burner that does *not* sink its batch can leave later tokens
in it failing, which recovers after the 10-minute window for balances but is
kept for metadata. Coalescing is per page rather than per token, so overlapping
pages of different sizes read separately.

### Measured against mainnet

2026-10-30, with an index seeded over the last 400 blocks, on an account with 95
tokens:

| | |
|---|---|
| Cold page, balances and metadata | **0.40 s**, 2 `eth_call`s |
| Warm page | **~2 ms**, 0 `eth_call`s |
| Multicall balance vs direct `balanceOf` at the same block | 12 of 12 identical |
| Non-standard metadata | MKR's `bytes32` symbol decodes correctly |
| 75 polls at 1/s across 7 new blocks | ~7,180 clean hits, **6 dirty refreshes**, 2 new tokens, 0 degraded |

## Keeping up with the chain

[`internal/indexer`](internal/indexer) has two modes, and the split is the
design:

**Backfill** sweeps from the start block to the tip in 200-block ranges — one
sequential loop, because concurrency was measured and buys ~15% (see above), so
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

### Measured end to end

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
   and writing the pairs is now in the loop. The full 365,796-block span is
   therefore **~75 minutes**, not ~25 — still a one-off cost, and still one a
   checkpoint saves every later replica from paying.
2. **The store is bigger than the "low single-digit GB" estimate implies at
   this span.** 330 MB per 14k blocks does not extrapolate linearly — dedup
   improves the further the sweep runs, which is exactly what the growth curve
   in `docs/measurements.md` shows — but the honest statement today is that
   the full-span figure has been *projected*, not measured. Phase 10 measures
   it.

### Unreliable RPC, in layers

The assignment asks how the service copes with an RPC that is slow, erroring,
rate-limiting or behind the tip. Each of those is handled where it is cheapest,
not in one catch-all:

| | |
|---|---|
| Slow | per-method deadlines; `eth_getLogs` gets the long one, header reads fail fast so a tick can retry |
| Erroring | the client retries a *call* with full-jitter backoff; the indexer retries a *range*; neither kills the follower, which keeps trying while `/readyz` reports not-ready |
| Rate limiting | a client-side token bucket that **shrinks on a 429**, so the service settles below the provider's limit instead of bouncing off it |
| Range too large | classified as "make it smaller" — including a bare HTTP 413 — and the scan range halves, stickily, for the rest of the run |
| Behind the tip | every request is clamped to the head the node itself reported; a head that moves *backwards* (a lagging pool member) retargets the sweep instead of failing it |
| Lying | logs are checked against the header's block hash, since `eth_getLogs` carries no `removed` flag, and a mismatch is counted and refused rather than indexed |

Steady state costs about **20 requests a minute** — one head poll per tick, one
`eth_getLogs` per block, and the safe/finalized tags on an epoch cadence — and
that figure is independent of how many users the service has.

## The store

[`internal/store`](internal/store) is the durable index, on embedded Pebble.
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

## Layout

```
cmd/tokendiscovery/   serve · backfill · checkpoint · version
internal/core/
  domain/             chain-agnostic vocabulary (Address, AccountToken, Watermarks, …)
  ports/              the interfaces everything is built against
internal/config/      env-based configuration, with phase-1 measurements as defaults
internal/obs/         slog + the Prometheus metric set
internal/api/         router, /tokens and /nfts handlers, ETags, RFC 9457 errors
internal/balances/    balance resolution: cache, live reads, budget, degradation
internal/chains/      the registry that resolves a chain slug to its adapter
internal/chains/evm/  generic EVM adapter (shared by every EVM network), Multicall3
internal/store/       Pebble index: keyspace, merge operators, NFT ownership, checkpoints
internal/indexer/     backfill, tip follower, reorg handling, readiness
tools/measure/        phase-1 measurement tool
tools/loadtest/       5,000 users polling once a minute, reporting RPC per request
testdata/             captured mainnet logs + hand-written edge cases
docs/                 architecture notes, measurements, assignment
```

The interfaces in `internal/core/ports` are the seam the assignment asks for:
adding an EVM network is a config block, and adding a non-EVM network is a new
`ChainAdapter` implementation with storage and API untouched.

## A second network: Linea

Linea is the network Trezor Suite does not support yet that best tests that
claim. Suite already signs for any EVM chain ID, so the backend is the only
missing piece. Linea is EVM, so the adapter, decoder, store and API run
unchanged. Its finality is the one thing that is genuinely different: as a
zk-rollup it finalises when a proof lands on L1, about 4 hours behind the head
rather than about 13 minutes.

```sh
CHAIN=linea RPC_URL=https://rpc.linea.build ./bin/tokendiscovery backfill   # ~250 requests
CHAIN=linea RPC_URL=https://rpc.linea.build ./bin/tokendiscovery serve -addr :8081
curl localhost:8081/v1/chains/linea/accounts/<address>/tokens?balances=true
```

`CHAIN` selects a preset in `internal/config` and defaults the store to
`./data-linea`, so it never shares a directory with the Ethereum store in
`./data-ethereum`. At startup `eth_chainId` is checked against the preset before the
store is opened, so pointing a Linea config at an Ethereum endpoint is a startup
error rather than a mislabelled index. The only value that had to differ from
Ethereum, apart from the chain itself, is the finality-stall threshold. The
numbers behind it are in [`docs/measurements.md`](docs/measurements.md#second-network-linea)
and the consequences are in [`docs/limits.md`](docs/limits.md#chain-and-reorgs).

## Technology choices

| | Why | Rejected |
|---|---|---|
| **Go** | The work is I/O-bound fan-out and streaming millions of logs: goroutines, low memory, one static binary. | — |
| **Pebble** | Embedded, so a read never crosses a network and there is no shared database to bottleneck. Sorted keys make `account → tokens` one seek, an LSM suits the write-heavy backfill, merge operators keep that path read-free, and hardlinked checkpoints make cloning a store cheap. | BoltDB is too slow under write load. Badger is viable, but its checkpointing is weaker. |
| **No Postgres, no Redis** | The index and balance cache share one local store, so invalidation is a local write in the same atomic batch, not a distributed-cache problem. There is nothing to deploy alongside. | Shared state is what you would add for a leader/replica split, and nothing below that needs it. |
| **Multicall3** | The largest saving on the read path: N `balanceOf` in one `eth_call`, on the same address on most EVM chains. | Per-token calls, or JSON-RPC batches, which providers bill per call. |
| **REST** | The contract is tiny, and ETag/304 is a real lever for clients polling once a minute. | GraphQL gives that up for flexibility nobody here needs. |
| **Stdlib where it reaches** | `encoding/json` measured at 243 MB/s, so it is not the bottleneck. `encoding/binary` for the store, `log/slog` for logs, a few dozen lines for retries. Dependencies are `pebble`, `chi`, `prometheus/client_golang`, `x/time/rate`, `x/sync/singleflight`, `x/crypto/sha3`. | `go-ethereum`: the service needs keccak and a small ABI subset, not a client library. |

## Documentation

- [`docs/limits.md`](docs/limits.md) — what this does not do, and what happens at each edge
- [`docs/rpc-budget.md`](docs/rpc-budget.md) — what the service costs, against the naive design, and the load test
- [`docs/measurements.md`](docs/measurements.md) — phase-1 numbers and how to reproduce them
- [`docs/architecture.md`](docs/architecture.md) — design notes, what became of them, and whether a bad index can spread
- [`docs/openapi.yaml`](docs/openapi.yaml) — the API contract
- [`docs/presentation.md`](docs/presentation.md) — the key decisions, as a talk outline
- [`docs/assignment.md`](docs/assignment.md) — the original task
