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
| 7 | REST API | ⬜ |
| 8 | Balances (Multicall3) | ⬜ |
| 9 | ERC-721 endpoint | ⬜ |
| 10 | Docs and load test | ⬜ |

## Quick start

```sh
cp .env.example .env     # add your RPC_URL (it carries an API key — .env is gitignored)
make                     # fmt, vet, test, build

./bin/tokendiscovery version
./bin/tokendiscovery serve          # /healthz, /readyz, /metrics (discovery lands in phase 7)
```

`/readyz` is backed by the indexer's own position: an instance that is up but
behind the tip reports **not ready** and is taken out of rotation rather than
answering with a stale head.

```sh
```

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
`checkpoint` takes the store's directory lock, so it needs the service stopped;
doing it from a live instance is an admin endpoint that lands with the API.

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
entry is not "fresh for 30 seconds" — it is provably still correct and can be
served forever without an RPC call. Resolving by block number rather than write
order is also what stops a late-landing in-flight read from clearing an
invalidation that still applies.

A reorg takes the opposite treatment per table: per-block digests above the
common ancestor are dropped and the balance cache is flushed wholesale (every
cached value is a claim about a height), while account-token pairs are kept —
re-scanning re-adds the canonical ones, and an orphaned pair is at worst an
extra token whose live balance reads zero.

`checkpoint` is the scale-out story: Pebble hardlinks the sstables, so cloning a
multi-GB store costs no extra disk up front and a new replica starts warm
instead of paying for its own backfill.

## Layout

```
cmd/tokendiscovery/   serve · backfill · checkpoint · version
internal/core/
  domain/             chain-agnostic vocabulary (Address, AccountToken, Watermarks, …)
  ports/              the interfaces everything is built against
internal/config/      env-based configuration, with phase-1 measurements as defaults
internal/obs/         slog + the Prometheus metric set
internal/api/         router, health, metrics, RFC 9457 errors
internal/chains/evm/  generic EVM adapter (shared by every EVM network)
internal/store/       Pebble index: keyspace, merge operators, checkpoints
internal/indexer/     backfill, tip follower, reorg handling, readiness
tools/measure/        phase-1 measurement tool
testdata/             captured mainnet logs + hand-written edge cases
docs/                 architecture notes, measurements, assignment
```

The interfaces in `internal/core/ports` are the seam the assignment asks for:
adding an EVM network is a config block, and adding a non-EVM network is a new
`ChainAdapter` implementation with storage and API untouched.

## Documentation

- [`docs/measurements.md`](docs/measurements.md) — phase-1 numbers and how to reproduce them
- [`docs/architecture.md`](docs/architecture.md) — design notes and open questions (expanded in phase 10)
- [`docs/assignment.md`](docs/assignment.md) — the original task
