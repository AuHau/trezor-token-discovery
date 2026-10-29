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
| 4 | Log decoder | ⬜ |
| 5 | Pebble store | ⬜ |
| 6 | Indexer (backfill + tip follower + reorgs) | ⬜ |
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

`backfill` must complete before `serve` will start. Serving a half-indexed store
would return confidently wrong *empty* token lists — the one failure mode that
genuinely misleads a user — so it is made impossible rather than described in
the API.

```sh
./bin/tokendiscovery backfill       # seed the store (phase 6)
./bin/tokendiscovery serve          # follow the tip and answer queries
```

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
| Full 50-day backfill | ~1.8k calls, ~9 GB gzipped, **~25 min** |
| Concurrency | **flat** (238 blocks/s at 1 worker, 278 at 16) — so the backfill is a sequential loop |

## Layout

```
cmd/tokendiscovery/   serve · backfill · checkpoint · version
internal/core/
  domain/             chain-agnostic vocabulary (Address, AccountToken, Watermarks, …)
  ports/              the interfaces everything is built against
internal/config/      env-based configuration, with phase-1 measurements as defaults
internal/obs/         slog + the Prometheus metric set
internal/api/         router, health, metrics, RFC 9457 errors
tools/measure/        phase-1 measurement tool
docs/                 architecture notes, measurements, assignment
```

The interfaces in `internal/core/ports` are the seam the assignment asks for:
adding an EVM network is a config block, and adding a non-EVM network is a new
`ChainAdapter` implementation with storage and API untouched.

## Documentation

- [`docs/measurements.md`](docs/measurements.md) — phase-1 numbers and how to reproduce them
- [`docs/architecture.md`](docs/architecture.md) — design notes and open questions (expanded in phase 10)
- [`docs/assignment.md`](docs/assignment.md) — the original task
