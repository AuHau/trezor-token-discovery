# trezor-token-discovery

ERC-20 and ERC-721 **token discovery over plain JSON-RPC**: the question a non-rich
node cannot answer, *which tokens has this account interacted with, and what are the
balances?*

A PoC backend that watches a chain from a configured start block, keeps the minimum
state needed to answer that question, and serves it over REST, without being a full
indexer and without burning paid RPC credits. It runs on Ethereum mainnet and Linea.

- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API](#api)
- [Handling an unreliable RPC](#handling-an-unreliable-rpc)
- [Measured results](#measured-results)
- [Technology choices](#technology-choices)
- [Project layout](#project-layout)
- [Documentation](#documentation)

## How it works

ERC-20 and ERC-721 transfers are both logs with the same
`topic0 = keccak256("Transfer(address,address,uint256)")`, with `from` and `to` as
indexed topics. So **one `eth_getLogs` per block range, filtered only on `topic0`,
serves every user at once**. From each log the service keeps a single fact, the
pair `(account, token, standard)`, and nothing else.

- **Discovery** is a local prefix scan with zero RPC calls.
- **Balances** are read live through Multicall3 and cached with event-driven
  invalidation, so a cached balance stays valid until a transfer touches it.
- **The two standards** are told apart by log shape, at no RPC cost: ERC-20 has 3
  topics and 32 bytes of data, ERC-721 has 4 topics and empty data.

[`docs/architecture.md`](docs/architecture.md#how-each-part-works) covers each part
in depth.

## Quick start

Requires Go 1.26 and an Ethereum JSON-RPC endpoint.

```sh
cp .env.example .env        # then set RPC_URL
make                        # fmt, vet, test, build

set -a; . ./.env; set +a    # the binary reads the environment only, not .env

./bin/tokendiscovery backfill -from-block 26290000   # a short demo seed, a few minutes
./bin/tokendiscovery serve

curl "localhost:8080/v1/chains/ethereum/accounts/0x9De929252c9017FE8A4Cd5986475a658aD87Ce33/tokens?balances=true"
```

### Commands

| Command | Does | Flags |
|---|---|---|
| `backfill` | Seeds the store from the start block to the tip. The full span takes ~85 min and ~1.9k requests. | `-from-block N` seeds a shorter span |
| `serve` | Follows the tip and serves the API, `/healthz`, `/readyz` and `/metrics`. | `-addr` overrides `HTTP_ADDR` |
| `checkpoint` | Clones the store for a new replica. Needs the service stopped, because it takes the store's directory lock. | `-out DIR`, required |
| `version` | Prints build information. | |

`backfill` must complete before `serve` will start. Serving a half-indexed store
would return confidently wrong *empty* token lists, so it is refused. An interrupted
`backfill` resumes when re-run, because the cursor is committed with every range.

`/readyz` reports the indexer's own position: an instance that is up but behind the
tip reports not ready, so it can be taken out of rotation.

## Configuration

All configuration comes from environment variables. The binary does not load `.env`
itself, so export it first as shown in the [quick start](#quick-start).

### Service

| Variable | Default | Description |
|---|---|---|
| `RPC_URL` | none, **required** | JSON-RPC endpoint, `http://` or `https://`. Usually carries an API key, so it is never logged in full. |
| `CHAIN` | `ethereum` | Network preset: `ethereum` or `linea`. Must match the endpoint. |
| `DATA_DIR` | `./data-<chain>` | Directory of the Pebble store. |
| `HTTP_ADDR` | `:8080` | Listen address for the API, health checks and metrics. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `LOG_FORMAT` | `text` | `text` or `json`. |

### Chain and RPC tuning

These override a value from the preset that `CHAIN` selects, and rarely need changing.

| Variable | `ethereum` | `linea` | Description |
|---|---|---|---|
| `CHAIN_START_BLOCK` | `25933634` | `31972391` | First block to index. Must not be zero. |
| `CHAIN_LOG_RANGE_BLOCKS` | `200` | `2000` | Blocks per `eth_getLogs` request, from 1 to 10000. |
| `CHAIN_TIP_POLL` | `4s` | `4s` | How often the head is polled, as a Go duration. Must be positive. |
| `CHAIN_WATERMARK_STRATEGY` | `consensus-tags` | `consensus-tags` | How safety watermarks are derived: `consensus-tags`, `immediate-finality` or `block-count`. |
| `RPC_RATE` | `25` | `25` | Client-side rate limit in requests per second, shared by the indexer and balance reads. Must be positive. |

### Behaviour

- An empty value counts as unset.
- An invalid value stops startup with an error saying what is wrong.
- Every override except `RPC_URL` is logged at startup as `env_overrides`, and the
  endpoint is logged with everything after the host removed.
- `RPC_URL` is required by every command except `version`, including `checkpoint`,
  which makes no RPC calls.
- `tools/measure`, `tools/loadtest` and `make test-integration` also read `RPC_URL`.
  The integration tests are skipped without it.

Every other value, such as RPC burst, timeouts and Multicall3 batch size, is set per
network in [`internal/config`](internal/config/config.go).

### Running another network

```sh
export CHAIN=linea RPC_URL=https://rpc.linea.build
./bin/tokendiscovery backfill                # ~250 requests
./bin/tokendiscovery serve -addr :8081
curl "localhost:8081/v1/chains/linea/accounts/<address>/tokens?balances=true"
```

One process serves one chain, so a second chain is a second process. Each chain
defaults to its own store, and `eth_chainId` is checked against the preset before the
store is opened, so a mismatched endpoint is a startup error rather than a mislabelled
index.

Linea is the network Trezor Suite does not support yet that best tests the adapter
design: Suite already signs for any EVM chain ID, so the backend is the only missing
piece. The adapter, decoder, store and API run unchanged, and adding it took one preset
in `internal/config`. Its log range is sized to its lower traffic, but the value that
really had to differ is the finality-stall threshold: as a zk-rollup it finalises about
4 hours behind the head rather than about 13 minutes. See the
[measurements](docs/measurements.md#second-network-linea) and
[limits](docs/limits.md#chain-and-reorgs).

## API

The full contract is in [`docs/openapi.yaml`](docs/openapi.yaml).

```
GET /v1/chains                                    configured networks
GET /v1/chains/{chain}/accounts/{address}/tokens  ERC-20 discovery
GET /v1/chains/{chain}/accounts/{address}/nfts    ERC-721 discovery
GET /v1/status                                    what this instance has indexed
GET /healthz · /readyz · /metrics                 operational
```

### ERC-20: `/tokens`

In steady state, plain discovery costs one Pebble prefix scan and zero RPC calls.

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

| Parameter | Description |
|---|---|
| `limit` | Page size, 1–1000, default 100. |
| `cursor` | The `nextCursor` from the previous page. |
| `balances` | Include each token's current balance and metadata. The only parameter that can spend RPC. |
| `include_zero` | Keep tokens whose balance reads zero, which are dropped by default. |

Reading the response:

- **`block.height`** is the height the data is valid for, taken from the index cursor
  rather than a live `eth_blockNumber`, so it describes the data actually returned.
- **`block.unconfirmed`** means the height is above the safe watermark, so a token
  could still be reorged out. The pair set is add-only, so the risk is an extra token,
  never a missing one.
- **`freshness`** is in every response, because "this account holds no tokens" and
  "this instance is 500 blocks behind" must never look the same.
- **`balance.raw`** is a decimal string, because a uint256 does not fit in a float64.

A balance carries one of three statuses:

| `status` | Meaning |
|---|---|
| `ok` | Current: no transfer has touched the pair since it was read, and it was read within the last day. |
| `stale` | A real value that a later transfer invalidated. |
| `unavailable` | No value, and never a zero standing in for one. |

### ERC-721: `/nfts`

```sh
curl "localhost:8080/v1/chains/ethereum/accounts/0xDa021A2C1C2F99574D861Ef8c6fA745851BEB7F6/nfts?counts=true&token_ids=true"
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

| Parameter | Description |
|---|---|
| `counts` | Include the live owned count per collection. The only parameter that can spend RPC. |
| `token_ids` | Include the token IDs the index knows of, at most 100 per collection. |
| `include_zero` | Keep collections whose count reads zero, which are dropped by default. |
| `limit`, `cursor` | As for `/tokens`. |

**The count and the list can disagree.** `ownedCount` is a live `balanceOf`.
`knownTokenIds` comes from indexed transfers, so it covers only the seeded window, and
it stops at finality, up to `tokenIdsThroughBlock`, about 13 minutes behind the head.
The reasoning is in [`docs/architecture.md`](docs/architecture.md#erc-721).

### Errors and caching

Errors are RFC 9457 problem documents with a stable `type`. A mixed-case address that
fails its EIP-55 checksum is rejected, because the alternative is answering a typo with
a valid empty token list:

```json
{ "type": "https://trezor.io/errors/address-checksum",
  "title": "Invalid account address", "status": 400,
  "detail": "address failed EIP-55 checksum verification",
  "requestId": "Deving/a3uWtaLyJm-000008" }
```

Every response carries an `ETag` and honours `If-None-Match`, which saves bandwidth
rather than RPC, as `td_http_bytes_saved_by_304_total` shows. On discovery routes it is a weak validator that leaves out the
heights, so a client polling once a minute can still get a 304 while the head moves.
The current head travels in `X-Block-Number` and `X-Block-Hash`, including on the 304.

## Handling an unreliable RPC

Each failure is handled where it is cheapest, not in one catch-all:

| | |
|---|---|
| Slow | Per-method deadlines. `eth_getLogs` gets the long one, and header reads fail fast so a tick can retry. |
| Erroring | The client retries a *call* with full-jitter backoff, and the indexer retries a *range*. Neither kills the follower, which keeps trying while `/readyz` reports not ready. |
| Rate limiting | A client-side token bucket that **shrinks on a 429**, so the service settles below the provider's limit instead of bouncing off it. |
| Range too large | Classified as "make it smaller", including a bare HTTP 413, and the scan range halves for the rest of the run. |
| Behind the tip | Every request is clamped to the head the node itself reported. A head that moves *backwards* (a lagging pool member) retargets the sweep instead of failing it. |
| Lying | Logs are checked against the header's block hash, since `eth_getLogs` carries no `removed` flag. A mismatch is counted and refused rather than indexed. |

Steady state costs about **20 requests a minute**: one head poll per tick, one
`eth_getLogs` per block, and the safe and finalized tags on an epoch cadence. That
figure does not depend on the number of users. See the
[RPC budget](docs/rpc-budget.md).

## Measured results

Against Ethereum mainnet on 2026-10-29 and 2026-10-30:

| | |
|---|---|
| Transfer logs per block | **574** |
| ERC-20 : ERC-721 split | **98.4% : 1.6%** |
| Logs unclassifiable by shape | **3 in 1,148,860**, which is why there is no ERC-165 probe |
| `Accept-Encoding: gzip` | **14.5× smaller, 12× faster** |
| `eth_getLogs` limits | A 10,000-**block** range cap, *no* result cap, and a bare HTTP 413 past ~200k blocks |
| Concurrency | **Flat** (238 blocks/s at 1 worker, 278 at 16), so the backfill is a sequential loop |
| Backfill, 14,143 blocks | **2m52s**, 71 `eth_getLogs` calls, 330 MB store |
| Full backfill, 371,574 blocks | **1h23m48s** at 74 blocks/s, ~1.9k `eth_getLogs` calls, 205M logs, **3.5 GB** store |
| Balances, cold page of 95 tokens | **0.40 s**, 2 `eth_call`s |
| Balances, warm page | **~2 ms**, 0 `eth_call`s |

The method and full results are in [`docs/measurements.md`](docs/measurements.md), and
the end-to-end runs are in
[`docs/architecture.md`](docs/architecture.md#measured-end-to-end).

## Technology choices

| | Why | Rejected |
|---|---|---|
| **Go** | The work is I/O-bound fan-out and streaming millions of logs: goroutines, low memory, one static binary. | — |
| **Pebble** | Embedded, so a read never crosses a network and there is no shared database to bottleneck. Sorted keys make `account → tokens` one seek, an LSM suits the write-heavy backfill, merge operators keep that path read-free, and hardlinked checkpoints make cloning a store cheap. | BoltDB is too slow under write load. Badger is viable, but its checkpointing is weaker. |
| **No Postgres, no Redis** | The index and balance cache share one local store, so invalidation is a local write in the same atomic batch, not a distributed-cache problem. There is nothing to deploy alongside. | Shared state is what you would add for a leader/replica split, and nothing below that needs it. |
| **Multicall3** | The largest saving on the read path: N `balanceOf` in one `eth_call`, on the same address on most EVM chains. | Per-token calls, or JSON-RPC batches, which providers bill per call. |
| **REST** | The contract is tiny, and ETag/304 is a real lever for clients polling once a minute. | GraphQL gives that up for flexibility nobody here needs. |
| **Stdlib where it reaches** | `encoding/json` measured at 243 MB/s, so it is not the bottleneck. `encoding/binary` for the store, `log/slog` for logs, a few dozen lines for retries. Dependencies are `pebble`, `chi`, `prometheus/client_golang`, `x/time/rate`, `x/sync/singleflight`, `x/crypto/sha3`. | `go-ethereum`: the service needs keccak and a small ABI subset, not a client library. |

## Project layout

```
cmd/tokendiscovery/   serve · backfill · checkpoint · version
internal/core/
  domain/             chain-agnostic vocabulary (Address, AccountToken, Watermarks, …)
  ports/              the interfaces everything is built against
internal/config/      env-based configuration and network presets
internal/obs/         slog + the Prometheus metric set
internal/rpc/         JSON-RPC client: rate limit, retries, deadlines, error classification
internal/api/         router, /tokens and /nfts handlers, ETags, RFC 9457 errors
internal/balances/    balance resolution: cache, live reads, budget, degradation
internal/chains/      the registry that resolves a chain slug to its adapter
internal/chains/evm/  generic EVM adapter (shared by every EVM network), Multicall3
internal/store/       Pebble index: keyspace, merge operators, NFT ownership, checkpoints
internal/indexer/     backfill, tip follower, reorg handling, readiness
tools/measure/        measurement tool
tools/loadtest/       5,000 users polling once a minute, reporting RPC per request
testdata/             captured mainnet logs + hand-written edge cases
docs/                 architecture, limits, measurements, API contract
```

The interfaces in `internal/core/ports` are the seam for new networks: an EVM network
is a preset, and a non-EVM network is a new `ChainAdapter` implementation with storage
and API untouched. [`docs/limits.md`](docs/limits.md#portability) lists what would not
survive that move.

Run `make test` for the offline tests, `make test-race` for the race detector, and
`make test-integration` for the tests against a real endpoint.

## Documentation

- [`docs/architecture.md`](docs/architecture.md): how each part works, and whether a bad index can spread
- [`docs/limits.md`](docs/limits.md): what this does not do, and what happens at each edge
- [`docs/rpc-budget.md`](docs/rpc-budget.md): what the service costs against the naive design, and the load test
- [`docs/measurements.md`](docs/measurements.md): the measured numbers and how to reproduce them
- [`docs/openapi.yaml`](docs/openapi.yaml): the API contract
- [`docs/presentation.md`](docs/presentation.md): the key decisions, as a talk outline
- [`docs/assignment.md`](docs/assignment.md): the original task
