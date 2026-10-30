# Limits

What this PoC does not do, what happens at each edge, and what lifting the limit
would take. Numbers come from [measurements](measurements.md) and the
[RPC budget](rpc-budget.md).

## What the data can and cannot say

| Limit | What happens | To lift it |
|---|---|---|
| **Window.** Nothing before the start block (25,933,634) is indexed. | A token acquired earlier and not touched since is invisible, even with a non-zero balance. This is the assignment's scope, and the biggest thing a real wallet would notice. | Backfill further back, or enrich per account on first request. |
| **Events only.** Discovery reads `Transfer` logs. | Tokens that never emit a standard `Transfer` are missed. Approval-only interactions are not "interactions" here. | Add `Approval`'s topic to the same scan. |
| **Classification by log shape.** No ERC-165 probe. | 3 logs in 1,148,860 did not match. They were fully unindexed `Transfer`s with no account topics, so they are skipped, not misfiled. A contract that lies about its shape lands in the wrong standard. | One cached call per new token. |
| **ERC-1155** is not indexed. | Those tokens do not appear. | A second topic and a batch-aware decoder. |
| **Spam is not filtered.** | Airdropped tokens appear. Without balances they are all listed; `balances=true` hides those provably at zero, not those that hold a worthless balance. | A curated list on the client or in front of the API. |

## Balances

| Limit | What happens |
|---|---|
| **Rebasing tokens can be up to a day out of date while reading `ok`.** | stETH and aTokens change without a `Transfer`, so nothing invalidates them, and a clean entry is only re-read after 7,200 blocks. This is the one case where `ok` overstates. Summing transfers would be worse: wrong for the same tokens and for fee-on-transfer tokens, with drift that never heals. A shorter limit for known rebasers would tighten it. |
| **Budget.** A request waits 800 ms for live reads. | Past that, a token is `stale` or `unavailable`, never an error and never a guessed zero, and the read carries on to fill the cache for the next poll. A cold cache at full load hits this, as measured in the [load test](rpc-budget.md). |
| **Rate limit.** The RPC client's bucket is 25 requests/s, shared with the indexer. | It sets how fast a cold cache can fill. At most about a second's worth of reads run at once, and a request past that is answered from the cache at once, degraded the same way. |
| **Coalescing is per page.** | Concurrent requests for the same page share a read. Overlapping pages of different sizes do not. Batching across accounts would cut a cold-cache stampede further and is not built. |
| **Reverting contracts** are not retried for 10 minutes. | A contract that recovers shows `unavailable` for up to that long. The memory is per process and bounded. |
| **Gas burners.** | One that sinks its batch is isolated by bisection. One that does not can make later tokens in the batch fail; balances recover after the 10-minute window, but metadata recorded as absent stays absent for the life of the store. |
| **Pinning** needs state the node still holds. | Outside its window the read retries at `latest` and reports the block it ran at. A result behind the page's height is `stale`. A result ahead of the index is served but not cached. |

## ERC-721

| Limit | What happens |
|---|---|
| **The ID list trails the head** by about two epochs (~13 min), up to ~19 min with the cached tag. | `ownedCount` is live, so the two can disagree, and `tokenIdsThroughBlock` says how far the list has come. |
| **The list only knows the window.** | An NFT acquired before the start block is counted but not listed. |
| **100 IDs per collection,** with no way to page further. | `tokenIdsTruncated` says so. |
| **Ownership comes from events.** | A non-compliant contract gets a wrong list, for its own collection only. |

## Chain and reorgs

| Limit | What happens |
|---|---|
| **A reorg can leave an extra token.** | Pairs are not deleted on a rewind, so a token seen only on an orphaned branch stays listed. With balances it reads zero and is filtered. |
| **Any reorg flushes the balance cache.** | Simple and correct, at the cost of one round of re-reads. |
| **A reorg deeper than the 128-header window is fatal,** as is one below the ownership view. | The service stops instead of indexing an unverified branch or reporting owners from a dead one. On mainnet this means a lying provider or broken finality, and the store has to be rebuilt. |
| **A real finality stall** falls back to a 12-block depth. | Serving continues, but the ownership view's guarantee is only as good as that depth until finality returns. The threshold is per chain: 256 blocks on Ethereum, 21,600 on Linea, where finality normally trails by ~4 h. |
| **On Linea, `unconfirmed` lasts hours.** | Linea's `safe` equals `finalized` (proven on L1), so `block.unconfirmed` is true for anything newer than the last proof. That is accurate, but it means something different from Ethereum's ~6 minutes. The ERC-721 ownership view trails by the same ~4 h. |
| **On Linea, the sequencer is trusted up to finality.** | Linea has a single sequencer and in practice does not reorg, but nothing stops it from doing so before a proof lands. The usual reorg handling still runs. |
| **A restart on Linea catches up block by block above `safe`.** | Bulk catch-up stops at `safe`, which is ~1,700 blocks behind, so the rest costs one header and one `eth_getLogs` per block: ~3.4k calls and a few minutes before `/readyz`. |

## Provider

| Limit | What happens | To lift it |
|---|---|---|
| **One RPC endpoint.** No failover. | If it is down, the index stalls and `/readyz` goes not-ready. Discovery keeps answering from the store, and balances degrade. | A provider pool in `internal/rpc`. The highest-value hardening step, and the port does not change. |
| **A lagging provider cannot be detected.** | `lagBlocks` is measured against the head that same node reports, so it reads 0 when the node itself is behind. A node that stops answering is caught, because readiness also checks the age of the last successful poll. | The same pool: `max(heads)` across independent providers. Even then, all of them lagging together reads 0. |
| **Tip lag.** | A 4 s poll plus one `eth_getLogs`, so a transfer is indexed seconds after its block. | WebSocket `newHeads`. |

## Operations and scale

| Limit | What happens | To lift it |
|---|---|---|
| **Two-step startup.** | `serve` refuses a store that never finished a backfill, so a new deployment waits for it (~85 min) or starts from a checkpoint. | — |
| **`checkpoint` needs the service stopped.** | Pebble locks the directory, so the command fails against a running `serve`. Cloning a warm store means taking an instance out of rotation. | An admin endpoint on `serve`: `Store.Checkpoint` already works on an open store. |
| **Each instance runs its own indexer.** | Reads scale by adding instances from a checkpoint, but tip-following RPC grows with them too. | A leader that indexes, and read replicas fed by a changefeed. |
| **A schema or decoder change means a rebuild.** | An old store is refused rather than misread. Rebuilding costs one backfill. | Deliberately not migrated. |
| **Cross-provider audit is not automated.** | Per-block digests are stored and a parser mismatch is refused, but nothing compares digests across providers, and checkpoints carry no manifest. So a bad index would be copied into clones without being noticed. See [architecture](architecture.md#can-a-bad-index-propagate-into-clones). | A second endpoint in an audit job. |

## Portability

Adding an EVM network is a preset in `internal/config`, selected with `CHAIN`.
Ethereum and Linea have both been run (see
[measurements](measurements.md#second-network-linea)). One process serves one
chain from its own data directory, and a second chain is a second process. The
store and API are already chain-keyed, so serving several chains from one process
needs only the wiring in `cmd/`. At boot, `eth_chainId` is checked against the
configured chain before the store is opened, so a mismatched `RPC_URL` cannot
index one network under another's ID.

A non-EVM network is a new `ChainAdapter`, with these parts not surviving the move:

- `domain.Address` is a fixed 20 bytes. Solana and Sui use 32, so it would have to
  become variable-width, which reaches the store's key codec.
- EIP-55 validation and the `Transfer`-log model are EVM concepts. Chains whose
  RPC already lists holdings by owner would not need this service's index at all.
