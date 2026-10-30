# Architecture
This document layouts the solution architecture for task specified in [assignment](assignment.md).

## Notes

Some notes for setting up the doc:

- #### RPC
	- Support multiple RPC providers in order to avoid when some if down or lagging
	- There will be a primary RPC that will be used for full indexing, and backup RPCs will be used to listen for new blocks.
- #### Datastructure
	- Non-structured data storage, because it can be easily extended to different token standards (ERC20 vs NFTs etc)
	- Most probably key-value based, so we can have instant address lookups. Something like `address->tokens`
	- A preferable data engine that can be checkpointed. The idea would be that when more instances of the backend are needed, we would checkpoint the current database that would be used as a starting point on the new scaled-up instances in order to prevent full reindexing
		- The potential issue is that if there is some wrong data indexed in the first run, this will be copied over into new instances - what checks could there be to discover this inconsistency?

## How the notes above turned out

The [README](../README.md) explains how each part works, and [limits](limits.md)
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

### Multiple RPC providers: not built

The notes wanted a primary for indexing and backups for new blocks. The PoC has
one endpoint, because a pool with head tracking, failover and cooldown is a
subsystem of its own. It stays the highest-value next step: besides failover, it
is the only way to detect a provider that is silently behind. The work is confined
to `internal/rpc`, since `ChainAdapter` does not see it.

### Storage: as the notes proposed

Key-value, with an instant `address → tokens` lookup, and checkpointable: Pebble.
The pair key puts the standard last rather than one table per standard, so a new
standard adds rows and not a schema.

### Can a bad index propagate into clones?

Yes, if it is wrong in a way nothing checks. What exists today:

1. **Per-block digests.** Every indexed block stores a content hash that does not
   depend on log order. Two honest runs over the same finalized range must agree,
   so a mismatch is unambiguous. Nothing compares them automatically yet.
2. **Version gates.** The schema and parser versions are stored and checked, so a
   store from a different build is refused rather than misread. A decoder fix bumps
   the version and forces a rebuild, which is affordable because a backfill is a
   one-off of about 1,800 calls.
3. **Logs checked against headers.** Logs carrying a block hash that disagrees with
   the header are refused.

What is missing is the part that would catch a bad clone: a job that re-scans a
sampled finalized range through a second provider and compares digests, and a
manifest stored with each checkpoint. The most useful check on top of that is a
balance cross-check, sampling accounts and asserting that a non-zero `balanceOf`
implies the pair exists. That targets the one error that really harms a user: a
false negative. A false positive is just a row the API filters out.
