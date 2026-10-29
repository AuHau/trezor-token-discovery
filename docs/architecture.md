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