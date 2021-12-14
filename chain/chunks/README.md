# near-chunks

This crate cotains functions to handle chunks. In NEAR - the block consists of multiple chunks - at most one per shard.

When a chunk is created, the creator encodes its contents using Reed Solomon encoding (ErasureCoding) and adds cross-shard receipts - creating PartialEncodedChunks that are later sent to all the validators (each validator gets a subset of them). This is done for data availability reasons (so that we need only a part of the validators to reconstruct the whole chunk). You can read more about it in the Nightshade paper (https://near.org/nightshade/)


A honest validator will only approve a block if it receives its assigned parts for all chunks in the block - which means that for each chunk, it has `has_all_parts()` returning true. 

For all nodes (validators and non-validators), they will only accept/process a block if the following requirements are satisfied:

* for every shard it tracks, a node has to have the full chunk,
* for every shard it doesn't track, a node has have the receipts from this shard to all shards 


If node tracks given shard (that is - it wants to have a whole content of the shard present in the local memory) - it will want to receive the necessary amount of PartialChunks to be able to reconstruct the whole chunk. As we use ReedSolomon, this means that they need `data_shard_count` PartialChunks (which is lower than `total_shard_count`). Afterwards, it will reconstruct the chunk and persist it in the local storage (via chain/client).

When the PartialEncodedChunk is received (in chain/client) - it will try to validate it immediately, if the previous block is already available (via `process_partial_encoded_chunk()`) or store it for future validation (via `store_partial_encoded_chunk()`).

## ShardsManager
Shard Manager is responsible for:

* **fetching partial chunks** - it can ask other nodes for the partial chunks that it is missing. It keeps the track of the requests via RequestPool and can be asked to resend them when someone calls `resend_chunk_requests` (this is done periodically by the client actor).
* **storing partial chunks** - it stores partial chunks within local cache before they can be used to reconstruct for the full chunk.   
  `stored_partial_encoded_chunks` stores non-validated partial chunks while `ChunkCache` stores validated partial chunks. (we need the previous block in order to validate partial chunks). This data is also used when other nodes are requesting a partial encoded chunk (see below).
* **handling partial chunks requests** - when request for the partial chunk arrives, it handles reading it from store and returning to the requesting node
* **validating chunks** - once it receives correct set of partial chunks for the chunk, it can 'accept the chunk' (which means that validator can sign the block if all chunks are accepted)
* **storing full chunks** - it stores the full chunk into our storage via `decode_and_persist_encoded_chunk()`, which calls store_update's `save_chunk()`


## ChunkCache

Cache for validated partial chunks that we've seen - also stores the mapping from the blocks to the chunk headers.

## RequestPool

Tracks the requests for chunks that were sent - so that we don't resend them unless enough time elapsed since the last attempt.