# near-chunks

This crate cotains functions to handle chunks. In NEAR - the block consists of multiple chunks - at most one per shard.

When chunk is created, the creator encodes it using Reed Solomon encoding (ErasureCoding) - creating PartialChunks, that are later sent to all the validators (each validator gets a subset of them).

Each validator is waiting to receive its share of PartialChunks (`has_all_parts`) - and once they are verified, it can accept the chunk (and if all chunks are accepted chain/client can sign the block).

If validator cares about the given shard (that is - it wants to have a whole content of the shard present in the local memory) - it will want to receive the necessary amount of PartialChunks to be able to reconstruct the whole chunk. As we use ReedSolomon, this means that they need `data_shard_count` PartialChunks (which is lower than `total_shard_count`). Afterwards, it will reconstruct the chunk and persist it in the local cache (ChunkCache).


## ShardsManager
Shard Manager is responsible for:

* **fetching partial chunks** - it can ask other nodes for the partial chunks that it is missing. It keeps the track fo the request via RequestPool
* **handling partial chunks requests** - when request for the partial chunk arrives, it handles reading it from store and returning to the requesting node

* **storing partial chunks** - it stores partial chunks within local cache (`stored_partial_encoded_chunks`)

* **validating chunks** - once it receives correct set of partial chunks for the chunk, it can 'accept the chunk' (which means that validator can sign the block if all chunks are accepted)

* **storing full chunks** - full chunks (that are reconstructed from partial chunks if it gets enough parts) are stored in ChunkCache, and are later used to answer queries from client library. This happens only for the shards that given node cares about.

## ChunkCache

Cache for chunks that we've seen - also stores the mapping from the blocks to the chunk headers.

## RequestPool

Tracks the requests for chunks that were sent - so that we don't resend them unless enough time elapsed since the last attempt.
