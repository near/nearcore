Here we describe structures used for flat storage implementation.

## FlatStorage

This is the main structure which owns information about ValueRefs for all keys from some fixed 
shard for some set of blocks. It is shared by multiple threads, so it is guarded by RwLock:

* Chain thread, because it sends queries like:
    * "new block B was processed by chain" - supported by add_block
    * "flat storage head can be moved forward to block B" - supported by update_flat_head
* Thread that applies a chunk, because it sends read queries "what is the ValueRef for key for block B"
* View client (not fully decided)

Requires ChainAccessForFlatStorage on creation because it needs to know the tree of blocks after
the flat storage head, to support get queries correctly.

## FlatStorageManager

It holds all FlatStorages which NightshadeRuntime knows about and:

* provides views for flat storage for some fixed block - supported by new_flat_state_for_shard
* sets initial flat storage state for genesis block - set_flat_storage_for_genesis
* adds/removes/gets flat storage if we started/stopped tracking a shard or need to create a view - add_flat_storage_for_shard, etc.

## FlatStorageChunkView

Interface for getting ValueRefs from flat storage for some shard for some fixed block, supported
by get_ref method.

## FlatStorageCreator

Creates flat storages for all tracked shards or initiates process of background flat storage creation if for some shard we have only Trie but not FlatStorage. Supports update_status which checks background job results and updates creation statuses and should create flat storage when all jobs are finished.

## Other notes

### Chain dependency

If storage is fully empty, then we need to create flat storage from scratch. FlatStorage is stored
inside NightshadeRuntime, and it itself is stored inside Chain, so we need to create them in the same order
and dependency hierarchy should be the same. But at the same time, we parse genesis file only during Chain
creation. That’s why FlatStorageManager has set_flat_storage_for_genesis method which is called 
during Chain creation.

### Regular block processing vs. catchups

For these two usecases we have two different flows: first one is handled in Chain.postprocess_block,
the second one in Chain.block_catch_up_postprocess. Both, when results of applying chunk are ready,
should call Chain.process_apply_chunk_result → RuntimeAdapter.get_flat_storage_for_shard → 
FlatStorage.add_block, and when results of applying ALL processed/postprocessed chunks are ready,
should call RuntimeAdapter.get_flat_storage_for_shard → FlatStorage.update_flat_head.

(because applying some chunk may result in error and we may need to exit there without updating flat head - ?)
