# High level overview

When mainnet launched, the neard client stored all the chain's state in a single
RocksDB column `DBCol::State`. This column embeds the entire [NEAR state
trie](./trie.md) directly in the key-value database, using roughly
`hash(borsh_encode(trie_node))` as the key to store a `trie_node`. This gives a
content-addressed storage system that can easily self-verify.

Flat storage is a bit like a database index for the values stored in the trie.
It stores a copy of the data in a more accessible way to speed up the lookup
time. 

Drastically oversimplified, flat storage uses a hashmap instead of a trie. This
reduces the lookup time from `O(d)` to `O(1)` where `d` is the tree depth of the
value.

But the devil is in the detail. Below is a high-level summary of implementation
challenges, which are the reasons why the above description is an
oversimplification.

## Time dimension of the state

The blockchain state is modified with every chunk. Neard must be able to travel
back in time and resolve key lookups for older versions, as well as travel to
alternative universes to resolve requests for chunks that belong to a different
fork.

Using the full trie embedding in RocksDB, this is almost trivial. We only need
to know the state root for a chunk and we can start traversing from the root to
any key. As long as we do not delete (garbage collect) unused trie nodes, the
data remains available. The overhead is minimal, too, since all the trie nodes
that have not been changed are shared between tries.

Enter flat storage territory: A simple hashmap only stores a snapshot. When we
write a new value to the same key, the old value is overwritten and no longer
accessible. A solution to access different versions on each shard is required.

The minimal solution only tracks the final head and all forks building on top. A
full implementation would also allow replaying older chunks and doing view
calls. But even this minimal solution pulls in all complicated details regarding
consensus and multiplies them with all the problems listed below.

## Fear of data corruption

State and FlatState keep a copy of the same data and need to be in sync at all
times. This is a source for errors which any implementation needs to test
properly. Ideally, there are also tools to quickly compare the two and verify
which is correct.

Note: The trie storage is verifiable by construction of the hashes. The flat
state is not directly verifiable. But it is possible to reconstruct the full
trie just from the leaf values and use that for verification.

## Gas accounting requires a protocol change

The trie path we take in a trie lookup affects the gas costs, and hence the
balance subtracted from the user. In other words, the lookup algorithm leaks
into the protocol. Hence, we cannot switch between different ways of looking up
state without a protocol change.

This makes things a whole lot more complicated. We have to do the data migration
and prepare flat storage, while still using trie storage. Keeping flat storage
up to date at this point is pure overhead. And then, at the epoch switch where
the new protocol version begins, we have to start using the new storage in all
clients simultaneously. Anyone that has not finished migration, yet, will fail
to produce a chunk due to invalid gas results.

In an ideal future, we want to make gas costs independent of the position in the
trie and then this would no longer be a problem.

## Performance reality check

Going from `O(d)` to `O(1)` sounds great. But let's look at actual numbers.

A flat state lookup requires exactly 2 database requests. One for finding the
`ValueRef` and one for dereferencing the value. (Dereferencing only happens if
the value is present and if enough gas was paid to cover reading the potentially
large value, otherwise we short-circuit.)

A trie lookup requires exactly `d` trie node lookups where `d` is the depth in
the trie, plus one more for dereferencing the final value.

Clearly, `d + 1` is worse than `1 + 1`, right? Well, as it turns out, we can
cache the trie nodes with surprisingly high effectiveness. In mainnet
workloads (which were often optimized to work well with the trie shape) we
observe a 99% cache hit rate in many cases. 

Combine that with the fact that a typical value for `d` is somewhere between 10
and 20. Then we may conclude that, in expectation, a trie lookup (`d * 0.01 + 1`
requests) requires less DB requests than a flat state lookup (`1 + 1` requests). 

In practice, however, flat state still has an edge over accessing trie storage
directly. And that is due to two reasons.

1. DB keys are in better order, leading to more cache hits in RocksDB's block cache.
2. The flat state column is much smaller than the state column.

We observed a speedup of 100x and beyond for reading a single value from
`DBCol::FlatState` compared to reading it from `DBCol::State`. So that is
clearly a win. But one that is due to the data layout inside RocksDB, not due to
algorithmic improvements.

## Updating the Merkle tree

When we update a value in the blockchain state, all ancestor nodes change their
value due to the recursive nature of Merkle trees.

Updating flat state is easy enough but then we would not know the new state
root. Annoyingly, it is rather important to have the state root in due time, as
it is included in the chunk header.

To update the state root, we need to read all nodes between the root and all
changed values. At which point, we are doing all the same reads we were trying
to avoid in the first place.

This makes flat state for writes algorithmically useless, as we still have to do
`O(d)` requests, no matter what. But there are potential benefits from data
locality, as flat state stores values sorted by a trie key instead of a
perfectly random hash.

For that, we would need a fast index not only for the state value but also for
all intermediate trie nodes. At this point, we would be back to a full embedding
of the trie in a key-value store / hashmap. Just changing the keys we use for
the database.

Note that we can enjoy the benefits of read-only flat storage to improve read
heavy contracts. But a read-modify-write pattern using this hybrid solution is
strictly worse than the original implementation without flat storage.

# Implementation status and future ideas

As of March 2023, we have implemented a read-only flat storage that only works
for the frontier of non-final blocks and the final block itself. Archival calls
and view calls still use the trie directly.

## Things we have solved

We are fairly confident we have solved the time-travel issues, the data
migration / protocol upgrade problems, and got a decent handle on avoiding
accidental data corruption.

This improves the worst case (no cache hits) for reads dramatically. And it
paves the way for further improvements, as we have now a good understanding of
all these seemingly-simple but actually-really-hard problems.

## Flat state for writes

How to use flat storage for writes is not fully designed, yet, but we have some
rough ideas how to do it. But we don't know the performance we should expect.
Algorithmically, it can only get worse but the speedup on RocksDB we found with
the read-only flat storage is promising. But one has to wonder if there are not
also simpler ways to achieve better data locality in RocksDB.

## Inlining values

We hope to get a jump in performance by avoiding dereferencing `ValueRef` after
the flat state lookup. At least for small values  we could store the value
itself also in flat state. (to be defined what small means)

This seems very promising because the value dereferencing still happens in the
much slower `DBCol::State`. If we assume small values are the common case, we
would thus expect huge performance improvements for the average case.

It is not clear yet, if we can also optimize large lookups somehow. If not, we
could at least charge them at a higher rate than we do today, to reflect the
real DB cost better.

# Code guide

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
* adds/removes/gets flat storage if we started/stopped tracking a shard or need to create a view - create_flat_storage_for_shard, etc.

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
