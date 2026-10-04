# High level overview

When mainnet launched, the neard client stored all the chain's state in a single
RocksDB column `DBCol::State`. This column embeds the entire [NEAR state
trie](./trie_storage.md) directly in the key-value database, using roughly
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
rough ideas on how to do it. But we don't know the performance we should expect.
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

This is the main structure that tracks which blocks are supported by flat storage for a single
`ShardUId` and how to answer state queries for them. Conceptually it keeps two kinds of data:

* the **flat head**, a `BlockInfo` describing the block whose full key/value mapping is stored in
  `DBCol::FlatState`, and
* a set of **cached deltas** (`FlatStateDelta`) for blocks above the flat head, stored both on
  disk and in memory.

`FlatStorage` is shared across multiple threads (chain processing, chunk application, view
clients), so it is wrapped in `RwLock` and all operations are done through `&self`.

The most important methods are:

* `add_delta(delta: FlatStateDelta)` — records state changes for a newly processed block. The
  method writes the delta and its metadata to `DBCol::FlatStateChanges`/`DBCol::FlatStateDeltaMetadata`
  and caches it in memory. It ensures that the new block is reachable from the current flat head.
* `update_flat_head(block_hash: &CryptoHash)` — advances the flat head towards the given block.
  Internally it finds a path from the current flat head to `block_hash`, applies the corresponding
  deltas to `DBCol::FlatState`, updates `DBCol::FlatStorageStatus` with the new `flat_head`, and
  garbage‑collects old deltas from disk and memory.
* `clear_state(store_update: &mut FlatStoreUpdateAdapter)` — removes all flat state values and
  deltas for this shard from the database and marks the status as `FlatStorageStatus::Empty`.
* `get_head_hash()` — returns the hash of the current flat head block.
* `set_flat_head_update_mode(enabled: bool)` — turns advancing the flat head on or off. When
  disabled, calls to `update_flat_head` become no‑ops. This is used, for example, while taking
  state snapshots so that flat storage does not move ahead.

## FlatStorageManager

`FlatStorageManager` owns `FlatStorage` instances for all shards known to the runtime and provides
thread‑safe access to them. It is responsible for wiring flat storage into block processing,
initialization and resharding logic.

Its main responsibilities are:

* **Initialization**
  * `set_flat_storage_for_genesis(...)` — sets up the initial `FlatStorageStatus::Ready` for a
    shard at the genesis block in an empty database.
  * `create_flat_storage_for_shard(shard_uid)` — constructs an in‑memory `FlatStorage` for the
    given shard based on the status and deltas stored on disk and registers it in the manager.

* **Tracking per‑block changes and advancing the head**
  * `save_flat_state_changes(block_hash, prev_hash, height, shard_uid, state_changes)` — converts
    `RawStateChangesWithTrieKey` for a block into a `FlatStateDelta` and either
    * calls `FlatStorage::add_delta` if flat storage is already loaded for this shard, or
    * persists the delta on disk so it can be picked up later when flat storage is created.
    It returns a `FlatStoreUpdateAdapter` that must be committed together with trie changes.
  * `update_flat_storage_for_shard(shard_uid, new_flat_head)` — asks the corresponding
    `FlatStorage` to advance its flat head to (or towards) `new_flat_head`, applying and
    garbage‑collecting deltas as needed. Fork‑related `BlockNotSupported` errors are logged and
    ignored; other errors are treated as fatal.

* **Serving read views**
  * `chunk_view(shard_uid, block_hash) -> Option<FlatStorageChunkView>` — if flat storage exists
    for the shard, returns a `FlatStorageChunkView` that can be attached to a `Trie` to answer
    reads for the state as of `block_hash`. If flat storage is still being created in the
    background, returns `None`.

* **Lifecycle, resharding and snapshots**
  * `remove_flat_storage_for_shard(shard_uid, store_update)` — removes the `FlatStorage` object
    from memory and calls `FlatStorage::clear_state` to delete all related data from disk.
  * `resharding_catchup_height_reached(...)` — inspects `FlatStorageStatus` for a set of shards
    undergoing resharding and reports how far their resharding catchup has progressed.
  * `want_snapshot(block_hash, min_chunk_prev_height)` and `snapshot_taken(block_hash)` — coordinate
    state snapshots with flat storage. While a snapshot for `block_hash` is pending, the manager
    disables advancing flat heads on all `FlatStorage` instances; once `snapshot_taken` is called
    for the same block hash, head updates are re‑enabled.
  * `snapshot_height_wanted()` / `snapshot_hash_wanted()` — expose the requested snapshot point to
    other components (for example, resharding logic) so they do not advance beyond it.

## FlatStorageChunkView

`FlatStorageChunkView` is a lightweight read‑only view over flat storage for a given shard and a
given block. It is created by `FlatStorageManager::chunk_view` and is then passed into `Trie` so
that trie lookups can be served from flat storage instead of walking the on‑disk trie.

The core methods are:

* `get_value(&self, key: &[u8]) -> Result<Option<FlatStateValue>, StorageError>` — returns the
  flat‑state value for a raw trie key as of the block associated with this view. The return type is
  `FlatStateValue`, which is either a `Ref(ValueRef)` or an `Inlined(Vec<u8>)`. In the trie layer
  this is converted into an `OptimizedValueRef`, so that inlined values can be used directly while
  still preserving correct gas accounting.
* `contains_key(&self, key: &[u8]) -> Result<bool, StorageError>` — checks whether the given key
  is present in the flat state for this block.
* `iter_range(&self, from: Option<&[u8]>, to: Option<&[u8]>) -> FlatStateIterator` — iterates over
  flat‑state entries in a given key range for the shard, reading from `DBCol::FlatState` at the
  current flat head. This is primarily used for range queries and tools.
* `get_head_hash(&self) -> CryptoHash` and `shard_uid(&self) -> ShardUId` — expose the underlying
  flat storage head and shard identity for observability and debugging.

## Other notes

### Chain dependency

If storage is fully empty, then we need to create flat storage from scratch. FlatStorage is stored
inside NightshadeRuntime, and it itself is stored inside Chain, so we need to create them in the same order
and dependency hierarchy should be the same. But at the same time, we parse genesis file only during Chain
creation. That’s why FlatStorageManager has set_flat_storage_for_genesis method which is called
during Chain creation.

### Regular block processing vs. catchups

For these two use cases we have two different flows: first one is handled in Chain.postprocess_block,
the second one in Chain.block_catch_up_postprocess. Both, when results of applying chunk are ready,
should call Chain.process_apply_chunk_result → RuntimeAdapter.get_flat_storage_for_shard →
FlatStorage.add_block, and when results of applying ALL processed/postprocessed chunks are ready,
should call RuntimeAdapter.get_flat_storage_for_shard → FlatStorage.update_flat_head.

(because applying some chunk may result in error and we may need to exit there without updating flat head - ?)
