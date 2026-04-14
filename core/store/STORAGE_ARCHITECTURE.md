# Storage Architecture

This document describes the internals of the storage layer in nearcore: how blockchain state is stored on disk, how the trie and refcounting system work, how flat storage accelerates reads, and how state updates flow from the runtime to RocksDB.

## Overview

The storage layer manages all persistent state for a nearcore node. At its core is a RocksDB instance organized into column families (one per `DBCol`). On top of this sit several abstractions:

- **Store / StoreUpdate** — A thin wrapper providing typed access and atomic writes via `WriteBatch`.
- **Trie** — A Merkle-Patricia trie storing contract state, with refcounted nodes in `DBCol::State`.
- **MemTries** — An in-memory copy of the trie for fast updates, used by validators.
- **Flat Storage** — A flat key→value map that bypasses trie traversal for reads.
- **Caching** — An LRU trie cache with optional async prefetching for receipts.

State modifications flow top-down: the runtime writes through `TrieUpdate`, which produces `TrieChanges` (refcount deltas), which are applied to `DBCol::State` via `StoreUpdate`, which becomes an atomic `WriteBatch` in RocksDB.

---

## Store and StoreUpdate

`Store` is a read-only handle to the database. It transparently handles RC column decoding — callers get plain values without the refcount suffix.

`StoreUpdate` is a transaction builder. It accumulates operations (set, delete, increment/decrement refcount) and applies them all at once via `commit()`, which maps to a single RocksDB `WriteBatch`. This guarantees atomicity: either all operations in an update succeed or none do.

The API enforces column semantics at runtime:
- `set()` — for regular columns; panics on RC or insert-only columns
- `insert_ser()` — for insert-only columns (written once, deleted by GC)
- `increment_refcount()` / `decrement_refcount()` — for RC columns only

---

## Database Columns

### Reference-Counted Columns

Only three columns use refcounting. All other columns use plain `set`/`delete`. Calling `set()` on an RC column or `increment_refcount()` on a non-RC column will panic at runtime.

| Column | Key | Content |
|---|---|---|
| `State` | `ShardUId + CryptoHash` | Trie nodes and values |
| `Transactions` | Transaction hash | Signed transaction data |
| `Receipts` | Receipt hash | Receipt data |

These use the RocksDB merge operator for atomic refcount updates. The value is stored alongside the refcount, and entries are cleaned up by the compaction filter when RC reaches zero.

### Insert-Only Columns

Written once during block processing, later deleted by GC: `Block`, `BlockHeader`, `BlockInfo`, `Chunks`, `InvalidChunks`, `PartialChunks`, `TransactionResultForBlock`. Using `set_ser()` instead of `insert_ser()` on them will panic.

### Other Notable Columns

| Column | Content |
|---|---|
| `TrieChanges` | Per-block, per-shard refcount deltas consumed by GC |
| `BlockRefCount` | How many blocks reference each block as `prev_hash` |
| `ChunkExtra` | Per-shard state root and other metadata after applying a chunk |
| `FlatState` | Flat storage key→value pairs |
| `FlatStateChanges` | Flat storage deltas per block |
| `BlockPerHeight` | All block hashes at a given height, grouped by epoch |

---

## Reference Counting

### How It Works

Refcounting is used for data that can be referenced by multiple blocks (e.g., a trie node shared between a canonical block and a fork, or a receipt included in multiple blocks). Instead of duplicating the data, each reference increments the refcount, and each removal decrements it. The data is only physically deleted when the refcount reaches zero.

The refcount is encoded as a little-endian i64 appended to the value bytes:

```
[value_bytes...][refcount as i64 LE]
```

`Store::get()` strips the suffix transparently, but direct RocksDB reads will see it.

### RocksDB Integration

RC columns use two custom RocksDB hooks:

- **Merge operator** — When multiple writes target the same key, the merge operator sums the refcount deltas and keeps the payload from the first operand that has one. This makes concurrent refcount updates atomic without read-modify-write cycles.
- **Compaction filter** — During RocksDB compaction, entries with RC=0 are removed. Entries with negative RC are kept (they represent an over-decrement that will be resolved by a future increment).

Negative refcounts are valid temporarily. An over-decrement creates a negative RC entry that the compaction filter preserves. This handles out-of-order merge operands in RocksDB. The entry resolves when a matching increment arrives. RC=0 entries persist until the next compaction cycle removes them.

### Refcount Lifecycle for Trie Nodes

1. **Block processing** — The runtime applies transactions and receipts, producing `TrieChanges`. New trie nodes are added via `apply_insertions()`, which increments their RC in `DBCol::State`.
2. **Fork GC** — When a non-canonical block is discarded, `revert_insertions()` decrements RC for the nodes that block added.
3. **Canonical GC** — When a canonical block ages out, `apply_deletions()` decrements RC for the old nodes it superseded.
4. **Compaction** — Once RC reaches 0, the compaction filter removes the entry from disk.

Every `increment_refcount` during block processing must eventually have a matching `decrement_refcount` during GC. Breaking this symmetry corrupts state.

---

## The Trie

NEAR uses a Merkle-Patricia trie to store all contract state. Each shard has its own trie, identified by a `ShardUId`. Trie nodes are content-addressed (keyed by their hash) in `DBCol::State`.

### TrieUpdate

`TrieUpdate` is the runtime-facing API for reading and writing state:

1. **`set(key, value)` / `remove(key)`** — Writes go into a `prospective` buffer.
2. **`commit(cause)`** — Moves `prospective` into `committed`, tagging each change with a `StateChangeCause` (for the state changes RPC). Can be called multiple times — changes accumulate.
3. **`finalize()`** — Applies all `committed` changes to the trie, producing a `TrieUpdateResult` containing the `TrieChanges` (refcount deltas) and the new state root.

When the same key is written multiple times within a single block, only the final value enters the trie.

### TrieChanges

`TrieChanges` is the output of a trie update — it records which trie nodes were inserted (new) and which were deleted (superseded):

```rust
pub struct TrieChanges {
    pub old_root: StateRoot,
    pub new_root: StateRoot,
    insertions: Vec<TrieRefcountAddition>,
    deletions: Vec<TrieRefcountSubtraction>,
}
```

These are stored in `DBCol::TrieChanges` during block processing and later read by GC to apply the inverse operations. The fork model is:

```
         __changes1___state1
state0 /
        \__changes2___state2

To store state0 + state1 + state2: apply insertions from changes1 and changes2
To discard state2 (fork GC):       revert insertions from changes2
To discard state0 (canonical GC):   apply deletions from changes1
```

There are two production paths for `TrieChanges`: the memtrie path (`MemTrieUpdate`) and the disk trie path (`TrieStorageUpdate`). Both must produce equivalent results. The memtrie path is the default for validators.

### MemTries

When memtries are enabled (default for validators), trie updates happen in-memory first via `MemTrieUpdate`, which produces the same `TrieChanges` as the disk path. This avoids disk reads during block processing. The disk trie path (`TrieStorageUpdate`) is used as a fallback when memtries are not available.

---

## Flat Storage

### Purpose

Trie lookups require traversing multiple nodes from root to leaf, each requiring a disk read (or cache hit). Flat storage eliminates this by maintaining a flat key→value map in `DBCol::FlatState` that mirrors the current trie state for each shard.

### How It Works

Flat storage maintains a snapshot at a specific block (`flat_head`) plus a chain of in-memory deltas for subsequent blocks. To read a value at a given block:

1. Look up the value in the flat state snapshot (at `flat_head`)
2. Apply deltas for each block between `flat_head` and the target block
3. Return the result

The flat head advances periodically, folding deltas into the base snapshot.

Flat storage is purely a read optimization. The trie in `DBCol::State` remains the authoritative source of state. If flat storage is unavailable (e.g., during initial creation after state sync), reads fall back to trie traversal transparently.

### Limits

Flat storage has soft limits that trigger warnings but don't hard-fail:

| Limit | Value | Purpose |
|---|---|---|
| Hop limit | 100 blocks | Max delta chain length before warning |
| Cached changes limit | 150 MiB/shard | Memory cap for cached deltas |
| Head gap | 2 blocks | Min distance before advancing flat head |

---

## Resharding and Storage

When a shard splits, the two child shards need their own copies of the parent's trie data. This is handled through a multi-step process:

1. **Initial split** — `retain_split_shard()` produces the initial state for each child, applied to the parent's prefix in `DBCol::State`.
2. **Mapping** — A shard UID mapping is set up so that reads for the child shard are redirected to the parent's prefix. This is handled transparently by `TrieStoreAdapter`.
3. **Block processing during mapping** — New blocks write to both child and parent prefixes, so both stay up to date.
4. **Background batch copy** — A background process (`TrieStateResharder`) iterates the parent's trie and copies all data to the child's own prefix.
5. **Mapping removal** — Once the copy is complete, the mapping is removed. The child shard now reads from its own prefix.

GC is 5 epochs behind the head, so the batch copy (which runs immediately after resharding) completes long before GC reaches the resharding-era blocks. `gc_parent_shard_after_resharding()` checks that no child still depends on the parent before deleting the parent's state.

---

## Trie Caching

### Shard Cache

Each shard has an LRU cache (`TrieCache`) for trie nodes. It's thread-safe and bounded by a configurable byte limit (default: 50MB for validators). Large values exceeding `max_cached_value_size` are not cached.

### Read Path

When `TrieCachingStorage` needs a trie node:

1. Check the per-shard LRU cache
2. Check the async prefetch queue (if receipt prefetching is enabled)
3. Fall back to flat storage (for value lookups)
4. Fall back to a direct RocksDB read

After chunk processing, `update_cache()` batch-updates the cache with newly accessed and evicted entries.

---

## Configuration

Key `StoreConfig` settings:

| Setting | Default | Description |
|---|---|---|
| `trie_cache.default_max_bytes` | 50MB (validators) | Per-shard trie node cache size |
| `enable_receipt_prefetching` | true | Async prefetch receipts during apply |
| `max_open_files` | 10000 | RocksDB file descriptor limit |
| `col_state_cache_size` | 25MB | LRU size for deserialized State column objects |

---

## Key Source Files

| File | Description |
|---|---|
| `core/store/src/store.rs` | `Store` (read) and `StoreUpdate` (transaction builder), atomic commit |
| `core/store/src/columns.rs` | `DBCol` enum: all database columns with their properties |
| `core/store/src/db/refcount.rs` | RC encoding, RocksDB merge operator, compaction filter |
| `core/store/src/db/rocksdb.rs` | RocksDB backend, WriteBatch, column family configuration |
| `core/store/src/trie/mod.rs` | `TrieChanges`, `TrieRefcountDeltaMap`: structures representing refcount deltas |
| `core/store/src/trie/shard_tries.rs` | `apply_insertions`, `apply_deletions`, `revert_insertions` |
| `core/store/src/trie/update.rs` | `TrieUpdate`: the runtime-facing API for state modifications |
| `core/store/src/trie/trie_storage.rs` | `TrieCache` and `TrieCachingStorage`: LRU cache with async prefetch |
| `core/store/src/trie/mem/mod.rs` | `MemTries`: in-memory trie used for fast updates on validators |
| `core/store/src/flat/storage.rs` | `FlatStorage`: flat key→value state with a delta chain |
| `core/store/src/adapter/trie_store.rs` | `TrieStoreAdapter`: shard UID prefix mapping used during resharding |
| `core/store/src/config.rs` | `StoreConfig`: cache sizes, compaction settings, split storage |

See also: [GC Architecture](../../chain/chain/GC_ARCHITECTURE.md) for how old data is cleaned up.
