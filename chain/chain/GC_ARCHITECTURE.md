# Garbage Collection Architecture

This document describes the internals of the GC (garbage collection) layer in nearcore: how old block and state data is identified, cleaned up, and how trie refcounts are maintained.

## Overview

GC removes old block, chunk, receipt, transaction, and trie state data to bound storage growth on non-archival nodes. It runs as a separate actor (`GCActor`) that ticks every 500ms, processing a small number of blocks per cycle (default: 2) to avoid interfering with block processing.

The core invariant: data older than `gc_num_epochs_to_keep` (default: 5) epochs behind the chain head is eligible for deletion. The boundary between "keep" and "delete" is called `gc_stop_height`, calculated as the first block height of the oldest kept epoch.

There are three GC modes:

- **Fork** — When a block turns out to not be on the canonical chain, its state changes are undone. The trie nodes it added have their refcounts decremented (`revert_insertions`).
- **Canonical** — When a canonical block is old enough, the pre-block state it superseded can be discarded. The old trie nodes have their refcounts decremented (`apply_deletions`).
- **StateSync** — A complete reset used during state sync. Deletes everything and starts fresh.

GC errors are logged but don't crash the node (`debug_assert!(false)` fires in debug builds only). This is intentional — GC failures shouldn't be fatal.

---

## The GC Actor

`GCActor` is a standalone tokio actor that calls `ChainStore::clear_data()` on each tick. It creates its own `ChainStore` wrapping the shared RocksDB instance. If `clear_data()` returns an error, the actor logs it and continues — it will retry on the next tick.

Before the main block GC, `clear_data()` runs two independent cleanup passes that execute on every tick regardless of the block budget:
- `clear_state_transition_data()` — removes old `StateTransitionData` entries
- `clear_witnesses_data()` — removes old chunk witness data

---

## How Block GC Works

The main cleanup happens in `clear_old_blocks_data()`. It runs in two phases within a single call, sharing the same `gc_blocks_limit` budget (default: 2 blocks per cycle). If fork cleaning exhausts the budget, canonical cleaning doesn't run at all.

### Phase 1 — Fork Clearing (top-down)

Starting from `fork_tail` and working downward, fork GC examines each height for non-canonical blocks. For each fork block found, it:
1. Calls `clear_block_data()` in Fork mode (reverts trie insertions)
2. Follows the `prev_hash` ancestor chain until it reaches a block on the canonical chain
3. Decrements the parent's `BlockRefCount` at each step; stops when refcount stays positive

Fork GC processes up to `gc_fork_clean_step` (100) heights per cycle before moving on. When `gc_stop_height` increases at a new epoch, `fork_tail` is set to the new `gc_stop_height` and then gradually works its way back down toward `tail` over subsequent cycles.

### Phase 2 — Canonical Clearing (bottom-up)

Starting from `tail + 1` and working upward toward `gc_stop_height`, canonical GC processes one block per height:
1. Fetches all blocks at the height (should be exactly 1 after fork cleaning)
2. Checks the predecessor's refcount is exactly 1 — if it's higher, a fork hasn't been cleaned yet, so GC stops
3. Calls `clear_block_data()` in Canonical mode (applies trie deletions)
4. Runs `gc_parent_shard_after_resharding()` and `gc_state()` for resharding/epoch cleanup
5. Advances the tail

Each GC operation commits independently — fork blocks commit per block, canonical blocks commit per height. A crash mid-cycle leaves partially-GC'd state, but the next cycle resumes from the persisted tail/fork_tail.

### What `clear_block_data()` Deletes

For a single block, this function:
1. Processes trie refcount changes via `gc_trie_changes()` — applies the appropriate refcount operations based on GC mode
2. Loads the block and its shard layout
3. Deletes per-shard data: IncomingReceipts, StateHeaders, ChunkExtra, etc.
4. Deletes per-block data: Block, BlockHeader, NextBlockHashes, BlockRefCount, etc.
5. In Fork mode: decrements the parent block's refcount. In Canonical mode: deletes chunk data and headers.

Everything is staged in a `ChainStoreUpdate` and committed atomically.

---

## Block Refcount

Each block tracks how many other blocks reference it as their `prev_hash`, stored in `DBCol::BlockRefCount`. This is the mechanism that coordinates fork and canonical GC:

- When a new block is processed, its parent's refcount is incremented.
- When a fork block is GC'd, its parent's refcount is decremented.
- Canonical GC checks that the predecessor has refcount exactly 1 before proceeding. A refcount greater than 1 means a fork block still references the predecessor and must be cleaned first.
- A refcount of 0 on the canonical chain is treated as an error.

---

## Trie Refcount Operations

The bridge between block processing and GC is `TrieChanges` — a per-block, per-shard record of which trie nodes were inserted and which were deleted when the block was applied. GC reads these stored records and applies the inverse operations:

| When | Operation | Effect |
|---|---|---|
| Block applied | `apply_insertions` | Increment RC for new trie nodes |
| Canonical GC | `apply_deletions` | Decrement RC for superseded trie nodes |
| Fork GC | `revert_insertions` | Decrement RC for the fork's new trie nodes |

The key invariant: `revert_insertions` is the exact inverse of `apply_insertions`. Each insertion record is converted to a subtraction with the same hash and refcount value.

For untracked shards, TrieChanges don't exist in the DB. `gc_trie_changes()` silently skips these — this is intentional.

`OutgoingReceipts` and `State` have dedicated GC paths (`gc_outgoing_receipts` and `gc_trie_changes` respectively). Routing them through the generic `gc_col` is a programming error and will panic.

See [Storage Architecture](../../core/store/STORAGE_ARCHITECTURE.md) for details on how refcounting works at the RocksDB level.

---

## GC at Epoch and Resharding Boundaries

### gc_stop_height Calculation

`gc_stop_height` is computed in `get_gc_stop_height_impl()`: starting from the head block, walk back `gc_num_epochs_to_keep` epochs and return the first block height of the oldest kept epoch. For archival nodes with split storage, this is additionally bounded by the cold head or cloud head.

### Untracked Shard State Cleanup

`gc_state()` runs only when canonical GC reaches the last block of a finished epoch. It identifies shards the node no longer tracks by:
1. Getting all shards from the GC epoch's shard layout
2. Removing shards tracked in the current or next epoch
3. Walking backward from head to the GC epoch, checking `TrieChanges` existence to see if the node was tracking each shard in intermediate epochs
4. Deleting the entire state prefix for any shard that was never tracked

### Parent Shard Cleanup After Resharding

After a shard split, the parent shard's trie data becomes redundant once children have their own copies. `gc_parent_shard_after_resharding()` checks whether any child shard still maps to the parent (via the shard UID mapping used during resharding). Once no child depends on the parent, the parent's state prefix is deleted.

---

## State Sync Reset

`reset_data_pre_state_sync()` is a completely separate path from regular GC. It loops from tail to the GC height, clearing all block data in StateSync mode, then calls `delete_all_state()` on the trie and resets the tail to genesis. This is used when a node falls too far behind and needs to re-sync state from scratch.

---

## GC Invariants

These invariants are documented in the code and must hold for GC to operate correctly:

1. The genesis block is never removed
2. No block except genesis has height <= genesis_height
3. Tail exists, is always on the canonical chain, and there is exactly one Tail
4. Head exists and is always on the canonical chain
5. All blocks have heights in [Tail, Head]
6. If A is an ancestor of B, then height(A) < height(B)
7. The oldest block where a fork started is at the lowest height on that fork
8. The oldest fork block's parent is always on the canonical chain

---

## Configuration

GC behavior is controlled by `GCConfig` in the node's client config:

| Setting | Default | Description |
|---|---|---|
| `gc_blocks_limit` | 2 | Max blocks processed per GC cycle |
| `gc_fork_clean_step` | 100 | Max heights scanned for fork cleaning per cycle |
| `gc_num_epochs_to_keep` | 5 | Number of epochs to retain (minimum: 3) |
| `gc_step_period` | 500ms | Time between GC ticks |

---

## Key Source Files

| File | Description |
|---|---|
| `chain/client/src/gc_actor.rs` | GC actor: scheduling, error handling, 500ms tick |
| `chain/chain/src/garbage_collection.rs` | All GC logic: fork clearing, canonical clearing, state cleanup, column-specific deletion |
| `chain/chain/src/store/mod.rs` | ChainStore/ChainStoreUpdate: block data persistence and cache |
| `chain/chain/src/runtime/mod.rs` | `get_gc_stop_height_impl()`: determines how far back GC can reach |
| `core/store/src/trie/shard_tries.rs` | `apply_insertions`, `apply_deletions`, `revert_insertions`: trie refcount operations |
| `core/chain-configs/src/client_config.rs` | `GCConfig`: gc_blocks_limit, gc_fork_clean_step, gc_num_epochs_to_keep |

See also: [Storage Architecture](../../core/store/STORAGE_ARCHITECTURE.md) for details on refcounting, trie changes, and the store layer.
