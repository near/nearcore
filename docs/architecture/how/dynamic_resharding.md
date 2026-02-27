# Dynamic Resharding - Feature Summary

## 1. Overview

Dynamic resharding is a protocol-level feature for the NEAR blockchain that enables automatic splitting of shards based on runtime state size, without requiring a protocol upgrade. It replaces the legacy "static resharding" approach where shard layout changes were hard-coded into specific protocol versions.

The feature is gated behind the `ProtocolFeature::DynamicResharding` protocol feature flag.

### Key Design Principles

- **One split per epoch**: At most one shard can be split per epoch boundary. The shard with the largest memory usage (or a forced shard) is selected.
- **Two-epoch delay**: A split proposed at the end of epoch N takes effect in epoch N+2, giving the network one full epoch (N+1) to prepare (state migration, validator reassignment, etc.).
- **Per-epoch shard layout**: With dynamic resharding, the shard layout is stored in `EpochInfo` rather than being derived from the protocol version via `EpochConfig`. This allows different epochs at the same protocol version to have different shard layouts.
- **Backwards-compatible**: The system supports a `ShardLayoutConfig` enum that is either `Static` (legacy) or `Dynamic`, allowing gradual migration.
- **Deterministic**: The split algorithm is deterministic and reproducible from state witnesses (recorded trie storage), which is critical for stateless validation.

### High-Level Data Flow

```
[Chunk Application]
    |
    v
compute_proposed_split() -- runtime checks memory thresholds, runs find_trie_split()
    |
    v
[ChunkExtra.proposed_split] -- persisted in local storage
    |
    v
[ShardChunkHeaderInnerV5.proposed_split] -- embedded in chunk header, broadcast to network
    |
    v
[Block Production - last block of epoch]
    |
    v
get_upcoming_shard_split() -- collects proposals from chunk headers, picks the best
    |
    v
[BlockHeaderInnerRestV6.shard_split] -- embedded in block header
    |
    v
[EpochManager::finalize_epoch()]
    |
    v
next_next_shard_layout() -- derives ShardLayoutV3 for epoch N+2
    |
    v
[EpochInfoV5.shard_layout] -- new layout stored in epoch info for epoch N+2
```

---

## 2. Abstract Logic Description

### 2.1 Finding the Optimal Split Point

The core problem: given a shard's state trie, find an account ID boundary that divides the trie into two roughly equal halves by memory usage.

NEAR's state trie is a Merkle-Patricia trie with 16-ary branching. Each node stores a cumulative `memory_usage` value for its entire subtree. The trie has many "column" subtrees sharing a common root, each identified by the first byte of the key (see `trie_key::col`). Of these, only four use account ID as key or key prefix, which makes them relevant to the split algorithm (since shards are split by account ID boundaries):
- **ACCOUNT** (column 0) -- account records
- **CONTRACT_CODE** (column 1) -- WASM contract code
- **ACCESS_KEY** (column 2) -- access keys
- **CONTRACT_DATA** (column 9) -- contract key-value storage

The remaining columns (delayed receipts, buffered receipts, promise yields, etc.) store shard-global state not keyed by account ID, so they are not considered by the split search.

The algorithm descends through the four account-keyed subtrees simultaneously, performing a greedy binary-search-like traversal:

1. At each branch node, aggregate children memory usage across all four subtrees.
2. Compute a threshold (`total_memory / 2 - left_memory`).
3. Find the "middle child" -- the nibble where the cumulative sum first exceeds the threshold.
4. Descend into the middle child across all subtrees.
5. At each position, evaluate two split candidates:
   - **Split A**: Use the current path as the boundary (middle memory goes to the right shard).
   - **Split B**: Append "-0" to the current path (the current account's own data + attached data goes to the left, child accounts go to the right). This handles "big middle accounts" -- accounts with large contracts or many data entries.
6. Keep track of the best split seen so far (lowest `mem_diff = |left_memory - right_memory|`).
7. Stop when the ACCOUNT subtree reaches a leaf or when further descent cannot improve the split.

The algorithm enforces that the resulting boundary account ID is valid: at least 2 bytes long and composed of valid UTF-8 bytes. If the natural descent terminates too early, it force-descends into the first available child until the minimum path length is met.

### 2.2 Proposing a Shard Split

At each block, during chunk application, the runtime checks whether a shard should be split. The decision logic has the following priority:

1. **Max shard count check** (highest priority): If `num_shards >= max_number_of_shards`, no split. This overrides everything, including force-split.
2. **Force split**: If the shard is in `force_split_shards`, split it regardless of memory thresholds.
3. **Block split**: If the shard is in `block_split_shards`, do not split regardless of thresholds.
4. **Threshold checks**: If `total_mem_usage >= memory_usage_threshold`, compute the split. If both children would have at least `min_child_memory_usage`, propose the split.

The result (`Option<TrieSplit>`) is stored in `ChunkExtra` and embedded in the chunk header (`ShardChunkHeaderInnerV5.proposed_split`).

### 2.3 Selecting the Split at Epoch Boundary

At the last block of each epoch, the block producer:

1. Collects `proposed_split` values from all chunk headers in the block.
2. Calls `get_upcoming_shard_split()` which:
   - Checks if dynamic resharding is enabled (via `ShardLayoutConfig::Dynamic`).
   - Checks the resharding cooldown (`can_reshard()` -- verifies `epoch_height - last_resharding >= min_epochs_between_resharding`).
   - Calls `pick_shard_to_split()` to select the winning shard: forced shards have priority, otherwise the shard with highest `total_memory()` wins.
3. Embeds the result as `shard_split: Option<(ShardId, AccountId)>` in `BlockHeaderInnerRestV6`.

### 2.4 Deriving the New Shard Layout

During `EpochManager::finalize_epoch()`, when finalizing epoch N:

1. Read `block_info.shard_split()` from the last block of epoch N.
2. Call `next_next_shard_layout()` which has three phases:
   - **Static fallback**: If the next-next epoch config has a static layout, use it (backward compat).
   - **Transitional**: If the current epoch doesn't have dynamic resharding enabled, carry forward the existing layout.
   - **Dynamic**: If a split is present, call `ShardLayout::derive_v3()` to create a new `ShardLayoutV3`.
3. Store the resulting layout in `EpochInfoV5.shard_layout` for epoch N+2.

When bootstrapping from a legacy layout (V1 or V2), the system reconstructs the split history by calling `get_shard_layout_history()` to retrieve all historical layouts, then uses `ShardLayoutV3::derive_with_layout_history()` to build a V3 layout with full ancestor tracking.

### 2.5 Validation

Both `proposed_split` and `shard_split` are validated to prevent forging:

- **Chunk header validation**: During state witness validation, the `proposed_split` in the received chunk header is compared against the locally-computed `ChunkExtra.proposed_split()`. Mismatch produces `InvalidChunkHeaderShardSplit`.
- **Block header validation**: During block processing, the `shard_split` in the block header is recomputed by calling `get_upcoming_shard_split()` with the block's chunk headers. Mismatch produces `InvalidBlockHeaderShardSplit`.

---

## 3. New and Modified Data Structures

- **`TrieSplit`** (`core/primitives/src/trie_split.rs`) -- Result of finding the optimal split point: boundary account and left/right memory usage. Stored in chunk headers and `ChunkExtra`.

- **`ShardLayoutV3`** (`core/primitives/src/shard_layout/v3.rs`) -- New shard layout version for dynamic resharding. Unlike V2, stores the full cumulative split history and a derived ancestor map enabling O(1) shard tracking.

- **`DynamicReshardingConfig`** (`core/primitives/src/epoch_manager.rs`) -- Resharding parameters (thresholds, limits, force/block lists). Stored in `EpochConfig`.

- **`ShardLayoutConfig`** (`core/primitives/src/epoch_manager.rs`) -- Enum replacing the old `shard_layout` field in `EpochConfig`. Either `Static` (layout defined inline) or `Dynamic` (layout managed at runtime via `DynamicReshardingConfig`).

- **`EpochInfoV5`** (`core/primitives/src/epoch_info.rs`) -- Adds `shard_layout` and `last_resharding` fields, making `EpochInfo` the authoritative source of shard layouts instead of `EpochConfig`.

- **`BlockHeaderInnerRestV6`** (`core/primitives/src/block_header.rs`) -- Adds `shard_split: Option<(ShardId, AccountId)>`, set only for the last block of an epoch. Also removes deprecated challenge fields.

- **`ShardChunkHeaderInnerV5`** (`core/primitives/src/sharding/shard_chunk_header_inner.rs`) -- Adds `proposed_split: Option<TrieSplit>`, set during chunk application.

- **`BlockInfoV4`** (`core/primitives/src/epoch_block_info.rs`) -- Adds `shard_split`, propagated from the block header. Removes deprecated `slashed` map.

- **`ChunkExtraV5`** (`core/primitives/src/types.rs`) -- Adds `proposed_split: Option<TrieSplit>`, used to validate incoming chunk headers.

- **`FindSplitError`** (`core/store/src/trie/split.rs`) -- Error type for the trie split algorithm.

---

## 4. Key Functions

### Trie split (`core/store/src/trie/split.rs`)

- **`find_trie_split`** -- Public entry point. Dispatches to memtrie or disk trie path, creates a `TrieDescent`, and runs `find_mem_usage_split()`.
- **`total_mem_usage`** -- Returns the trie root's `memory_usage` field. Used as a quick threshold check before the more expensive split computation.
- **`TrieDescent::find_mem_usage_split`** -- Core algorithm loop. At each step, descends into the "middle child" across all four subtrees, evaluates two split candidates (direct boundary vs. "-0" suffix), and tracks the best split found.
- **`TrieDescent::best_split_at_current_path`** -- Evaluates the two split candidates at the current position (Split A: boundary as-is; Split B: boundary + "-0" suffix to push a big account to the left). Returns whichever has lower `mem_diff`.

### Runtime (`chain/chain/src/runtime/mod.rs`)

- **`NightshadeRuntime::compute_proposed_split`** -- Called during `apply_transactions`. Gates on `DynamicResharding` feature flag, then delegates to `check_dynamic_resharding`.
- **`check_dynamic_resharding`** -- Module-level function implementing the threshold-based decision logic (max shards, force/block lists, memory threshold, min child size).

### Epoch Manager (`chain/epoch-manager/src/lib.rs`)

- **`get_upcoming_shard_split`** -- Called during block production and block validation. Collects `proposed_split` values from chunk headers, checks resharding cooldown, calls `pick_shard_to_split` to select the winner.
- **`next_next_shard_layout`** -- Called during `finalize_epoch`. Three-phase logic: static fallback, transitional (dynamic not yet active), and dynamic (derive new V3 layout from `block_info.shard_split()`).
- **`can_reshard`** -- O(1) check using `EpochInfoV5::last_resharding`. Returns whether enough epochs have passed since the last resharding.
- **`pick_shard_to_split`** -- Pure function: picks from `force_split_shards` first, then falls back to highest `total_memory()`.
- **`get_shard_layout`** -- Single source of truth for shard layouts. Checks `EpochInfo::shard_layout()` first (V5+), falls back to `EpochConfig::static_shard_layout()` for older versions.
- **`get_shard_layout_history`** -- Collects all distinct static shard layouts across protocol versions (newest to oldest). Used for bootstrapping V3 from V1/V2.
- **`is_produced_block_last_in_epoch`** -- Like `is_next_block_epoch_start`, but works for blocks not yet in the store. Used during block production to decide whether to include `shard_split`.

### Validation (`chain/chain/src/validate.rs`)

- **`validate_chunk_with_chunk_extra_and_receipts_root`** -- Extended to compare `chunk_header.proposed_split()` against `prev_chunk_extra.proposed_split()`. Prevents chunk producers from forging the field.
- **`validate_block_shard_split`** -- Recomputes `shard_split` from chunk headers via `get_upcoming_shard_split()` and compares against the block header value. Prevents block producers from forging the field.

### Shard Layout Derivation (`core/primitives/src/shard_layout/v3.rs`)

- **`ShardLayoutV3::derive`** -- Derives a new V3 layout by splitting one shard. Inserts new boundary account, allocates two new shard IDs (`max + 1`, `max + 2`), updates the split map.
- **`ShardLayoutV3::derive_with_layout_history`** -- Same as `derive` but for bootstrapping from V1/V2 layouts. Reconstructs the split history from a sequence of historical layouts.
- **`build_shard_split_map`** -- Builds cumulative `ShardsSplitMapV3` from a layout history by iterating `windows(2)` and extracting parent-child relationships.

---

## 5. Key Differences Between Static and Dynamic Resharding

| Aspect | Static Resharding | Dynamic Resharding |
|--------|------------------|--------------------|
| **Shard layout source** | `EpochConfig.shard_layout` (determined by protocol version) | `EpochInfo.shard_layout` (stored per-epoch in V5) |
| **When layout changes** | Only on protocol version upgrade | Automatically at epoch boundaries based on trie memory usage |
| **Layout version** | V0, V1, or V2 | V3 (with cumulative split history) |
| **Config type** | `ShardLayoutConfig::Static { shard_layout }` | `ShardLayoutConfig::Dynamic { dynamic_resharding_config }` |
| **Split history** | V2 stores only the most recent split map | V3 stores full cumulative split history + ancestor maps |
| **EpochInfo version** | V4 or earlier | V5 (adds `shard_layout` and `last_resharding` fields) |
| **Block header** | V5 (with deprecated challenges fields) | V6 (adds `shard_split`, removes challenges) |
| **Chunk header** | V4 | V5 (adds `proposed_split: Option<TrieSplit>`) |
| **BlockInfo** | V3 (with deprecated `slashed` map) | V4 (adds `shard_split`, removes `slashed`) |
| **ChunkExtra** | V4 | V5 (adds `proposed_split`) |
| **Shard tracking** | Iterate through protocol versions comparing layouts | O(1) lookup via `ShardLayoutV3::ancestor_uids()` |
| **Resharding cooldown** | N/A | `EpochInfoV5::last_resharding` enables O(1) check |
| **Shard count** | Fixed per protocol version | Bounded by `max_number_of_shards` |
| **Thread pool sizing** | Based on `shard_layout.num_shards()` | Based on `max_number_of_shards` (conservative upper bound) |

### Migration Path

When the network first enables dynamic resharding (transitioning from static to dynamic):
1. The current shard layout (V2) cannot directly become V3 because V2 lacks cumulative split history.
2. `ShardLayoutV3::derive_with_layout_history()` is called, which uses `EpochManager::get_shard_layout_history()` to reconstruct the split history from all historical layouts.
3. `build_shard_split_map()` processes the layout history (newest to oldest, `windows(2)`) to extract parent-child relationships from V2+ layouts.
4. The resulting V3 layout has full ancestor tracking and is ready for further dynamic splits.

---

## 6. Uncompleted TODOs

### Dynamic Resharding Specific

1. **`chain/chain/src/runtime/mod.rs:540-541`** -- `compute_proposed_split` has two unresolved checks: verifying whether the *next* block is the last of the epoch, and checking the resharding cooldown at the runtime layer (the epoch manager does implement the cooldown, but the runtime layer has its own TODO about it).

2. **`chain/epoch-manager/src/lib.rs:663`** -- `next_next_shard_layout()` still takes `next_next_epoch_config` for backward compatibility with static-resharding tests. Should be removed once tests are migrated.

3. **`chain/epoch-manager/src/adapter.rs:736`** -- `get_shard_uids_pending_resharding()` returns empty for dynamic resharding. Dynamic trie loading needs to be implemented.

4. **`chain/epoch-manager/src/test_utils.rs:439`** -- Test utilities still create `BlockInfoV3` instead of `BlockInfoV4`.

5. **`core/store/src/config.rs:217`** -- Cache size computation for dynamic resharding protocol versions is currently skipped.

6. **`tools/fork-network/src/cli.rs:353,707`** -- Fork network tool does not support dynamic resharding yet.

7. **`tools/database/src/memtrie.rs:399`** -- CLI tool for finding boundary accounts needs updating for dynamic resharding.

### General Resharding TODOs (May Affect Dynamic Resharding)

8. **`chain/epoch-manager/src/shard_assignment.rs:198`** -- Shard assignment for validators after resharding is not yet implemented.

9. **`chain/client/src/stateless_validation/shadow_validate.rs:22`** -- Shadow validation breaks across resharding boundaries.

10. **`chain/chain/src/stateless_validation/state_witness.rs:260`** -- `get_incoming_receipts_for_shard` generates invalid proofs on resharding boundaries.

11. **`chain/chain/src/resharding/manager.rs:249`** -- The resharding manager doesn't set all `ChunkExtra` fields (notably the new `proposed_split` field).

12. **`runtime/runtime/src/congestion_control.rs:336`** -- Parent shard's outgoing buffer cleanup after resharding.

13. **`nightly/pytest-sanity.txt:274`** -- Integration between resharding and other features (stateless validation, state sync, congestion control) is incomplete.

### Spice-Resharding Integration

14. Multiple TODOs in `chunk_executor_actor.rs`, `spice_data_distributor_actor.rs`, `spice_chunk_validation.rs`, and `spice_chunk_application.rs` indicate that the Spice feature does not yet handle resharding transitions properly.
