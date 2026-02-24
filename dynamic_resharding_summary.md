# Dynamic Resharding - Feature Summary

## 1. Overview

Dynamic resharding is a protocol-level feature for the NEAR blockchain that enables automatic splitting of shards based on runtime state size, without requiring a protocol upgrade. It replaces the legacy "static resharding" approach where shard layout changes were hard-coded into specific protocol versions.

The feature was developed across 27 commits (PRs #14037 through #15142) from August 2025 to February 2026. It is gated behind the `ProtocolFeature::DynamicResharding` protocol feature flag, currently assigned to nightly protocol version **150**.

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

NEAR's state trie is a Merkle-Patricia trie with 16-ary branching. Each node stores a cumulative `memory_usage` value for its entire subtree. The trie has four "column" subtrees sharing a common root, identified by the first nibble of the key:
- **ACCOUNT** (column 9) -- account records
- **CONTRACT_CODE** (column 1) -- WASM contract code
- **ACCESS_KEY** (column 6) -- access keys
- **CONTRACT_DATA** (column 7) -- contract key-value storage

The algorithm descends through all four subtrees simultaneously, performing a greedy binary-search-like traversal:

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

## 3. Data Structures

### 3.1 `TrieSplit` (`core/primitives/src/trie_split.rs`)

The result of finding the optimal split point in a shard's trie.

```rust
pub struct TrieSplit {
    pub boundary_account: AccountId,  // The account ID at the split point
    pub left_memory: u64,             // Memory usage of left partition (excluding boundary)
    pub right_memory: u64,            // Memory usage of right partition (including boundary)
}
```

**Methods:**
- `new(boundary_account, left_memory, right_memory)` -- constructor
- `dummy()` -- creates a sentinel with `right_memory = u64::MAX` (worse than any real split)
- `is_dummy()` -- checks for sentinel
- `split_path_bytes()` -- returns the boundary account as bytes
- `mem_diff()` -- `left_memory.abs_diff(right_memory)`
- `total_memory()` -- `left_memory.saturating_add(right_memory)`

**Derives:** `Debug, Clone, Eq, PartialEq, BorshSerialize, BorshDeserialize, serde::Serialize, serde::Deserialize, schemars::JsonSchema`

**Note:** Memory values are artificial, calculated using `TRIE_COSTS` constants. They are roughly 2x the actual RAM usage of the memtrie.

### 3.2 `ShardLayoutV3` (`core/primitives/src/shard_layout/v3.rs`)

The shard layout version designed for dynamic resharding. Key differences from V2:

```rust
pub struct ShardLayoutV3 {
    pub(crate) boundary_accounts: Vec<AccountId>,
    pub(crate) shard_ids: Vec<ShardId>,
    pub(crate) id_to_index_map: BTreeMap<ShardId, ShardIndex>,
    pub(crate) shards_split_map: ShardsSplitMapV3,   // Full cumulative split history
    pub(crate) last_split: ShardId,                   // Most recently split parent
    pub(crate) shards_ancestor_map: ShardsAncestorMapV3, // Derived: shard -> [ancestors]
}
```

**Types:**
- `ShardsSplitMapV3 = BTreeMap<ShardId, Vec<ShardId>>` -- cumulative history of all splits
- `ShardsAncestorMapV3 = BTreeMap<ShardId, Vec<ShardUId>>` -- derived ancestor chains

**Key methods:**
- `new(boundary_accounts, shard_ids, shards_split_map, last_split)` -- validates and derives ancestor map
- `derive(base: &Self, new_boundary_account)` -- derive V3 from existing V3
- `derive_with_layout_history(base: &ShardLayout, new_boundary_account, layout_history: &[ShardLayout])` -- derive V3 from V1/V2 with reconstructed history
- `recent_split()` -- returns only the most recent split (for `ReshardingEventType`)
- `ancestor_uids(shard_id)` -- returns all ancestor shard UIDs (for O(1) shard tracking)
- `account_id_to_shard_id(account_id)` -- binary search on boundary accounts
- `get_children_shards_ids(parent_shard_id)` -- children from most recent split only
- `try_get_parent_shard_id(shard_id)` -- parent from most recent split only

**Standalone function:**
- `build_shard_split_map(layout_history: &[ShardLayout]) -> ShardsSplitMapV3` -- reconstructs split history from a sequence of layouts (newest to oldest)

**Fixed version:** `version()` always returns `3`.

### 3.3 `DynamicReshardingConfig` (`core/primitives/src/epoch_manager.rs`)

Configuration parameters for dynamic resharding. Stored in `EpochConfig` (not `RuntimeConfig`).

```rust
pub struct DynamicReshardingConfig {
    pub memory_usage_threshold: u64,
    pub min_child_memory_usage: u64,
    pub max_number_of_shards: NumShards,
    pub min_epochs_between_resharding: EpochHeight,
    pub force_split_shards: Vec<ShardId>,
    pub block_split_shards: Vec<ShardId>,
}
```

**Default values:** All thresholds set to `999_999_999_999_999` (effectively disabled). Force/block lists empty.

**Production values (protocol version 150):**
- `memory_usage_threshold`: 40,000,000,000 (~20 GB actual RAM)
- `min_child_memory_usage`: 10,000,000,000 (~5 GB actual RAM)
- `max_number_of_shards`: 16
- `min_epochs_between_resharding`: 2

### 3.4 `ShardLayoutConfig` (`core/primitives/src/epoch_manager.rs`)

Mutually exclusive enum representing either static or dynamic shard management:

```rust
#[serde(untagged)]
pub enum ShardLayoutConfig {
    Static { shard_layout: ShardLayout },
    Dynamic { dynamic_resharding_config: DynamicReshardingConfig },
}
```

Replaces the old `shard_layout: ShardLayout` field in `EpochConfig`. The `#[serde(untagged)]` attribute enables seamless JSON/YAML deserialization.

**Methods:**
- `static_shard_layout()` -- returns `Option<&ShardLayout>` (None for Dynamic)
- `dynamic_resharding_config()` -- returns `Option<&DynamicReshardingConfig>` (None for Static)

### 3.5 `EpochInfoV5` (`core/primitives/src/epoch_info.rs`)

Extended epoch info that stores the shard layout per-epoch:

```rust
pub struct EpochInfoV5 {
    // ... all V4 fields ...
    pub shard_layout: ShardLayout,                // NEW: per-epoch shard layout
    pub last_resharding: Option<EpochHeight>,     // NEW: epoch of most recent resharding
    // ... internal fields ...
}
```

Created when `ProtocolFeature::DynamicResharding` is enabled. The `shard_layout` field makes this the authoritative source of truth for shard layouts, replacing the protocol-version-based lookup in `EpochConfig`.

The `last_resharding` field stores the epoch height at which the most recent resharding occurred, enabling O(1) cooldown checks. `None` means no resharding has happened since dynamic resharding was enabled.

**Accessors on `EpochInfo`:**
- `shard_layout()` -- returns `Some(&ShardLayout)` for V5, `None` for older versions
- `last_resharding()` -- returns `Option<EpochHeight>`, `None` for pre-V5

### 3.6 `BlockHeaderInnerRestV6` (`core/primitives/src/block_header.rs`)

New block header "inner rest" struct with the `shard_split` field:

```rust
pub struct BlockHeaderInnerRestV6 {
    // ... all V5 fields except deprecated challenges_root and challenges_result ...
    pub shard_split: Option<(ShardId, AccountId)>,
}
```

Created when `ProtocolFeature::DynamicResharding.enabled(current_protocol_version)`. The `shard_split` field may only be set for the last block of an epoch. Also removes deprecated `challenges_root` and `challenges_result` fields from V5.

**Accessor on `BlockHeader`:**
- `shard_split()` -- returns `Option<&(ShardId, AccountId)>`, `None` for pre-V6 headers

### 3.7 `ShardChunkHeaderInnerV5` (`core/primitives/src/sharding/shard_chunk_header_inner.rs`)

New chunk header inner struct with the `proposed_split` field:

```rust
pub struct ShardChunkHeaderInnerV5 {
    // ... all V4 fields ...
    pub proposed_split: Option<TrieSplit>,
}
```

Created when `ProtocolFeature::DynamicResharding.enabled(protocol_version)`. The `proposed_split` field contains the result of `find_trie_split()` if the shard exceeds the memory threshold, or `None` otherwise.

**Accessor on `ShardChunkHeaderInner`:**
- `proposed_split()` -- returns `Option<&TrieSplit>`, `None` for pre-V5 headers

### 3.8 `BlockInfoV4` (`core/primitives/src/epoch_block_info.rs`)

Extended block info that carries the shard split decision:

```rust
pub struct BlockInfoV4 {
    // ... all V3 fields except deprecated `slashed` map ...
    pub shard_split: Option<(ShardId, AccountId)>,
}
```

Created when `ProtocolFeature::DynamicResharding` is enabled. Populated from `header.shard_split()` via `BlockInfo::from_header()`.

### 3.9 `ChunkExtraV5` (`core/primitives/src/types.rs`)

Extended chunk extra that stores the proposed split:

```rust
pub struct ChunkExtraV5 {
    // ... all V4 fields ...
    pub proposed_split: Option<TrieSplit>,
}
```

**Accessor on `ChunkExtra`:**
- `proposed_split()` -- returns `Option<&TrieSplit>`

### 3.10 `FindSplitError` (`core/store/src/trie/split.rs`)

Error type for the trie split algorithm:

```rust
pub enum FindSplitError {
    Storage(StorageError),         // Underlying storage error
    NoRoot,                        // Trie has no root
    NotFound,                      // Trie is empty or contains invalid keys
    Key(Vec<u8>),                  // Key is not a valid account ID (too short or odd length)
    Utf8(std::str::Utf8Error),     // Split key is not valid UTF-8
    AccountId(ParseAccountError),  // Split key is not a valid account ID
}
```

### 3.11 Internal Algorithm Structures (`core/store/src/trie/split.rs`)

These are internal to the trie split module:

- **`SubtreeIdx`** -- enum identifying the four subtrees: `Account`, `ContractCode`, `AccessKey`, `ContractData`
- **`TrieDescentStage<NodePtr>`** -- enum tracking the descent state within a single subtree:
  - `CutOff` -- path not present
  - `AtLeaf { memory_usage }` -- at a leaf
  - `InsideExtension { memory_usage, remaining_nibbles, child }` -- inside an extension node
  - `AtBranch { memory_usage, children }` -- at a branch node
- **`TrieDescent<NodePtr, Value, Storage>`** -- the main algorithm struct tracking descent across all four subtrees simultaneously. Tracks `left_memory`, `right_memory`, `middle_memory`, and `nibbles` (path walked so far).

---

## 4. Key Functions and Their Interactions

### 4.1 Trie Split Functions (`core/store/src/trie/split.rs`)

**`find_trie_split(trie: &Trie) -> FindSplitResult<TrieSplit>`**

Public entry point. Dispatches to either memtrie or disk trie path based on `trie.lock_memtries()`. Creates a `TrieDescent` and calls `find_mem_usage_split()`.

**`total_mem_usage(trie: &Trie) -> FindSplitResult<u64>`**

Returns the root node's `memory_usage` field, which in NEAR's trie stores the total memory of the entire subtree. Used as the first check in the resharding decision.

**`TrieDescent::find_mem_usage_split(self) -> FindSplitResult<TrieSplit>`**

Core algorithm loop:
1. Initialize with a dummy best split.
2. While `next_step()` returns a nibble to descend into:
   a. Call `descend_step()` to update memory accounting.
   b. Call `best_split_at_current_path()` to evaluate the two split candidates (direct boundary vs. boundary + "-0" suffix).
   c. Update the best split if the current one is better.
3. Handle forced steps if the path is too short for a valid account ID.
4. Return the best split found.

**`TrieDescent::best_split_at_current_path() -> FindSplitResult<Option<TrieSplit>>`**

Evaluates two candidates at the current position:
- **Split A**: Boundary = current nibble path. Left = `left_memory`, Right = `right_memory + middle_memory`.
- **Split B**: Boundary = current nibble path + "-0". Left = `left_memory + current_nodes_mem_usage + attached_data_mem_usage`, Right = remaining. This handles big accounts by pushing the boundary account's own data to the left shard.

Returns whichever has lower `mem_diff()`.

**`pick_shard_to_split(proposed_splits, config) -> Option<(ShardId, TrieSplit)>`**

Standalone pure function that selects which shard to split:
1. Check `force_split_shards` first (first match wins, in order).
2. Fall back to shard with highest `total_memory()`.
3. `debug_assert` that selected shard is not in `block_split_shards`.

### 4.2 Runtime Functions (`chain/chain/src/runtime/mod.rs`)

**`NightshadeRuntime::compute_proposed_split(...) -> Result<Option<TrieSplit>, Error>`**

Called during `apply_transactions()`. Gates on `ProtocolFeature::DynamicResharding`. Delegates to `check_dynamic_resharding()`.

**`check_dynamic_resharding(shard_trie, shard_id, shard_layout, config) -> Result<Option<TrieSplit>, FindSplitError>`**

Module-level function implementing the decision logic:
1. `max_number_of_shards` check
2. `force_split_shards` check
3. `block_split_shards` check
4. `memory_usage_threshold` check
5. `min_child_memory_usage` check

### 4.3 Epoch Manager Functions (`chain/epoch-manager/src/lib.rs`)

**`EpochManager::get_upcoming_shard_split(protocol_version, parent_hash, chunk_headers) -> Result<Option<(ShardId, AccountId)>, EpochError>`**

Called during block production (last block of epoch) and block validation. Collects proposed splits from chunk headers, checks if dynamic resharding is enabled, verifies cooldown, and picks the best shard to split.

**`EpochManager::next_next_shard_layout(current_epoch_config, current_protocol_version, next_next_epoch_config, next_shard_layout, block_info) -> Result<ShardLayout, EpochError>`**

Called during `finalize_epoch()`. Three-phase decision:
1. If `next_next_epoch_config` has a static layout, use it.
2. If current epoch doesn't have dynamic resharding, carry forward.
3. If dynamic resharding is enabled and `block_info.shard_split()` is present, derive a new V3 layout.

**`EpochManager::can_reshard(block_info, min_epochs_between_resharding) -> Result<bool, EpochError>`**

O(1) check: reads `next_epoch_info.last_resharding()` and compares against `min_epochs_between_resharding`.

**`EpochManager::get_shard_layout(epoch_id) -> Result<ShardLayout, EpochError>`**

The single source of truth for shard layouts. Checks `EpochInfo::shard_layout()` first (V5+), falls back to `EpochConfig::static_shard_layout()` for older versions.

**`EpochManager::get_shard_layout_history(earliest_protocol_version, protocol_version) -> Vec<ShardLayout>`**

Collects all distinct shard layouts from `earliest_protocol_version` to `protocol_version` (exclusive). Used for bootstrapping V3 from V1/V2 layouts.

**`EpochManager::is_produced_block_last_in_epoch(block_height, parent_hash, last_final_block_hash) -> Result<bool, EpochError>`**

Predicts whether a block being produced (not yet in the store) will be the last in its epoch. Needed because `is_next_block_epoch_start()` only works for blocks already stored.

### 4.4 Validation Functions (`chain/chain/src/validate.rs`)

**`validate_chunk_with_chunk_extra_and_receipts_root(...)`**

Extended to compare `chunk_header.proposed_split()` against `prev_chunk_extra.proposed_split()`. Returns `InvalidChunkHeaderShardSplit` on mismatch.

**`validate_block_shard_split(epoch_manager, header, chunk_headers) -> Result<(), Error>`**

Recomputes the expected `shard_split` by calling `epoch_manager.get_upcoming_shard_split()` with the block's chunk headers. Returns `InvalidBlockHeaderShardSplit` on mismatch.

### 4.5 Block/Chunk Production

**Block production (`chain/client/src/client.rs`):**
1. Compute `last_final_block` using `prev.last_final_block_for_height(height)`.
2. Check `is_produced_block_last_in_epoch()`.
3. If last block of epoch, call `get_upcoming_shard_split()` with chunk headers.
4. Pass `shard_split` to `Block::produce()`.

**Chunk production (`chain/client/src/chunk_producer.rs`):**
1. Read `chunk_extra.proposed_split()`.
2. Pass to `ShardChunkWithEncoding::new()` which selects V5 header when dynamic resharding is enabled.

### 4.6 CLI Tools

**`neard database find-boundary-account --shard-uid XXX` (`tools/database/src/memtrie.rs`):**

Operator tool that loads memtries for a shard, calls `find_trie_split()`, and prints the boundary account and memory split info.

---

## 5. Key Differences Between Static and Dynamic Resharding

| Aspect | Static Resharding | Dynamic Resharding |
|--------|------------------|--------------------|
| **Shard layout source** | `EpochConfig.shard_layout` (determined by protocol version) | `EpochInfo.shard_layout` (stored per-epoch in V5) |
| **When layout changes** | Only on protocol version upgrade | Automatically at epoch boundaries based on trie memory usage |
| **Layout version** | V0, V1, or V2 | V3 (with cumulative split history) |
| **Config type** | `ShardLayoutConfig::Static { shard_layout }` | `ShardLayoutConfig::Dynamic { dynamic_resharding_config }` |
| **Split history** | V2 stores only the most recent split map | V3 stores full cumulative split history + ancestor maps |
| **EpochConfig field** | `shard_layout_config: Static` | `shard_layout_config: Dynamic` |
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

1. **`chain/chain/src/runtime/mod.rs:540-541`**
   ```
   // TODO(dynamic-resharding): Check if the *next* block is the last block of the epoch
   // TODO(dynamic_resharding): Check how many epochs since last resharding
   ```
   The `compute_proposed_split` method currently uses `is_next_block_epoch_start` to check if the current block is the last in the epoch, but there's a note that this check may not correctly handle the "next" block case. The resharding cooldown check (epochs since last resharding) is also marked as incomplete in the runtime layer (though the epoch manager layer does implement it).

2. **`chain/epoch-manager/src/lib.rs:663`**
   ```
   // TODO(dynamic_resharding): remove `next_next_epoch_config` when tests are adjusted
   ```
   The `next_next_shard_layout()` method still takes `next_next_epoch_config` for backward compatibility with tests that use static resharding. This parameter should be removed once all tests are migrated.

3. **`chain/epoch-manager/src/adapter.rs:736`**
   ```
   // TODO(dynamic_resharding): remove this method when dynamic trie loading is implemented
   ```
   The `get_shard_uids_pending_resharding()` method returns an empty result for dynamic resharding. Dynamic trie loading needs to be implemented.

4. **`chain/epoch-manager/src/test_utils.rs:439`**
   ```
   // TODO(dynamic_resharding): Start using BlockInfoV4 in the tests.
   ```
   Test utilities still create `BlockInfoV3` instead of `BlockInfoV4`.

5. **`core/store/src/config.rs:217`**
   ```
   // TODO(dynamic_resharding): decide if we need to support custom cache sizes
   ```
   Cache size computation for dynamic resharding protocol versions is currently skipped.

6. **`tools/fork-network/src/cli.rs:353,707`**
   ```
   // TODO(dynamic_resharding): decide how to support dynamic resharding in fork network
   ```
   Fork network tool does not support dynamic resharding yet (returns error).

7. **`tools/database/src/memtrie.rs:399`**
   ```
   // TODO(dynamic_resharding): decide how to deal with this when dynamic resharding is enabled
   ```
   The CLI tool for finding boundary accounts needs updating for dynamic resharding.

### General Resharding TODOs (May Affect Dynamic Resharding)

8. **`chain/epoch-manager/src/shard_assignment.rs:198`**
   ```
   /// TODO(resharding) - implement shard assignment
   ```
   Shard assignment for validators after resharding is not yet implemented.

9. **`chain/client/src/stateless_validation/shadow_validate.rs:22`**
   ```
   // TODO(resharding) This doesn't work if shard layout changes.
   ```
   Shadow validation breaks across resharding boundaries.

10. **`chain/chain/src/stateless_validation/state_witness.rs:260`**
    ```
    /// TODO(resharding): `get_incoming_receipts_for_shard` generates invalid proofs on resharding boundaries
    ```
    Receipt proofs are invalid across resharding boundaries.

11. **`chain/chain/src/resharding/manager.rs:249`**
    ```
    // TODO(resharding): set all fields of `ChunkExtra`. Consider stronger typing
    ```
    The resharding manager doesn't set all `ChunkExtra` fields (notably the new `proposed_split` field).

12. **`runtime/runtime/src/congestion_control.rs:336`**
    ```
    /// TODO(resharding) - remove the parent outgoing buffer once it's empty.
    ```
    Parent shard's outgoing buffer cleanup after resharding.

13. **`nightly/pytest-sanity.txt:274`**
    ```
    # TODO(resharding) Tests for resharding are disabled because resharding is not compatible with stateless validation, state sync and congestion control.
    ```
    Integration between resharding and other features (stateless validation, state sync, congestion control) is incomplete.

### Spice-Resharding Integration TODOs

14. Multiple TODOs in `chunk_executor_actor.rs`, `spice_data_distributor_actor.rs`, `spice_chunk_validation.rs`, and `spice_chunk_application.rs` indicate that the Spice feature does not yet handle resharding transitions properly.

---

## 7. Commit History

| # | PR | Title | Category |
|---|-----|-------|----------|
| 1 | #14037 | feat(memtrie): find boundary account for shard using memory_usage | Trie split algorithm |
| 2 | #14072 | feat(cli): find boundary account for a potential shard split | CLI tooling |
| 3 | #14117 | tests: memtrie split (boundary account search) | Testing |
| 4 | #14146 | feat(memtrie): make trie split generic (disk/memory) | Trie split generification |
| 5 | #14323 | test: find trie split (big random trie) | Testing + bug fixes |
| 6 | #14395 | feat(trie): optimization for big middle account in finding trie split | Algorithm optimization |
| 7 | #14407 | chore(trie): add error type for trie split, remove unwraps | Error handling |
| 8 | #14504 | feat(runtime): dynamic resharding dry run | Dry run infrastructure |
| 9 | #14536 | feat: protocol feature for dynamic resharding | Protocol feature + EpochInfoV5 |
| 10 | #14678 | chore: split shard_layout into separate source files | Refactoring |
| 11 | #14679 | feat: shard layout v3 | ShardLayoutV3 |
| 12 | #14627 | chore(dynamic_resharding): make shard_layout private in EpochConfig | Architecture enforcement |
| 13 | #14760 | feat: runtime config for dynamic resharding | DynamicReshardingConfig |
| 14 | #14777 | feat: proposed split in chunk header | ShardChunkHeaderInnerV5 |
| 15 | #14791 | feat(runtime): propose shard split | Core proposal logic |
| 16 | #14856 | feat: force and block resharding of particular shards | Operational controls |
| 17 | #14822 | chore: get rid of new_for_ constructors | Code cleanup |
| 18 | #14874 | chore: move DynamicReshardingConfig from RuntimeConfig to EpochConfig | Architecture refactor |
| 19 | #14887 | BlockHeaderV6: added shard_split, removed challenges | BlockHeaderV6 |
| 20 | #14906 | feat(epoch_manager): adjust shard layout (dynamic resharding) | Epoch finalization logic |
| 21 | #15002 | feat(dynamic-resharding): derive ShardLayoutV3 from older versions | V1/V2 -> V3 bootstrap |
| 22 | #15024 | chore(dynamic-resharding): store last_resharding in EpochInfo | Resharding cooldown |
| 23 | #15041 | chore(dynamic-resharding): use current epoch config in next_next_shard_layout | Correctness fix |
| 24 | #15056 | chore(dynamic-resharding): add proposed_split to ChunkHeaderView | RPC/view compatibility |
| 25 | #15058 | feat(dynamic-resharding): move dynamic resharding to nightly | Nightly enablement |
| 26 | #15110 | chore(dynamic-resharding): remove code relying on static layout | Hardening |
| 27 | #15142 | fix(dynamic-resharding): validate shard splits | Security validation |

### Superseded/Removed Code

- **Dry run infrastructure** (PR #14504): The `dynamic_resharding_dry_run` boolean config option was removed in PR #14791 when the feature was integrated into the actual chunk application pipeline.
- **`DynamicReshardingConfig` in `RuntimeConfig`** (PR #14760): Moved to `EpochConfig` in PR #14874. The `DynamicReshardingConfigView`, all `Parameter` enum variants, and the `252.yaml` override file were all removed.
- **`new_for_dynamic_resharding()` constructors** (PR #14791): Replaced with a unified `new()` constructor in PR #14822.
- **`legacy_shard_layout()` naming** (PR #14627): Renamed to `static_shard_layout()` in PR #14874.
- **Backward epoch iteration in `can_reshard()`** (PR #14906): Replaced with O(1) `last_resharding` check in PR #15024.
- **Panicking `static_shard_layout()` accessor** (PR #14627/PR #15058): Changed to return `Option<ShardLayout>` in PR #15110.
- **Protocol version 252**: Dynamic resharding was initially at version 252 to avoid test conflicts (PR #14627), moved to nightly version 150 in PR #15058.
