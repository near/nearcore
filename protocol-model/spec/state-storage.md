# State & storage

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `core/store/src/trie/mod.rs`, `core/store/src/trie/raw_node.rs`, `core/store/src/trie/shard_tries.rs`, `core/store/src/trie/state_parts.rs`, `core/store/src/trie/trie_recording.rs`, `core/store/src/trie/trie_storage.rs`, `core/store/src/trie/split.rs`, `core/store/src/trie/state_snapshot.rs`, `core/store/src/flat/{storage,delta,chunk_view,mod}.rs`, `core/store/src/db/{refcount,splitdb,colddb}.rs`, `core/store/src/adapter/trie_store.rs`, `core/store/src/columns.rs`, `core/primitives/src/trie_key.rs`, `core/primitives/src/state.rs`

## Role

This component defines how per-shard state is represented, hashed, persisted, and proved. The canonical state is a Merkle-Patricia trie (MPT) per shard, keyed by encoded `TrieKey`s; its root hash is the `StateRoot` committed in each chunk's `ChunkExtra`. On top of the trie sit three derived/auxiliary structures: **flat storage** (a flat key→value-ref map per shard giving near-O(1) reads), **in-memory tries (memtries)** (the whole trie kept in an arena for fast reads/writes and witness generation), and **partial storage / proofs** (the set of trie nodes touched during a chunk, replayed during stateless validation). State mutations are batched in `TrieUpdate` (see [runtime-execution](runtime-execution.md)) and committed as `TrieChanges` (refcount deltas) into the reference-counted `DBCol::State` column. It is read and written by [runtime-execution](runtime-execution.md) and VM host functions; it produces state parts for [sync](sync.md), produces `PartialStorage` proofs for [stateless-validation](stateless-validation.md), and its trie/flat split mechanics feed resharding ([sharding-chunks](sharding-chunks.md)).

## Key data structures

- **`RawTrieNode` / `RawTrieNodeWithSize`** — `core/store/src/trie/raw_node.rs:27` / `:11` — the on-disk node. Four variants (borsh discriminant explicit): `Leaf(Vec<u8> key, ValueRef) = 0`, `BranchNoValue(Children) = 1`, `BranchWithValue(ValueRef, Children) = 2`, `Extension(Vec<u8> key, CryptoHash child) = 3`. `RawTrieNodeWithSize` wraps a node with a `memory_usage: u64` that *is* serialized and contributes to the hash (comment `raw_node.rs:9`). The node hash is `CryptoHash::hash_bytes(borsh(self))` (`raw_node.rs:17` — `RawTrieNodeWithSize::hash`).
- **`Children<T = CryptoHash>`** — `core/store/src/trie/raw_node.rs:50` — `[Option<T>; 16]` (`NUM_CHILDREN = 16`, `mod.rs:72`). Serialized as a 16-bit `ChildrenMask` bitmap followed by only the present children (`raw_node.rs:79` serialize / `:94` deserialize), so empty slots cost nothing on disk.
- **`ValueRef`** — `core/primitives/src/state.rs:46` — `{ length: u32, hash: CryptoHash }`; the trie stores value *references*, not values, so gas can be charged on `length` before the value is dereferenced. The value itself lives in `DBCol::State` keyed by its own hash.
- **`FlatStateValue`** — `core/primitives/src/state.rs:107` — `Ref(ValueRef) = 0` or `Inlined(Vec<u8>) = 1`. `on_disk` inlines values `<= INLINE_DISK_VALUE_THRESHOLD` (= 4000 bytes, `core/primitives-core/src/config.rs:14`; `should_inline` at `state.rs:149`, called by `on_disk` at `state.rs:116`), otherwise stores a ref; inlining lets short values be served with a single DB read.
- **`TrieKey`** — `core/primitives/src/trie_key.rs:171` — the logical state key, a `#[repr(u8)]` + `#[borsh(use_discriminant = true)]` enum whose discriminant is the column byte (each variant `= col::X`, `trie_key.rs:171`–`:187`; the `col` byte constants at `trie_key.rs:21`). `append_into` (`trie_key.rs:452`) serializes it as `col_byte ++ account_id_bytes ++ [separator ++ suffix]`; this byte string is the path nibbled into the trie. Column bytes form a logical layout: account 0, contract code 1, access key 2 (`trie_key.rs:24`–`:29`), received data 3, postponed-receipt-id 4, postponed-receipt 6, delayed-receipt/indices 7, contract data 9, promise-yield 10–12, buffered-receipt indices/data 13–14, bandwidth scheduler state 15, buffered-receipt-group queue 16–17, global contract code/nonce 18–19, promise-yield status 20 (`trie_key.rs:33`–`:77`).
- **`PartialState`** — `core/primitives/src/state.rs:14` — `TrieValues(Vec<TrieValue>) = 0`, a set of unique serialized nodes/values (`TrieValue = Arc<[u8]>`, `state.rs:7`). Used both for proofs (`PartialStorage`) and for state parts.
- **`PartialStorage`** — `core/store/src/trie/mod.rs:76` — `{ nodes: PartialState }`; the recorded proof from a chunk's reads, replayable to reconstruct the same access pattern.
- **`TrieChanges`** — `core/store/src/trie/mod.rs:478` — `{ old_root, new_root, insertions: Vec<TrieRefcountAddition>, deletions: Vec<TrieRefcountSubtraction>, memtrie_changes (borsh-skipped), children_memtrie_changes (borsh-skipped) }`. The persisted unit of state mutation: insertions/deletions are refcount deltas on node/value hashes. The doc comment (`mod.rs:459`–`:476`) explains the fork model — to keep two child states, apply insertions of both; to discard a state, apply its insertions as deletions.
- **`TrieRefcountAddition` / `TrieRefcountSubtraction`** — `core/store/src/trie/mod.rs:309` / `:333` — an addition carries `(hash, payload, rc: NonZeroU32)`; a subtraction carries only `(hash, rc)` (the payload field is a vestigial `IgnoredVecU8`, `mod.rs:339`/`:346`). Built from a `TrieRefcountDeltaMap` (`mod.rs:392`) whose `into_changes` (`mod.rs:416`) splits net-positive vs net-negative refcounts and sorts both for a canonical representation (`mod.rs:437`).
- **`FlatStateChanges` / `FlatStateDelta` / `FlatStateDeltaMetadata`** — `core/store/src/flat/delta.rs:61` / `:13` / `:29` — a delta is per `(shard, block)`: `changes` is `HashMap<Vec<u8> key, Option<FlatStateValue>>` (`None` = deletion), `metadata.block` is the `BlockInfo`, and `prev_block_with_changes` (`delta.rs:34`) links to the previous changed block so empty blocks can be skipped while walking.
- **`FlatStorageChunkView`** — `core/store/src/flat/chunk_view.rs:17` — the per-chunk read handle embedded in `Trie`; resolves `get_value`/`contains_key` (`chunk_view.rs:41`/`:45`) against the flat head plus the delta chain up to its `block_hash`.
- **`Trie`** — `core/store/src/trie/mod.rs:245` — a read/update handle bound to one `(storage, root)`. Optional fields select the read path: `memtries` (memtrie), `flat_storage_chunk_view` (flat), and `recorder: Option<TrieRecorder>` (proof capture). `children_memtries` (`mod.rs:251`) exists only to keep child memtries consistent across forks near a resharding boundary.
- **`WrappedTrieChanges`** — `core/store/src/trie/shard_tries.rs:795` — bundles `TrieChanges` + `state_changes: Vec<RawStateChangesWithTrieKey>` + `shard_uid` + `block_height` for committing a chunk's results into the store.

## Behavior

### Trie structure, paths, and hashing

1. **Path.** A key's bytes are split into 4-bit nibbles (`NibbleSlice`, `mod.rs:9`); the trie is walked nibble by nibble. A branch has 16 child slots (one per nibble) plus an optional value. An extension compresses a shared nibble run. A leaf holds the remaining key tail (`extension`) plus a `ValueRef`.
2. **Hashing.** Each node's hash is `hash(borsh(RawTrieNodeWithSize { node, memory_usage }))` (`raw_node.rs:17`). Because `memory_usage` is inside the hashed bytes, two structurally identical subtrees with different aggregate sizes hash differently. The `StateRoot` is the hash of the root node; the empty trie root is `Trie::EMPTY_ROOT = StateRoot::new()` (`mod.rs:605`).
3. **On-disk lookup** walks from the root via `lookup_from_state_column` (`mod.rs:1269`): retrieve node by hash, match Leaf/Extension/Branch, descend or return the `ValueRef`. `retrieve_raw_node` short-circuits `EMPTY_ROOT` to `None` (`mod.rs:1173`).

### Read path selection (`get_optimized_ref`)

`Trie::get_optimized_ref` (`mod.rs:1526`) picks the source in priority order, controlled by `KeyLookupMode` (`mod.rs:96`):

1. **Memtrie**, if `self.memtries.is_some()` — `lookup_from_memory` (`mod.rs:1341`); under a read lock it walks the arena and, if `use_trie_accounting_cache` or witness recording is on, serializes each visited node and feeds it to the access tracker and/or recorder (`mod.rs:1353`–`:1360`).
2. **Flat storage**, if mode is `MemOrFlatOrTrie` and a `flat_storage_chunk_view` is present — `lookup_from_flat_storage` (`mod.rs:1241`, dispatched at `mod.rs:1537`). If recording, it *also* walks the on-disk trie to capture the nodes proving the value (or its absence) and `debug_assert`s the two agree (`mod.rs:1248`–`:1258`).
3. **On-disk trie** otherwise — `lookup_from_state_column` (`mod.rs:1540`).

`get_optimized_ref` returns an `OptimizedValueRef` (`mod.rs:558`): either a `Ref(ValueRef)` or an `AvailableValue(ValueAccessToken)` (the value already in hand, e.g. inlined in flat storage; `mod.rs:560`, with `from_flat_value` mapping an inlined flat value into it at `mod.rs:573`). `deref_optimized` (`mod.rs:1550`) turns it into bytes, charging gas / recording exactly as if the value had been read from the trie — this keeps gas accounting identical regardless of which read path served the request (`mod.rs:1546` doc; the `AvailableValue` arm still calls `track_mem_lookup`/`track_disk_lookup` and `recorder.record`, `mod.rs:1559`–`:1571`).

### Update path (`update` → `TrieChanges`)

`Trie::update` (`mod.rs:1585`) takes an iterator of `(key_bytes, Option<value>)` (`Some` = set, `None` = delete). It first force-reads any contract codes the recorder asked to record (`recorder.codes_to_record`, `mod.rs:1591`–`:1602`), then branches:

- **Memtrie present** → `update_with_memtrie` (`mod.rs:1611`): obtains a tracking mode (`RefcountsAndAccesses(recorder)` when recording, else `Refcounts`, `mod.rs:1621`–`:1626`), applies inserts/deletes to the main memtrie (and, rarely, to child memtries for resharding forks, `mod.rs:1644`–`:1665`), and calls `to_trie_changes` to emit `TrieChanges` including `memtrie_changes` (`mod.rs:1667`).
- **No memtrie** → `update_with_trie_storage` (`mod.rs:1677`): copies the root into a mutable `TrieStorageUpdate`, applies `generic_insert`/`generic_delete` per change (`mod.rs:1687`+), then flattens new `RawTrieNodeWithSize` nodes, computes their hashes, and produces `TrieChanges`.

Either way the result's `insertions`/`deletions` are refcount deltas: every newly created node/value is a +1 (or +N if shared), every node that became unreferenced along the path is a −1.

### Commit path (`TrieChanges` → DB) and refcounting GC

`ShardTries::apply_all` (`shard_tries.rs:382`) writes a chunk's `TrieChanges` into a `TrieStoreUpdateAdapter`:

1. `apply_insertions_inner` (`shard_tries.rs:281`) calls `increment_refcount_by(shard_uid, hash, payload, rc)` for each insertion (`shard_tries.rs:289`), and updates the in-memory `TrieCache`.
2. `apply_deletions_inner` (`shard_tries.rs:266`) calls `decrement_refcount_by(shard_uid, hash, rc)` for each deletion (`shard_tries.rs:274`).
3. `WrappedTrieChanges` drives the full commit: `insertions_into` / `deletions_into` (`shard_tries.rs:837`/`:842`), `state_changes_into` (writes `DBCol::StateChanges`, `shard_tries.rs:856`), `trie_changes_into` (writes `DBCol::TrieChanges` for GC/undo, `shard_tries.rs:895`), and `apply_mem_changes` (applies memtrie changes, `shard_tries.rs:832`).

The `DBCol::State` column is reference-counted (`columns.rs:540` — `is_rc` returns true for `State`). Refcounts are stored as a little-endian `i64` suffix on the value; RocksDB's merge operator sums them at compaction and **removes the key when the count hits 0** (`db/refcount.rs:110` — `refcount_merge`, with `Ordering::Equal => Vec::new()` at `:126`). The DB row key for a node/value is `shard_uid.to_bytes() (8) ++ node_or_value_hash (32)` = 40 bytes (`adapter/trie_store.rs:234` — `get_key_from_shard_uid_and_hash`), so identical content under different shards is stored separately. A missing node read yields `StorageError::MissingTrieValue` (`adapter/trie_store.rs:58`). Because deletions corrupt GC refcounts if replayed, resharding writes use `TrieChanges::insertions_only` (`mod.rs:517`).

`revert_insertions` (`shard_tries.rs:360`) applies each insertion's `.revert()` (`mod.rs:379`) as a subtraction — this is how a discarded fork's nodes are released.

### Flat storage

Flat storage maps every trie *leaf* key to a `FlatStateValue` for the state at a particular block, avoiding root-to-leaf traversal (module doc `flat/mod.rs:1`). It is split into:

- A **flat head**: the on-disk `DBCol::FlatState` snapshot at one block (normally the last final block), plus its `FlatStorageStatus::Ready` marker (`flat/storage.rs:241`, `:404`).
- **In-memory deltas** per block from the head forward, cached in `FlatStorageInner.deltas` and persisted in `DBCol::FlatStateChanges` / `DBCol::FlatStateDeltaMetadata` (`columns.rs:268`/`:272`).

A read for `(block_hash, key)` (`flat/storage.rs:297` — `FlatStorage::get_value`) collects the delta chain from `block_hash` back to the head via `get_blocks_to_head` (`flat/storage.rs:88`, following `prev_block_with_changes` to skip empty blocks, `:108`), returns the first delta that mentions the key, and otherwise reads the on-disk flat head (`flat/storage.rs:315`). This read path yields only `ValueRef`s from deltas, never inlined values (`flat/storage.rs:309`); the actual value is then fetched from `DBCol::State`. `add_delta` (`flat/storage.rs:441`) appends a block's delta, rejecting a block whose parent is neither the head nor a known delta (`flat/storage.rs:451`). `update_flat_head` (`flat/storage.rs:432` → `update_flat_head_impl` `:364`) advances the head by applying intervening deltas to `DBCol::FlatState`, rewriting the `Ready` status (`:404`), and pruning deltas at or below the new head — all in per-block store commits so a crash mid-advance is recoverable. Head advancement is suppressed while any `FlatHeadHold` is active (`update_flat_head_impl` early-returns when `move_head_hold_count > 0`, `flat/storage.rs:370`), which subsystems like state snapshots and background memtrie loading use to pin the head (`hold_flat_head`, `:494`).

Flat storage and memtries are *node-local optimizations*, not consensus state — but which of them is loaded changes trie-node-touched (TTN) counting and therefore gas, so it is protocol-relevant (`KeyLookupMode` doc `mod.rs:100`–`:102`; `Trie` field doc `mod.rs:261`–`:274`).

### Partial storage (proofs) for stateless validation

When a `Trie` is built with `recording_reads_new_recorder`/`recording_reads_with_proof_size_limit` (`mod.rs:652`/`:660`), every node/value retrieved through `internal_retrieve_trie_node` (`mod.rs:778`) and every dereferenced value is fed to a `TrieRecorder.record` (`trie_recording.rs:109`). Crucially, reads that hit flat storage or memtrie *also* record the equivalent on-disk trie nodes (`mod.rs:1248`, `mod.rs:1353`) so the proof can stand alone. `recorded_storage` (`trie_recording.rs:165`) drains the recorder into a sorted `PartialStorage` (`:174`). Size is tracked live: `recorded_storage_size` (actual, `:199`) and `recorded_storage_size_upper_bound` (`:203`), the latter incremented by 2000 bytes per key removal (`record_key_removal`, `:140`–`:142`) and by contract-code length (`record_code_len`, `:148`–`:155`); `check_proof_size_limit_exceed` (`:161`) compares the upper bound against `proof_size_limit`. A verifier reconstructs a `Trie` from the proof via `from_recorded_storage` (`mod.rs:714`), backed by `TrieMemoryPartialStorage` which returns `MissingTrieValue` for any hash not in the proof (`trie_storage.rs:325`–`:334`). Detail of how the witness is transmitted/verified: [stateless-validation](stateless-validation.md).

### State parts (for sync)

State is partitioned into `num_parts` contiguous ranges by DFS order, with each part boundary at the node whose prefix-sum of `memory_usage` crosses `total_size / num_parts * part_id` (`state_parts.rs:52` — `find_state_part_boundary`, threshold at `:68`). Boundaries are found by descending the trie following accumulated `memory_usage` (`find_node_in_dfs_order` `:381`, `find_child_in_dfs_order` `:325`). `get_trie_nodes_for_part_with_flat_storage` (`state_parts.rs:161`) builds a part: capture boundary nodes via a recording trie, iterate flat storage over the part's key range, look up referenced values in `DBCol::State`, rebuild a local trie, unite boundary + local nodes and `visit_nodes_for_state_part` (`:308`) to emit the exact `PartialState`. Each part includes the root-to-boundary paths and all left siblings, so a receiver can verify positions from just the state root (module doc `state_parts.rs:14`). `validate_state_part` (`state_parts.rs:400`) reconstructs a trie from the part, visits its nodes, and requires that visited-node count equals provided-node count (else `UnexpectedTrieValue`, `:418`). `apply_state_part` (`state_parts.rs:472` → `apply_state_part_impl` `:423`) traverses the part building a `TrieChanges` of +1 insertions plus a `FlatStateChanges` delta, and is `expect`ed infallible for a valid part (`:478`). Part *format/transport*: [sync](sync.md).

### State snapshots

A `StateSnapshot` (`state_snapshot.rs:65`) is a read-only checkpoint of the hot store taken at `prev_block_hash` (`:67`), with flat storage created (via `FlatStorageManager::mark_ready_and_create_flat_storage`, `state_snapshot.rs:88`) and its flat head moved to the last-chunk replay point for each included shard on construction (`StateSnapshot::new`, `state_snapshot.rs:78`–`:118`). It is the source for generating state parts without blocking live state. The flat head is pinned by `FlatHeadHold` guards held on the manager side while a snapshot is pending (`flat/manager.rs:94`, `:312`; the `holds` field `:30`), not by the `StateSnapshot` struct itself.

### Trie split (resharding mechanics)

`Trie::retain_split_shard` (`mod.rs:1746`) produces a `TrieChanges` retaining only the left or right side of a boundary account (memtrie variant `retain_split_shard_with_memtrie` `:1758`, trie-storage variant `:1786`). The split logic (`core/store/src/trie/split.rs`) descends the account-keyed subtrees `SUBTREES = [ACCOUNT, CONTRACT_CODE, ACCESS_KEY, CONTRACT_DATA]` (`split.rs:26`), since shards are split by `AccountId`, not arbitrary bytes; attached-data subtrees (access keys, contract data) are separated from the account id by a separator byte (`split.rs:38`). The per-key retain intervals for a boundary account are computed in `core/store/src/trie/ops/resharding.rs` (`boundary_account_to_intervals`, `:35`). Orchestration: [sharding-chunks](sharding-chunks.md).

### Hot/cold (archival) database split

`SplitDB` (`db/splitdb.rs:18`) presents a hot+cold pair: reads hit hot first, falling back to cold only for cold-eligible columns (`get_raw_bytes` `:62`, gated on `col.is_cold()` `:66`); iterators merge-sort the two (`:95`). `ColdDB` (`db/colddb.rs:16`) stores the same byte format but forces every refcounted value's rc to 1 (cold data is never GC'd) and ignores/asserts-against decrements and deletes (module doc `colddb.rs:9`–`:16`; `assert_is_in_colddb` `:34`). Only columns for which `is_cold` returns true are copied to cold (`columns.rs:554`); `State`/`StateChanges` are cold (`columns.rs:575`/`:576`), while `StateParts`/`StatePartsApplied` (`columns.rs:613`) and `TrieChanges` (`columns.rs:615`) are explicitly *not* cold.

## Interactions

| Direction | Component | What crosses the boundary |
|-----------|-----------|---------------------------|
| consumes/produces | [runtime-execution](runtime-execution.md) | `TrieUpdate` reads/writes; emits `TrieChanges` + `RawStateChangesWithTrieKey`; VM host functions read via `get_optimized_ref`/`deref_optimized` with gas-accurate TTN counting; enforces per-receipt proof size via `recorded_storage_size_upper_bound` |
| produces | [sync](sync.md) | state parts (`PartialState`) generated/validated/applied here; transport & scheduling there |
| produces | [stateless-validation](stateless-validation.md) | `PartialStorage` proof recorded here; witness assembly/verification there |
| feeds | [sharding-chunks](sharding-chunks.md) | trie split (`retain_split_shard`) and flat-storage resharding mechanics |
| uses | [data-structures-serialization](data-structures-serialization.md) | borsh node encoding, `CryptoHash`, `TrieKey` |

## Protocol-version-gated behavior

Nearly all structures described here activated **before** the pinned stable version 86 (`STABLE_PROTOCOL_VERSION = 86`, `core/primitives-core/src/version.rs:628`) and are now baseline (their features were deprecated and their behavior is unconditionally on):

- **Flat storage reads** (NEP-399) — `_DeprecatedFlatStorageReads` (`version.rs:177`), activated at protocol version 61 (`version.rs:507`). Flat storage as a read path is always available at v86.
- **Stateless validation** (NEP-509) — `_DeprecatedStatelessValidation` (`version.rs:225`), activated at version 69 (`version.rs:520`). Proof recording (`PartialStorage`) and proof-size limits are baseline.
- **Exclude contract code from witness** — `_DeprecatedExcludeContractCodeFromStateWitness` (`version.rs:271`), version 73 (`version.rs:528`); contract codes recorded separately via the recorder's `codes_to_record` set (`mod.rs:1591`–`:1602`).
- **State-stored receipts** — `_DeprecatedStateStoredReceipt` (`version.rs:259`), version 72 (`version.rs:527`) — and **bandwidth scheduler** `_DeprecatedBandwidthScheduler` (`version.rs:274`), version 74 (`version.rs:534`) — populate the `BandwidthSchedulerState` / buffered-receipt-group `TrieKey` columns (`trie_key.rs:65`, `:68`–`:70`); always present at v86.

**Active at v86 (the one state-storage-relevant feature at the pinned version):**

- **`EnforcePerReceiptStorageProofLimit`** — `core/primitives-core/src/version.rs:449`, activates at **protocol version 86** (`version.rs:576`). This is the sole PV-86 feature on the 2.13.0 release. It gates *enforcement* of the per-receipt storage-proof limit: at v86, before executing a receipt the runtime snapshots the recorder's `recorded_storage_size_upper_bound()` (this component's live proof-size accumulator) and enforces the per-receipt delta against the limit; pre-v86 the snapshot is `None` and no per-receipt bound is enforced (`runtime/runtime/src/lib.rs:838`–`:845`). The size-accounting mechanism it relies on (`record_key_removal`'s 2000-byte charge, `record_code_len`, `recorded_storage_size_upper_bound`) lives here; the enforcement decision is in [runtime-execution](runtime-execution.md). Note: an older baseline showed PV 86 as `FixContractLoadingError`; on the 2.13.0 stable tree there is no such feature — `FixContractLoadingCost` is a nightly feature at version 129 (`version.rs:579`).

Features named as v83 baseline in this tree (all activate at 83, `version.rs:549`–`:557`) — `ExcludeExistingCodeFromWitnessForCodeLen`, `InvalidTxGenerateOutcomes`, `FixAccessKeyAllowanceCharging`, `IncludeDeployGlobalContractOutcomeBurntStorage`, `GlobalContractDistributionNonce`, `EthImplicitGlobalContract` — are active named (non-deprecated) enum variants but do not change the state/storage representation described here. `MIN_SUPPORTED_PROTOCOL_VERSION = 83` (`version.rs:600`).

In-memory tries (memtries) are a node configuration / runtime optimization, **not** gated by a `ProtocolFeature`: `Trie` is constructed with or without memtries by the node, and TTN gas is kept identical across read paths by design (`mod.rs:261`, `mod.rs:1546`).

## Invariants & failure modes

- **Canonical refcount representation.** `TrieRefcountDeltaMap::into_changes` sorts insertions/deletions so equal `TrieChanges` serialize identically (`mod.rs:437`). Refcount reaching 0 deletes the DB key (`db/refcount.rs:126`).
- **Same key ⇒ same value.** Refcounted columns must never store differing values under one key; `refcount_merge` `debug_assert`s equal payloads (`db/refcount.rs:119`).
- **`memory_usage` integrity.** A `memory_usage` mismatch found while computing a state-part boundary returns `StorageInconsistentState` (`state_parts.rs:363`, in `find_child_in_dfs_order`).
- **Missing node.** Reading an absent hash returns `StorageError::MissingTrieValue` (disk: `adapter/trie_store.rs:58`; partial storage: `trie_storage.rs:333`).
- **State part validity.** `validate_state_part` requires visited-node count == provided-node count, else `UnexpectedTrieValue` (`state_parts.rs:415`–`:419`); `apply_state_part` is documented infallible for a valid part and `expect`s success (`state_parts.rs:477`).
- **NotWritableToDisk never finalized.** `state_changes_into` asserts no `StateChangeCause::NotWritableToDisk` change is committed — `"NotWritableToDisk changes must never be finalized."` (`shard_tries.rs:862`–`:868`).
- **Flat delta continuity.** `add_delta` rejects a block whose parent is neither the flat head nor a cached delta (`flat/storage.rs:451`); reads error with `BlockNotSupported` if no path to head exists (`flat/storage.rs:100`).
- **Flat head holds.** Head cannot advance while `move_head_hold_count > 0` (`flat/storage.rs:370`); a `FlatHeadHold` increments the count on creation (`:508`) and decrements on drop.
- **Cold storage immutability.** `ColdDB` forces refcounts to 1 and ignores/asserts-against decrements and deletes (`db/colddb.rs:9`–`:16`).

## Code anchors

| Location | Symbol | What happens here |
|----------|--------|-------------------|
| `core/store/src/trie/raw_node.rs:27` | `RawTrieNode` | 4 node variants (leaf/branch×2/extension) |
| `core/store/src/trie/raw_node.rs:17` | `RawTrieNodeWithSize::hash` | node hash = `hash(borsh(node, memory_usage))` |
| `core/store/src/trie/raw_node.rs:50`/`:79` | `Children` | 16-slot array, serialized as bitmap + present children |
| `core/primitives/src/state.rs:46` | `ValueRef` | `{length, hash}`; trie stores refs not values |
| `core/primitives/src/state.rs:107`/`:116` | `FlatStateValue::on_disk` | inline short values (≤4000B), ref long ones |
| `core/primitives/src/trie_key.rs:171`/`:452` | `TrieKey`/`append_into` | key enum; discriminant = column byte; byte encoding |
| `core/primitives/src/trie_key.rs:21` | `col::*` | logical column byte constants (account 0 … promise-yield-status 20) |
| `core/store/src/trie/mod.rs:1526` | `Trie::get_optimized_ref` | read-path selection memtrie→flat→disk |
| `core/store/src/trie/mod.rs:1269` | `lookup_from_state_column` | root-to-leaf nibble walk |
| `core/store/src/trie/mod.rs:1241` | `lookup_from_flat_storage` | flat read + trie record-for-proof |
| `core/store/src/trie/mod.rs:1341` | `lookup_from_memory` | memtrie arena walk + record |
| `core/store/src/trie/mod.rs:1550` | `deref_optimized` | dereference value with gas/record parity |
| `core/store/src/trie/mod.rs:1585`/`:1611`/`:1677` | `Trie::update*` | apply changes → `TrieChanges` (memtrie vs storage) |
| `core/store/src/trie/mod.rs:478`/`:416` | `TrieChanges`/`into_changes` | refcount delta unit; canonical sort |
| `core/store/src/trie/mod.rs:517` | `insertions_only` | resharding writes drop deletions |
| `core/store/src/trie/shard_tries.rs:382`/`:281`/`:266` | `apply_all`/inner | commit insertions/deletions as refcount ops |
| `core/store/src/trie/shard_tries.rs:360` | `revert_insertions` | release a discarded fork's nodes |
| `core/store/src/trie/shard_tries.rs:795`/`:856`/`:895` | `WrappedTrieChanges` | bundle + write State/StateChanges/TrieChanges |
| `core/store/src/adapter/trie_store.rs:234`/`:58` | `get_key_from_shard_uid_and_hash`/`get` | State row key = shard_uid++hash; missing node ⇒ MissingTrieValue |
| `core/store/src/db/refcount.rs:110`/`:35` | `refcount_merge`/`decode_value_with_rc` | rc summed on compaction; 0 ⇒ delete |
| `core/store/src/flat/storage.rs:297`/`:88` | `get_value`/`get_blocks_to_head` | delta-chain read then flat head |
| `core/store/src/flat/storage.rs:364`/`:441` | `update_flat_head_impl`/`add_delta` | advance head / append block delta |
| `core/store/src/flat/delta.rs:61`/`:105` | `FlatStateChanges`/`from_state_changes` | per-block key→value-ref delta |
| `core/store/src/trie/trie_recording.rs:109`/`:165` | `record`/`recorded_storage` | proof capture → sorted `PartialStorage` |
| `core/store/src/trie/trie_recording.rs:142`/`:161`/`:203` | `record_key_removal`/`check_proof_size_limit_exceed`/`recorded_storage_size_upper_bound` | proof-size accounting for per-receipt limit |
| `core/store/src/trie/mod.rs:714`/`trie_storage.rs:311` | `from_recorded_storage`/`TrieMemoryPartialStorage` | verifier-side trie over a proof (missing hash ⇒ MissingTrieValue, `trie_storage.rs:333`) |
| `core/store/src/trie/state_parts.rs:52`/`:161`/`:400`/`:472` | state part gen/validate/apply | DFS-by-memory partition; validate==visited; apply ⇒ +1 inserts |
| `core/store/src/trie/state_snapshot.rs:65` | `StateSnapshot` | read-only epoch-boundary checkpoint |
| `core/store/src/trie/mod.rs:1746`/`split.rs:26` | `retain_split_shard`/`SUBTREES` | resharding trie split by account boundary |
| `core/store/src/db/splitdb.rs:18`/`:62`, `colddb.rs:16`/`:34` | `SplitDB`/`ColdDB` | hot→cold read fallback; cold rc forced to 1 |
| `core/store/src/columns.rs:540`/`:554`/`:268` | `is_rc`/`is_cold`/`FlatState*` cols | State is refcounted; cold-eligibility; flat columns |
| `runtime/runtime/src/lib.rs:838` | `EnforcePerReceiptStorageProofLimit` gate | v86 per-receipt proof-size enforcement using recorder upper bound |
| `core/primitives-core/src/version.rs:628`/`:507`/`:520`/`:576` | version constants/feature versions | stable=86; flat-reads@61; stateless@69; EnforcePerReceiptStorageProofLimit@86 |

## Open questions

- Whether any node currently runs without memtries in the v86 production configuration (and thus relies on the flat/disk TTN-equivalence path) is a deployment fact not determinable from this code alone.
