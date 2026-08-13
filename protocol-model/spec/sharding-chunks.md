# Sharding & chunk lifecycle

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `core/primitives/src/shard_layout/`, `core/primitives/src/sharding.rs`, `core/primitives/src/reed_solomon.rs`, `chain/client/src/chunk_producer.rs`, `chain/chunks/src/{shards_manager_actor,logic,chunk_cache,client}.rs`, `chain/chain/src/resharding/`, `chain/epoch-manager/src/{lib,shard_assignment}.rs`

## Role

NEAR partitions account state across **shards**; each block carries one **chunk** per shard. This component covers the chunk lifecycle: how `ShardLayout` maps accounts to shards, how a chunk producer builds a chunk for its assigned shard and Reed–Solomon-encodes it into parts, how those parts and receipt proofs are distributed and reconstructed via the `ShardsManager`, what a *missing* chunk means, and **dynamic resharding** (splitting a shard at an epoch boundary). It sits downstream of [epoch management](epoch-validators-staking.md) (which assigns chunk producers and part owners), feeds [stateless validation](stateless-validation.md) (chunk *validation* via witnesses is out of scope here) and [chain & block processing](chain-block-processing.md), and the state transition a chunk applies belongs to [runtime execution](runtime-execution.md). Resharding storage mechanics (trie/flat-storage migration, memtrie preload) live in [state & storage](state-storage.md).

## Key data structures

- **`ShardLayout`** — `core/primitives/src/shard_layout/mod.rs:64` — versioned enum `V0|V1|V2|V3` holding everything needed to assign accounts to shards plus parent/child split history. V0 hashes account ids; V1/V2/V3 use sorted account-id boundaries. V2 introduced non-contiguous, stable `ShardId`s distinct from `ShardIndex`; V3 (dynamic resharding) additionally stores cumulative split history + ancestor maps. `single_shard()` (`:94`) builds a V2 single-shard layout.
- **`ShardUId`** — `core/primitives/src/shard_layout/mod.rs:479` — `{version: u32, shard_id: u32}`. Globally unique across layouts (because it carries the layout `version`), used as the DB/storage key for shard-indexed state; `ShardId` alone is the protocol-facing ordinal exposed in chunk headers. `to_bytes()` (`:497`) gives the 8-byte storage prefix (little-endian `version` then `shard_id`).
- **`ShardChunkHeader`** — `core/primitives/src/sharding.rs:391` — enum `V1|V2|V3`; production builds `V3` (`ShardChunkHeaderV3`, `:238`). Carries `prev_state_root`, `encoded_merkle_root`, `encoded_length`, `height_created`, `height_included`, `shard_id`, gas/balance, `prev_outgoing_receipts_root`, `tx_root`, congestion info, bandwidth requests, and (via the V5 inner) `proposed_split`.
- **`ShardChunkHeaderInner`** — `core/primitives/src/sharding/shard_chunk_header_inner.rs` — versioned; the V5 inner adds `proposed_split: Option<TrieSplit>` for dynamic resharding, and the V6 inner is the spice tx-only variant. Accessors gate on the version.
- **`EncodedShardChunkBody`** — `core/primitives/src/sharding.rs:1201` — `parts: Vec<Option<Box<[u8]>>>`, one slot per total part. `get_merkle_hash_and_paths()` (`:1218`) merklizes the parts into `encoded_merkle_root` + per-part `MerklePath`.
- **`TransactionReceipt`** — `core/primitives/src/sharding.rs:1229` — `(Vec<SignedTransaction>, Vec<Receipt>)`; this is the payload that is Reed–Solomon-encoded into the parts.
- **`ShardChunkWithEncoding`** — `core/primitives/src/sharding.rs:1420` — pairs the decoded `ShardChunk` with its `EncodedShardChunk` so encode/decode is done once and the two are guaranteed consistent.
- **`PartialEncodedChunk` / `PartialEncodedChunkPart` / `ReceiptProof`** — `chain/chunks/src/logic.rs` (constructed there, e.g. `create_partial_chunk` `:136`) — the over-the-wire unit: a header plus a subset of parts (each with its merkle proof) plus the receipt proofs the recipient needs.
- **`ChunkExtra` (V5)** — `core/primitives/src/types.rs` — per-(block,shard) post-application summary (state root, gas, congestion info, …); V5 adds `proposed_split`, the authoritative locally-computed split proposal used to validate incoming chunk headers.
- **`TrieSplit`** — `core/primitives/src/trie_split.rs` — result of the split search: `boundary_account: AccountId`, `left_memory`, `right_memory`; `total_memory()` returns their sum. Embedded in chunk headers and `ChunkExtra`.
- **`ReshardingSplitShardParams`** — `chain/chain/src/resharding/event_type.rs:19` — `{parent_shard, left_child_shard, right_child_shard, boundary_account, resharding_block}`; the deduced split event for the storage layer.
- **`AssignmentStrategy`** — `chain/epoch-manager/src/shard_assignment/mod.rs:140` — `CarryOver | Fresh | StickyResharding{shard_idx_mapping}`; chooses how chunk-producer→shard assignment carries across an epoch boundary.

## Behavior

### Account → shard mapping

1. `ShardLayout::account_id_to_shard_id` (`core/primitives/src/shard_layout/mod.rs:203`) dispatches by version. For V2/V3 the boundary accounts are sorted; the shard index is `boundary_accounts.partition_point(|x| x <= account_id)` and the result is `shard_ids[idx]` (`core/primitives/src/shard_layout/v2.rs:265`). V0 hashes the account id instead.
2. `account_id_to_shard_uid` (`:214`) wraps the id with the layout `version` to get the storage-unique `ShardUId`. Note: receipts must use `Receipt::receiver_shard`, not this function (doc comment `:201`).
3. `get_shard_index` (`:374`) maps `ShardId`→array index (identity for V0/V1, lookup for V2/V3). Parent/child relations: `get_children_shards_ids` (`:241`), `try_get_parent_shard_id` (`:253`); a shard has at most one parent.

### Chunk production

`ChunkProducer::produce_chunk` (`chain/client/src/chunk_producer.rs:155`):

1. Confirm the local signer is the assigned chunk producer for `(prev_block, shard_id)` via `get_chunk_producer_info_db` (`:168`); return `None` otherwise.
2. `produce_chunk_internal` (`:253`): if the next block starts a new epoch and the prev block is not caught up, skip with an error (`:269`–`:282`).
3. Load the prev block's `ChunkExtra` for this shard (`:292`) — supplies the *prev* state root, outcome root, gas used/limit, balance burnt, validator proposals, congestion info, bandwidth requests, and `proposed_split` (`:402`).
4. Build the ordered transaction list from the sharded pool via `prepare_transactions` (`:462`), running runtime tx validation against the prev state; valid txs are reintroduced into the pool until included (`:567`–`:571`). A background early-preparation job (`start_prepare_transactions_job`, `:663`) may have cached the result (`get_cached_prepared_transactions`, `:298`).
5. Compute `tx_root` by merklizing the signed txs (`:336`) and fetch the shard's outgoing receipts produced since the prev chunk (`get_outgoing_receipts_for_shard_from_store`, `:348`); `calculate_receipts_root` (`:232`) groups receipts by receiver and merklizes the group hashes (doc `:222`).
6. `ShardChunkWithEncoding::new` (`core/primitives/src/sharding.rs:1427`) assembles `TransactionReceipt(signed_txs, prev_outgoing_receipts)`, Reed–Solomon-encodes it (next section, `:1451`), merklizes the parts into `encoded_merkle_root` + merkle paths (`:1455`), and signs a `ShardChunkHeaderV3` (`:1457`). The `proposed_split` from the prev `ChunkExtra` is copied verbatim into the new header (`chunk_producer.rs:402`) — the producer does **not** recompute it. (A separate spice-only path `new_for_spice` (`:1487`) builds a tx-only chunk; inactive at v86.)

### Reed–Solomon encoding into parts

`reed_solomon_encode` (`core/primitives/src/reed_solomon.rs:18`):

1. Borsh-serialize the `TransactionReceipt`; `encoded_length` = byte length.
2. `part_length = ceil(encoded_length / data_parts)` (`reed_solomon_part_length`, `:76`); pad bytes to `data_parts * part_length` with zeros (`:33`).
3. Split into `data_parts` data shards, append `parity_parts` empty slots, then `rs.reconstruct` fills the parity shards (`:35`–`:41`). The encoder is built once per `ChunkProducer` with `data_parts = num_data_parts` and `parity_parts = num_total_parts − num_data_parts` (`chunk_producer.rs:125`–`:146`). The data threshold `num_data_parts` is roughly 1/3 of total validator seats (module doc, `shards_manager_actor.rs`); the chunk is reconstructible from any `num_data_parts` parts.

`ReedSolomonEncoder::new` (`reed_solomon.rs:108`) handles the degenerate single-part case (`total_parts <= 1`) by storing no RS instance (`None` branch, `:113`) and treating the whole payload as one part (`encode` `:138`, `decode` `:153`).

### Part assignment & distribution

`distribute_encoded_chunk` (`chain/chunks/src/shards_manager_actor.rs:2150`):

1. For each `part_ord` in `0..num_total_parts`, ask the epoch manager `get_part_owner(epoch_id, part_ord)` and group part ords by owner account (`:2170`–`:2176`). Part ownership is an epoch-level assignment, not per-shard.
2. Add every **next-epoch** block producer to the recipient map (even with no parts) because they begin tracking shards a full epoch early and need the receipt proofs (`:2180`–`:2187`).
3. Build receipt proofs (`make_outgoing_receipts_proofs`, `logic.rs:76`): the outgoing receipts are grouped per target shard and merklized; each recipient gets only the proofs for shards it cares about this-or-next epoch (`:2197`–`:2209`).
4. Send a `PartialEncodedChunkMessage` (the recipient's part ords + relevant receipt proofs + header + merkle paths) to every recipient except self (`:2218`–`:2238`).
5. Merge the producer's own parts/receipts into `encoded_chunks` and mark the chunk for inclusion (`:2242`–`:2244`).

### Receiving, forwarding, reconstruction

A node obtains parts three ways (module doc, `shards_manager_actor.rs`): a `PartialEncodedChunkResponse` to its own request; a `PartialEncodedChunk` pushed by the producer to part owners; or a `PartialEncodedChunkForward` from a part owner to shard-trackers. The last two are validator→validator only.

- **Requesting** — `request_partial_encoded_chunk` (`:462`): picks a *shard-representative target* (the chunk producer, or a random shard-tracking block producer when requesting own parts / from archival / from self) (`:522`–`:528`). For each missing part: if the node tracks the shard (`request_full`, `:487`) or owns the part it requests it; own parts are fetched from the representative target, others from the part owner (`:532`–`:560`).
- **Forwarding** — on receiving an *owned* part, `send_partial_encoded_chunk_to_chunk_trackers` (`:2019`) forwards it to block producers of this-or-next epoch that care about the shard (`:2049`–`:2065`), reducing request fan-out. `need_part` (`logic.rs:27`) and `need_receipt` (`logic.rs:18`) decide what a node must retain.
- **Validation order** — preliminary header checks (shard_id, protocol_version) happen on receipt; full signature validation (`validate_chunk_header_full`) is deferred until the prev block is processed inside `try_process_chunk_parts_and_receipts` (`:1804`–`:1835`). Merkle proofs of parts/receipts are checked against the header roots before merging.
- **Reconstruction** — `try_process_chunk_parts_and_receipts` (`:1781`): once `entry.parts.len() >= num_data_parts` the chunk `can_reconstruct` (`:1846`). If the node does **not** track the shard but has the parts/receipts it owns, it completes the chunk without decoding (`:1881`–`:1894`). Otherwise it fills an `EncodedShardChunk::from_header` with the gathered parts (`:1900`–`:1908`), asserts ≥`num_data_parts` present (`:1910`), and decodes; `decode_chunk` (`sharding.rs:1388`) reconstructs parts up to `encoded_length` and borsh-deserializes the `TransactionReceipt`. The full `ShardChunk` is persisted only if the node tracks the shard (`:1920`–`:1924`). A decode failure poisons the cache entry, drops the request, and reports `DecodedChunk::Invalid` (`:1927`–`:1960`).

### Missing / skipped chunks

A chunk header's `height_included` is set to the block height that actually included it; `is_new_chunk(block_height)` is true iff `height_included == block_height` (`core/primitives/src/sharding.rs:460`). When a chunk producer fails to produce (or a chunk is not received in time), the block carries the **previous** chunk header for that shard with an unchanged `height_included`, i.e. no new chunk — the shard's state is not advanced that height. Block processing and runtime treat such a shard as applying an empty/implicit transition (`is_new_chunk: false`, `chain/chain/src/chain_update.rs:622`; see [chain & block processing](chain-block-processing.md), [runtime execution](runtime-execution.md)).

### Dynamic resharding (split at epoch boundary)

Gated by `ProtocolFeature::DynamicResharding` (active at v85+, see below). Active only when the epoch config's `shard_layout_config` is `Dynamic` (`chain/epoch-manager/src/lib.rs:1929`). Pipeline, two-epoch delayed:

1. **Propose (chunk application, epoch N near the end)** — `compute_proposed_split` (`chain/chain/src/runtime/mod.rs:581`): returns `None` unless the feature is on, a dynamic config exists, the next block *could* be the last in the epoch (`is_next_block_possibly_last_in_epoch`, `:599`), and the cooldown has elapsed (`can_reshard`, `:603`). Otherwise it runs `check_dynamic_resharding` (`:1729`) which applies, in priority order: max-shard-count veto, force-split list, block-split list, then `total_mem_usage`≥threshold with both children ≥ `min_child_memory_usage`; on success `find_trie_split` (`:1750`/`:1759`, from `core/store/src/trie/split.rs`, see [state-storage](state-storage.md)) finds the boundary account. The result is stored in `ChunkExtra.proposed_split` (`:296`,`:430`) and copied into the next chunk header's V5 inner `proposed_split`.
2. **Select (last block of epoch N)** — `get_upcoming_shard_split` (`chain/epoch-manager/src/lib.rs:1919`): returns `None` for a static layout (`:1927`–`:1929`) or failed cooldown; collects `proposed_split` from all chunk headers and `pick_shard_to_split` (`:1963`) chooses force-split shards first, else the highest `(total_memory, shard_id)`. The chosen `(ShardId, AccountId)` is embedded in the block header.
3. **Derive (epoch finalization)** — `next_next_shard_layout` (`lib.rs:679`): static fallback if the N+2 config is static; else if the current epoch has no dynamic config, carry the N+1 layout forward; else if `block_info.shard_split()` is set (`:706`), `next_shard_layout.derive_v3(boundary_account, …)` (`:739`) builds the new V3 layout (allocating two new shard ids), stored for epoch **N+2** (`scheduled_epoch_height = epoch_height + 2`, `:732`).
4. **Execute (boundary block, storage layer)** — `ReshardingManager::start_resharding` (`chain/chain/src/resharding/manager.rs:47`) fires when `is_next_block_epoch_start` and the layout actually changes (`:66`–`:75`); `ReshardingEventType::from_shard_layout` (`event_type.rs:48`) deduces the single `SplitShard` event (erroring on two simultaneous splits, `:79`); `split_shard` / `process_memtrie_resharding_storage_update` (`manager.rs:107`,`:162`) retain the parent trie into left/right children (`:200`–`:206`), derive child congestion info (`:229`,`:297`), and atomically commit the `ShardUId` mapping + child `ChunkExtra` + trie nodes. Memtrie preload and flat-storage split are in [state-storage](state-storage.md). Old and new shards coexist: epoch N+1 nodes still run the old layout while preparing the children; the new layout takes effect at the N+1→N+2 boundary.

### Validator assignment across resharding

`AssignmentStrategy::select` (`chain/epoch-manager/src/shard_assignment/mod.rs:170`): `CarryOver` when layouts are equal (`:175`); `Fresh` when `StickyReshardingValidatorAssignment` is off or sticky construction fails (`:178`,`:187`); else `StickyResharding` (`:181`), which preserves assignment by `ShardId` and distributes a split parent's producers across its children via greedy stake-balanced bin-packing. `needs_changes_limit_override` (`:196`) raises the per-epoch reassignment limit only for `StickyResharding` so freshly split children reach target population in one epoch. Without the feature a layout change reassigns every chunk producer by `ShardIndex`, forcing extra state sync.

## Interactions

- **Consumes** from [epoch management](epoch-validators-staking.md): chunk-producer assignment (`get_chunk_producer_info_db`), part owners (`get_part_owner`), shard layout (`get_shard_layout`), and the resharding select/derive hooks.
- **Consumes** the prev `ChunkExtra` (post-application summary) from [runtime execution](runtime-execution.md) / [state & storage](state-storage.md); transaction pool from the client.
- **Produces** chunk headers consumed by [chain & block processing](chain-block-processing.md) (block assembly, `height_included`) and reconstructed `ShardChunk`s applied by [runtime execution](runtime-execution.md).
- **Parts and receipt proofs** move over [networking / P2P](networking-p2p.md) as `PartialEncodedChunk{,Forward,Request,Response}`.
- **Resharding** triggers trie/flat-storage migration in [state & storage](state-storage.md); the produced layout feeds back into epoch management.
- Chunk *validation* (witnesses, `proposed_split` forgery checks) is in [stateless validation](stateless-validation.md); enforcement sites cited below.

## Protocol-version-gated behavior

Activation versions verified against `core/primitives-core/src/version.rs::protocol_version`. Stable = 86 (`STABLE_PROTOCOL_VERSION`, `:628`); min supported = 83 (`:600`).

| Feature | Activates | Effect |
|---------|-----------|--------|
| `DynamicResharding` | **85** (`version.rs:342`→arm `:564`) — **active at 86** | Enables the propose/select/derive split pipeline; gates `compute_proposed_split` (`runtime/mod.rs`) and the V5 chunk-header-inner `proposed_split`. Note: `docs/architecture/how/dynamic_resharding.md` is **stale**, citing v153 — actual is 85. |
| `StickyReshardingValidatorAssignment` | **85** (`version.rs:409`→arm `:565`) — **active at 86** | Switches chunk-producer stickiness from `ShardIndex` to `ShardId` and bin-packs a split parent's producers into its children (`shard_assignment/mod.rs:178`). Doc cites v153; actual is 85. |
| Shard-layout version bumps (`_DeprecatedSimpleNightshadeV*`) | deprecated, ≤78 | Historical static reshardings that produced V0→V1→V2 layouts. At v86 the genesis/static path uses V2; dynamic produces V3. |
| `ShuffleShardAssignments` | 143 (nightly, `version.rs:582`) | Shuffle chunk-producer→shard assignments every epoch. **Not active at v86.** |
| `EarlyKickout` | 152 (nightly, `version.rs:583`) | Pre-compute/persist chunk-producer assignments. **Not active at v86.** |

`Spice` (v180, `version.rs:586`) adds a tx-only chunk path (`ShardChunkWithEncoding::new_for_spice`, `sharding.rs:1487`; V6 inner) but is not active at v86. **2.13.0 note:** the PV-86 feature on this release is `EnforcePerReceiptStorageProofLimit` (`version.rs:449`, arm `:576`), which affects [stateless validation](stateless-validation.md), not this component; there is no `FixContractLoadingError` on this release (`FixContractLoadingCost` exists but is nightly-only at v129, `:579`).

## Invariants & failure modes

- **A shard has at most one parent** across a layout change; `from_shard_layout` errors `"can't perform two reshardings at the same time!"` if two shards each have two children (`event_type.rs:79`).
- **`proposed_split` cannot be forged**: `validate_chunk_with_chunk_extra_and_receipts_root` (`chain/chain/src/validate.rs:133`) compares `chunk_header.proposed_split()` against the locally recomputed `prev_chunk_extra.proposed_split()`, returning `Error::InvalidChunkHeaderShardSplit` on mismatch (`:178`).
- **`shard_split` cannot be forged**: `validate_block_shard_split` (`validate.rs:193`) recomputes the block header's split via `get_upcoming_shard_split` and returns `Error::InvalidBlockHeaderShardSplit` on mismatch (`:218`).
- **Reconstruction needs ≥`num_data_parts`**: enforced by an assertion in `try_process_chunk_parts_and_receipts` (`shards_manager_actor.rs:1910`); a chunk whose decoded body fails proof validation is rejected and the cache entry poisoned (`:1927`).
- **`encoded_length` bound**: `reed_solomon_decode` rejects an attacker-supplied length > `MAX_ENCODED_LENGTH` (512 MiB, `reed_solomon.rs:15`) with `"encoded length is too large"` (`:56`, guard `:55`).
- **Cooldown** (`min_epochs_between_resharding`, must be > 0): back-to-back reshardings are unsafe — a fresh child would inherit the parent's `proposed_split` while its own chunk computes `None`, tripping `InvalidChunkHeaderShardSplit`; enforced via `can_reshard` (`runtime/mod.rs:603`).
- **Memtrie required for split**: `process_memtrie_resharding_storage_update` errors `"Memtrie not loaded"` if the parent memtrie is absent and `allow_resharding_without_memtries` is false (`manager.rs:213`).
- **Right-child congestion invariant**: after subtracting buffered receipts, `buffered_receipts_gas() == 0` is asserted (`manager.rs:362`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives/src/shard_layout/mod.rs:203` | `ShardLayout::account_id_to_shard_id` | account → shard dispatch by layout version |
| `core/primitives/src/shard_layout/v2.rs:265` | `ShardLayoutV2::account_id_to_shard_id` | boundary-account `partition_point` mapping |
| `core/primitives/src/shard_layout/mod.rs:479` | `ShardUId` | layout-unique storage id `{version, shard_id}` |
| `core/primitives/src/shard_layout/mod.rs:288` | `ShardLayout::derive_v3` | derive V3 layout for a split |
| `chain/client/src/chunk_producer.rs:155` | `ChunkProducer::produce_chunk` | producer entry; assignment check |
| `chain/client/src/chunk_producer.rs:253` | `produce_chunk_internal` | builds chunk from prev `ChunkExtra` + txs + receipts |
| `core/primitives/src/sharding.rs:1427` | `ShardChunkWithEncoding::new` | assembles + encodes + signs the chunk |
| `core/primitives/src/reed_solomon.rs:18` | `reed_solomon_encode` | data/parity part encoding |
| `core/primitives/src/reed_solomon.rs:49` | `reed_solomon_decode` | length-bounded decode of parts |
| `core/primitives/src/sharding.rs:1218` | `EncodedShardChunkBody::get_merkle_hash_and_paths` | parts → encoded_merkle_root + paths |
| `chain/chunks/src/logic.rs:76` | `make_outgoing_receipts_proofs` | receipt proofs per target shard |
| `chain/chunks/src/logic.rs:27` | `need_part` | is this node the part owner |
| `chain/chunks/src/shards_manager_actor.rs:2150` | `distribute_encoded_chunk` | part-ord → owner grouping, push to recipients |
| `chain/chunks/src/shards_manager_actor.rs:462` | `request_partial_encoded_chunk` | target selection for missing parts/receipts |
| `chain/chunks/src/shards_manager_actor.rs:2019` | `send_partial_encoded_chunk_to_chunk_trackers` | forward owned parts to shard trackers |
| `chain/chunks/src/shards_manager_actor.rs:1781` | `try_process_chunk_parts_and_receipts` | validation, merge, reconstruction |
| `core/primitives/src/sharding.rs:1388` | `EncodedShardChunk::decode_chunk` | parts → `ShardChunk` |
| `core/primitives/src/sharding.rs:460` | `ShardChunkHeader::is_new_chunk` | skipped-chunk detection via `height_included` |
| `chain/chain/src/runtime/mod.rs:581` | `NightshadeRuntime::compute_proposed_split` | propose a split during chunk application |
| `chain/chain/src/runtime/mod.rs:1729` | `check_dynamic_resharding` | threshold/force/block decision logic |
| `chain/epoch-manager/src/lib.rs:1919` | `get_upcoming_shard_split` | collect proposals, pick winner |
| `chain/epoch-manager/src/lib.rs:1963` | `pick_shard_to_split` | force-list then max `(total_memory, shard_id)` |
| `chain/epoch-manager/src/lib.rs:679` | `next_next_shard_layout` | derive N+2 layout at finalization |
| `chain/chain/src/resharding/manager.rs:47` | `ReshardingManager::start_resharding` | fire split at boundary block |
| `chain/chain/src/resharding/event_type.rs:48` | `ReshardingEventType::from_shard_layout` | deduce single `SplitShard` event |
| `chain/chain/src/validate.rs:133` | `validate_chunk_with_chunk_extra_and_receipts_root` | `InvalidChunkHeaderShardSplit` enforcement (`:178`) |
| `chain/chain/src/validate.rs:193` | `validate_block_shard_split` | `InvalidBlockHeaderShardSplit` enforcement (`:218`) |
| `chain/epoch-manager/src/shard_assignment/mod.rs:170` | `AssignmentStrategy::select` | CarryOver / StickyResharding / Fresh |
| `core/primitives-core/src/version.rs:564` | `protocol_version` match arm | DynamicResharding & Sticky activate at 85 |

## Open questions

- The plan/doc framed `DynamicResharding` and `StickyReshardingValidatorAssignment` as v153 features; both are actually grouped at **v85** in `version.rs:564`–`565` and so are active at stable v86. The dynamic-resharding pipeline code is fully present at this commit, but whether any mainnet/testnet **epoch config** at v86 actually sets `shard_layout_config = Dynamic` (vs. `Static`) is a deployment/genesis question not answerable from this code alone — see [genesis & configuration](genesis-configuration.md).
- The exact `num_data_parts` / `num_total_parts` values are epoch-config-derived (validator seat counts) and not fixed constants; the "~1/3" figure comes from the module doc rather than a numeric constant in this scope.
