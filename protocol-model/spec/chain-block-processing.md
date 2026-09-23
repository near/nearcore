# Chain & block-processing pipeline

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `chain/chain/src/chain.rs`, `chain/chain/src/chain_update.rs`, `chain/chain/src/validate.rs`, `chain/chain/src/orphan.rs`, `chain/chain/src/missing_chunks.rs`, `chain/chain/src/block_processing_utils.rs`, `chain/chain/src/garbage_collection.rs`, `chain/chain/src/store/mod.rs`, `chain/client/src/client.rs`, `chain/client/src/client_actor.rs`

## Role

This component is the orchestration layer that drives a block from "received/produced" to "committed and head updated". It validates a block, schedules chunk application, persists the results atomically, chooses the canonical head on forks, holds blocks whose parent or chunks are missing, and garbage-collects old data. It sits downstream of [networking-p2p](networking-p2p.md) / [sync](sync.md) (which deliver blocks) and ties together [consensus-finality](consensus-finality.md) (approval/finality gating), [epoch-validators-staking](epoch-validators-staking.md) (producer/epoch info, validator proposals), [sharding-chunks](sharding-chunks.md) (chunk headers/bodies), [stateless-validation](stateless-validation.md) (chunk endorsements), and [runtime-execution](runtime-execution.md) (the actual chunk apply). It persists everything through [state-storage](state-storage.md). Block processing is **two-phase and asynchronous**: a synchronous `preprocess` validates and schedules, chunk apply runs on a thread pool, and a separate `postprocess` step commits and updates the head.

## Key data structures

- **`Chain`** — `chain/chain/src/chain.rs` — owns `chain_store: ChainStore`, `epoch_manager`, `runtime_adapter`, `shard_tracker`, `doomslug_threshold_mode`, the orphan pools (`orphans`, `blocks_with_missing_chunks`, `blocks_pending_execution`, `optimistic_block_chunks`), `blocks_in_processing`, and the `apply_chunks_sender`/`apply_chunks_receiver` channel that carries finished apply results back to postprocessing.
- **`MaybeValidated<Arc<Block>>`** — the block passed into the pipeline; `validate_block` lazily marks the body validated so repeated processing (e.g. after orphan resolution) does not re-run `validate_block_impl` (`chain.rs:750` — `Chain::validate_block`).
- **`Provenance`** — `chain/chain/src/types.rs:82` — `NONE`, `PRODUCED` (we built it — skip approval/finality re-checks), or `SYNC`. Gates which validations run (`chain.rs:950` — `validate_header`).
- **`BlockPreprocessInfo`** — `chain/chain/src/block_processing_utils.rs:19` — the output of `preprocess_block` carried to postprocessing: `is_caught_up`, `state_sync_info`, `incoming_receipts`, `provenance`, `apply_chunks_done_waiter`, `block_start_processing_time`, `sandbox_patch_generation`.
- **`BlockToApply`** — `core/primitives/src/optimistic_block.rs:193` — `Normal(CryptoHash)` or `Optimistic(BlockHeight)`; tags which kind of block a finished apply-job batch belongs to.
- **`BlocksInProcessing`** — `chain/chain/src/block_processing_utils.rs:55` — bounded set (`MAX_PROCESSING_BLOCKS = 5`, `block_processing_utils.rs:16`) of blocks currently between preprocess and postprocess; `add_dry_run` (`block_processing_utils.rs:153`) rejects with `Error::TooManyProcessingBlocks` once full (`block_processing_utils.rs:158`, `block_processing_utils.rs:70`).
- **`AcceptedBlock`** — `chain/chain/src/types.rs:105` — `{ hash, status: BlockStatus, provenance }`, returned to the client after postprocess.
- **`BlockStatus`** — `chain/chain/src/types.rs:60` — `Next` (extends head), `Fork` (head unchanged), `Reorg(old_hash)` (head switched away from a different prev). `is_new_head()` is true for `Next`/`Reorg` (`types.rs:71`).
- **`BlockProcessingArtifact`** — `chain/chain/src/block_processing_utils.rs:84` — accumulates side effects of a processing round: `orphans_missing_chunks`, `blocks_missing_chunks`, `invalid_chunks`, challenges.
- **`ChainUpdate`** — `chain/chain/src/chain_update.rs:43` — short-lived helper holding a `ChainStoreUpdate` plus `epoch_manager`/`runtime_adapter`. "If rejected nothing will be updated in underlying storage" (`chain_update.rs:41`) — all writes accumulate in one `StoreUpdate` and are committed atomically by `commit` (`chain_update.rs:95`).
- **`Orphan`** — `chain/chain/src/orphan.rs:44` — `{ block, provenance, added }`; `OrphanBlockPool` (`orphan.rs:73`) caps at `MAX_ORPHAN_SIZE = 1024` (`orphan.rs:20`) and evicts by age (`MAX_ORPHAN_AGE_SECS = 300`, `orphan.rs:23`).
- **`Tip`** — `core/primitives/src/block.rs` (`Tip::from_header`) — the persisted head pointer (`last_block_hash`, `prev_block_hash`, `height`, `epoch_id`). The chain keeps three tips: **header head**, **body/chain head**, and **final head**.

## Behavior

### End-to-end ordered phases

Driven by `ClientActor`: blocks are fed via `Client::start_process_block` (`chain/client/src/client.rs:1447`), which calls `Chain::start_process_block_async` (`client.rs:1456`); finished blocks are drained by `try_process_unfinished_blocks` → `Client::postprocess_ready_blocks` (`client.rs:1472`, `client_actor.rs:1438`).

#### Phase 0 — entry & hash/signature gate

`Chain::start_process_block_async` (`chain.rs:1228`) records the receive time, then `start_process_block_impl` (`chain.rs:1650`). Step 0 calls `verify_block_hash_and_signature` (`chain.rs:1046`); an `Incorrect` result returns `Error::InvalidSignature` immediately (`chain.rs:1671`). The block hash and the height are always recorded as "processed" even on failure, so the same unrequested block is not reprocessed (`chain.rs:1254` — `save_block_hash_processed`, `chain.rs:1255` — `save_block_height_processed`).

#### Phase 1 — synchronous preprocess (validation + scheduling)

`preprocess_block` (`chain.rs:2386`) is the validation gate. **No chain state is written here.** It runs these checks in order:

1. `blocks_in_processing.add_dry_run` — bounded concurrency, else `TooManyProcessingBlocks` (`chain.rs:2396`).
2. Epoch known? else `EpochOutOfBounds` — checked before the header lookup so an unknown-epoch block is not treated as an orphan (`chain.rs:2401`).
3. Number of chunk headers == shard count, else `IncorrectNumberOfChunkHeaders` (`chain.rs:2406`).
4. Not already known (`check_block_known`, `chain.rs:3736`), else `BlockKnown` (`chain.rs:2411`).
5. Height not absurdly far ahead: `> head.height + epoch_length * 20` → `InvalidBlockHeight` (`chain.rs:2425`); skipped under the `sandbox` feature.
6. **Orphan check:** if `prev_hash` is neither head nor a stored block, partially verify the header signature (`partial_verify_orphan_header_signature`) and the chunk-header/body consistency (`block.check_validity()`), then return `Error::Orphan` (`chain.rs:2430`). Full approval verification is deliberately *not* run on orphans (TODO at `chain.rs:2443`).
7. First DB I/O — `get_previous_header` (`chain.rs:2449`). Reject old forks: `prev_height < get_gc_stop_height(head)` → `InvalidBlockHeight` (`chain.rs:2456`).
8. Catch-up decision via `get_catchup_and_state_sync_infos` (`chain.rs:2460`, `chain.rs:2611`): if the prev block is the first of a new epoch but the block before it isn't caught up, the block is `Orphan`ed; otherwise computes `is_caught_up` and any `StateSyncInfo`.
9. **Header validation** — `validate_header` (`chain.rs:2466`, see below).
10. VRF: `verify_block_vrf` against the producer's key, then `random_value() == hash(vrf_value)` else `InvalidRandomnessBeaconOutput` (`chain.rs:2470`, `chain.rs:2473`).
11. **Block body validation** — `validate_block` → `validate_block_impl` (`chain.rs:2476`, `chain.rs:763`): per-chunk genesis/new-chunk checks, chunk-header signatures, and `block.check_validity()`. Failure fires `byzantine_assert!` before returning (`chain.rs:2477`).
12. `validate_block_shard_split` — header's shard-split field matches the expected resharding split for the last block of an epoch (`chain.rs:2481`, `validate.rs:193`).
13. Gas price (`verify_gas_price_checked`, `chain.rs:2502` → `InvalidGasPrice`) and total supply (`verify_total_supply_checked`, `chain.rs:2530` → `InvalidTotalSupply`); both fire `byzantine_assert!` on a `Some(false)` result.
14. `validate_chunk_headers` — new chunks point at `prev_hash`, old (missing) chunks equal the prev block's chunk header, plus validator/chunk-proposal consistency (`chain.rs:2546`, `chain.rs:1080`).
15. Chunk endorsements: `validate_chunk_endorsements_in_block` (non-SPICE only) — >2/3 stake endorses each chunk; see [stateless-validation](stateless-validation.md) (`chain.rs:2548`).
16. **Missing-chunks check** — `ping_missing_chunks` returns `Error::ChunksMissing(headers)` if a chunk we track is absent from the store (`chain.rs:2551`, `chain.rs:1113`).
17. Collect incoming receipts from chunks (non-SPICE; SPICE defers this) (`chain.rs:2554`).
18. `check_if_finalizable` — drop blocks that cannot reach the final head (`Error::CannotBeFinalized`), bounded to `NUM_PARENTS_TO_CHECK_FINALITY` parents (`chain.rs:2565`, `chain.rs:842`).
19. `apply_chunks_preprocessing` builds the list of per-shard `UpdateShardJob`s (`chain.rs:2580`, `chain.rs:3246`), choosing `ApplyChunksMode::IsCaughtUp` vs `NotCaughtUp`. Returns `(apply_chunk_work, BlockPreprocessInfo, apply_chunks_still_applying)`.

The order is load-bearing: cheap/local checks precede the first DB I/O (`get_previous_header`, `chain.rs:2449`), and the orphan/epoch decisions are made before expensive validation so a block is parked in the right pool.

#### Phase 1b — preprocess error routing

On error, `start_process_block_impl` parks the block instead of dropping it (`chain.rs:1682` — the `Err(e)` match arm):
- `Orphan` → `save_orphan` into `OrphanBlockPool` (only if `height >= tail`), optionally requesting its missing chunks (`chain.rs:1691`).
- `ChunksMissing` → `blocks_with_missing_chunks.add_block_with_missing_chunks` plus a `BlockMissingChunks` artifact so the client fetches them (`chain.rs:1710`).
- `BlockPendingOptimisticExecution` → `blocks_pending_execution` pool (`chain.rs:1731`).
- `EpochOutOfBounds` / `BlockKnown` → logged and dropped (`chain.rs:1743`, `chain.rs:1749`).

#### Phase 2 — schedule chunk apply (async)

After a successful preprocess, the block is inserted into `blocks_in_processing` (`chain.rs:1777`) and `schedule_apply_chunks` (`chain.rs:1794`) spawns each shard's job on `apply_chunks_spawner`. The last job to finish sends `(BlockToApply, results)` back on `apply_chunks_sender` and fires the optional `ApplyChunksDoneSender` so the client knows to call postprocess. The actual per-chunk apply (transactions, receipts, state changes) is [runtime-execution](runtime-execution.md).

#### Phase 3 — postprocess & commit

`postprocess_ready_blocks` (`chain.rs:1442`) drains `apply_chunks_receiver` and, for each `Normal` block, calls `postprocess_ready_block` (`chain.rs:1890`):
1. Remove the block from `blocks_in_processing` (panics if absent — invariant, `chain.rs:1899`).
2. Push any chunk whose apply failed with bad data into `invalid_chunks` (`chain.rs:1917`).
3. `postprocess_block_only` (`chain.rs:1853`) → `ChainUpdate::postprocess_block` (see below) — the atomic commit. Returns `Option<Tip>` (Some iff this became the new head, `chain.rs:1922`).
4. If a new head crossed a protocol-version boundary, notify the runtime `on_protocol_version_update` (`chain.rs:1945`).
5. Per-shard storage maintenance: `start_resharding` and advance flat-storage/memtrie head to the last final block (`chain.rs:1975`, `chain.rs:1988` — `update_flat_storage_and_memtrie`). At an epoch boundary, retain only memtries we care about this/next epoch (`chain.rs:1994` — `retain_memtries`).
6. `check_orphans` — newly accepted block may unlock orphans (`chain.rs:2062`, `orphan.rs:390`).
7. `determine_status` computes `Next`/`Fork`/`Reorg` (`chain.rs:1568`) and returns an `AcceptedBlock` (`chain.rs:2069`).

### `ChainUpdate::postprocess_block` — the atomic commit (`chain_update.rs:225`)

All of the following accumulate in a single `ChainStoreUpdate` and are committed together by `ChainStoreUpdate::commit` → `finalize` → `StoreUpdate::commit` (`chain_update.rs:95`, `store/mod.rs:2273`, `store/mod.rs:1916`):
1. `apply_chunk_postprocessing` — persist each shard's `ShardUpdateResult` (new-chunk: chunk extra, trie changes, flat-state changes, optional state-transition data for witnesses; old-chunk: carry forward chunk extra with new state root) (`chain_update.rs:239`, `chain_update.rs:99`, `chain_update.rs:114`).
2. If not caught up, register the block for catch-up (`add_block_to_catchup`); save incoming receipts and any `StateSyncInfo` (`chain_update.rs:247`, `chain_update.rs:249`, `chain_update.rs:252`).
3. `save_block_header` + `update_header_head` (`chain_update.rs:256`, `chain_update.rs:257`).
4. `add_validator_proposals` to the epoch manager and merge its store update; `save_chunk_producers_for_header` (`chain_update.rs:272`, `chain_update.rs:275`). See [epoch-validators-staking](epoch-validators-staking.md).
5. If `ContinuousEpochSync` is enabled and this is the first block of an epoch, `update_epoch_sync_proof` keyed off `prev_hash` (`chain_update.rs:281`, `chain_update.rs:291`).
6. `save_block` (stores the body even on a non-canonical fork) and `inc_block_refcount(prev_hash)` (`chain_update.rs:301`, `chain_update.rs:302`).
7. `update_head` (`chain_update.rs:320`, fork choice below).
8. If this became head and it is an epoch's first block, save the previous epoch's light-client block (`chain_update.rs:322`).

### Fork choice / head update (`chain_update.rs:431` — `update_head`)

NEAR's fork-choice rule is **highest height wins**. `update_head` first calls `update_final_head_from_block` (advances the persisted **final head** to the header's `last_final_block()` if higher, `chain_update.rs:412`), then, iff `header.height() > current head.height`, saves the new **body head** via `save_body_head` (`chain_update.rs:439`). Because epoch boundaries and fork choice are both by height, the first block to cross an epoch end is guaranteed to become head (comment at `chain_update.rs:329`). A **reorg** is detected post-hoc in `determine_status` (`chain.rs:1568`): if the new head's `prev_block_hash` is not the old head's `last_block_hash`, it is `Reorg(old_head_hash)` (`chain.rs:1573`). There is no explicit revert step — the canonical chain is whatever the per-height index points to from the (now higher) head; orphaned side-fork blocks remain in the store until GC prunes them.

### Orphan & missing-chunk handling

- **Orphans** (unknown parent): held in `OrphanBlockPool`. When a block is accepted, `check_orphans(prev_hash, …)` (`orphan.rs:390`) re-submits orphans whose `prev_hash` just arrived via `remove_by_prev_hash` (`orphan.rs:198`), and (within `NUM_ORPHAN_ANCESTORS_CHECK = 3` depth, `orphan.rs:31`) requests missing chunks for near-descendant orphans, bounded by `MAX_ORPHAN_MISSING_CHUNKS = 5` (`orphan.rs:38`).
- **Blocks with missing chunks**: held in `MissingChunksPool` (`missing_chunks.rs`); `check_blocks_with_missing_chunks` (`chain.rs:2650`) re-submits them once their chunks are available (driven by the client when chunks arrive).
- **Optimistic blocks**: an `OptimisticBlock` lets chunk apply start before the full block arrives. `preprocess_optimistic_block` validates and queues it (`chain.rs:1263`); `process_optimistic_block` schedules apply jobs (`chain.rs:1333`); results are cached and reused by the normal pipeline. Disabled when SPICE is enabled. See `docs/architecture/how/optimistic_block.md`.

### Header-only validation (`chain.rs:870` — `validate_header`)

Used by both `preprocess_block` and the header-first / sync path (`process_block_header`, `chain.rs:1031`; `sync_block_headers`, `chain.rs:1491`). Checks, in order: no challenges present (`chain.rs:871`); timestamp not too far in the future (`chain.rs:876`); header signature (`chain.rs:882`); `epoch_id`/`next_epoch_id` match those derived from prev (`chain.rs:892`, `chain.rs:897`); header `latest_protocol_version` ≥ epoch protocol version (`chain.rs:905`); SPICE header consistency (`chain.rs:913`); `next_bp_hash` (`chain.rs:918`); chunk-mask length and `verify_chunks_included` (`chain.rs:929`, `chain.rs:933`); `prev_height` matches prev header (`chain.rs:938`); strictly increasing raw timestamp (`chain.rs:945`). For non-`PRODUCED` blocks (`chain.rs:950`) it additionally verifies aggregated **approvals** and the Doomslug 2/3 stake threshold (`chain.rs:953`, `chain.rs:969`), the derived `last_ds_final_block`/`last_final_block` (`chain.rs:991` — see [consensus-finality](consensus-finality.md)), the block Merkle root (`chain.rs:1000`), and — when `ValidateBlockOrdinalAndEpochSyncDataHash` is enabled for the block's `epoch_protocol_version` — the block ordinal (`InvalidBlockOrdinal`) and `epoch_sync_data_hash` (`InvalidEpochSyncDataHash`) (`chain.rs:1004`).

### Garbage collection (`garbage_collection.rs`)

`Chain::clear_data` (`garbage_collection.rs:48`) → `ChainStore::clear_data` (`garbage_collection.rs:128`), driven periodically by the client. State-transition data and chunk witnesses are cleared first (they accumulate fast), then `clear_old_blocks_data` (`garbage_collection.rs:153`) GCs blocks from the **tail** up to `gc_stop_height = get_gc_stop_height(head)` (the start of the epoch `gc_num_epochs_to_keep` epochs back), bounded by `gc_config.gc_blocks_limit` per run:
- **Forks clearing** (`clear_forks_data`, `garbage_collection.rs:445`): for each height from tail to gc-stop, delete any fork that terminates at that height or earlier, up to (excluding) the ancestor where the fork branched.
- **Canonical-chain clearing** (`clear_block_data`, `garbage_collection.rs:630`): delete canonical blocks from tail to gc-stop exclusive, advancing the tail.
- After canonical clearing of an epoch's last block, `gc_state` (`garbage_collection.rs:1117`) drops trie state for shards we no longer track and won't track next epoch.

The full GC contract (genesis kept, tail always on canonical chain, one tail, fork-protection above gc-stop) is documented inline (`garbage_collection.rs:57`) and in `docs/architecture/how/gc.md`. **Archival** nodes skip block deletion; `clear_archive_data` (`garbage_collection.rs:424`) only trims the height-indexed columns the archive does not need, keeping all block/chunk/state data.

## Interactions

- **Consumes**: blocks/headers from [networking-p2p](networking-p2p.md) and [sync](sync.md); producer/epoch info, validator proposals, shard layout, and `gc_stop_height` from [epoch-validators-staking](epoch-validators-staking.md); approvals + finality fields from [consensus-finality](consensus-finality.md); chunk headers/bodies and incoming receipts from [sharding-chunks](sharding-chunks.md); chunk endorsements from [stateless-validation](stateless-validation.md); per-shard apply results from [runtime-execution](runtime-execution.md).
- **Produces**: the persisted header/body/final heads; stored blocks, chunk extras, trie/flat-state changes, incoming receipts, validator proposals; `AcceptedBlock` events to the client; catch-up and state-sync registrations consumed by [sync](sync.md); state snapshots.
- **Persists** everything through [state-storage](state-storage.md) via `ChainStoreUpdate` (atomic `StoreUpdate`).

## Protocol-version-gated behavior

- **`ValidateBlockOrdinalAndEpochSyncDataHash`** — declared at `core/primitives-core/src/version.rs:441`, activates at **version 85** (voted arm at `version.rs:569`). Before 85, `validate_header` does not check `block_ordinal` or `epoch_sync_data_hash`; from 85 on it rejects a mismatched block ordinal (`Error::InvalidBlockOrdinal`) or epoch-sync-data hash (`Error::InvalidEpochSyncDataHash`) (`chain.rs:1004`). The gate is on the block's `epoch_protocol_version` (`chain.rs:1004`). On at v86.
- **`ContinuousEpochSync`** — declared at `version.rs:356`, activates at **version 85** (voted arm at `version.rs:563`). When enabled, `postprocess_block` updates the epoch-sync proof at each epoch's first block (`chain_update.rs:281`). Note the gate is `enabled(PROTOCOL_VERSION)` (binary-wide, not the block's epoch version) (`chain_update.rs:281`). On at v86.
- **Optimistic block production** — the feature enum is `_DeprecatedProduceOptimisticBlock`, activated at **version 77** (`version.rs:540`) and always on at v86. Enables the optimistic-block path (`preprocess_optimistic_block`, `chain.rs:1263`); does not change finality. See [consensus-finality](consensus-finality.md).
- **`Spice`** — declared at `version.rs:355`, activation version **180** (voted arm at `version.rs:586`), **not enabled** in the stable v86 build (`STABLE_PROTOCOL_VERSION = 86`, `version.rs:628`). When enabled it changes preprocess substantially: chunk-endorsement-in-block validation is skipped (`chain.rs:2548`), incoming-receipt collection is deferred (`chain.rs:2554`), certified-execution-result gathering (`chain.rs:2490`) and core-statement validation are added (`chain.rs:2569`), the header must satisfy `is_spice()` (`chain.rs:912`), and `postprocess_block` records uncertified chunks / spice endorsement stats (`chain_update.rs:307`).
- **`EnforcePerReceiptStorageProofLimit`** — declared at `version.rs:449`, activates at **version 86** (`version.rs:576`); this is the PV-86 feature on the 2.13.0 release. It is a [runtime-execution](runtime-execution.md) storage-proof-limit change, **not** a chain-pipeline change. (On this release there is no `FixContractLoadingError`; the only contract-loading feature is `FixContractLoadingCost`, which is nightly at v129, `version.rs:579`.)

The Doomslug finality rule itself is **not** version-gated at v86; see [consensus-finality](consensus-finality.md).

## Invariants & failure modes

- **Atomic commit.** A block either commits all of its writes via one `StoreUpdate::commit` (`store/mod.rs:2273`) or none — "if rejected nothing will be updated in underlying storage" (`chain_update.rs:41`). A crash mid-processing leaves the store consistent.
- **Bounded in-flight blocks.** At most `MAX_PROCESSING_BLOCKS = 5` blocks may be between preprocess and postprocess; excess → `TooManyProcessingBlocks` (`block_processing_utils.rs:16`, `block_processing_utils.rs:70`, `chain.rs:2396`).
- **Postprocess pool invariant.** A block reaching `postprocess_ready_block` must be in `blocks_in_processing`, else panic (`chain.rs:1899`).
- **No state below tail.** Blocks with `prev_height < gc_stop_height` are rejected as old forks (`chain.rs:2456`); orphans below tail are not stored (`chain.rs:1695`).
- **Highest-height fork choice.** Head advances only when `header.height() > head.height` (`chain_update.rs:436`); equal-height forks never overtake the head.
- **Finalizability.** Blocks that cannot be reached from the final head are dropped with `CannotBeFinalized` (`chain.rs:842`).
- **Byzantine assertions.** Block-body, gas-price, and total-supply failures fire `byzantine_assert!` (debug-fatal) before returning the corresponding `Error` (`chain.rs:2477`, `chain.rs:2508`, `chain.rs:2536`); chunk-signature/body failures likewise in `validate_block_impl` (`chain.rs:814`).
- **Client protocol-version guard.** If, on an epoch boundary, the network's protocol version exceeds the binary's `PROTOCOL_VERSION`, `check_protocol_version` panics rather than producing invalid blocks (`chain_update.rs:70`).
- **GC contract.** Genesis is never GC'd; the tail is always on the canonical chain and only one tail exists; the oldest fork branch point is never affected (`garbage_collection.rs:57`).
- **Block-known dedup.** Every attempted block hash/height is recorded as processed even on failure, so unrequested duplicates are filtered (`chain.rs:1254`, `chain.rs:1255`).

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `chain/chain/src/chain.rs:1228` | `Chain::start_process_block_async` | pipeline entry; records processed hash/height |
| `chain/chain/src/chain.rs:1650` | `start_process_block_impl` | hash/sig gate, preprocess, error routing, schedule apply |
| `chain/chain/src/chain.rs:1046` | `verify_block_hash_and_signature` | body-hash + header-signature gate |
| `chain/chain/src/chain.rs:2386` | `preprocess_block` | full validation gate, builds apply jobs (no writes) |
| `chain/chain/src/chain.rs:2611` | `get_catchup_and_state_sync_infos` | catch-up / orphan-at-epoch-boundary decision |
| `chain/chain/src/chain.rs:870` | `validate_header` | header validity, approvals, finality fields, ordinal/epoch-sync hash |
| `chain/chain/src/chain.rs:763` | `validate_block_impl` | per-chunk + body validation |
| `chain/chain/src/chain.rs:1080` | `validate_chunk_headers` | new vs old chunk header consistency |
| `chain/chain/src/chain.rs:1113` | `ping_missing_chunks` | detect locally-missing chunks → `ChunksMissing` |
| `chain/chain/src/chain.rs:842` | `check_if_finalizable` | drop non-finalizable blocks |
| `chain/chain/src/chain.rs:1794` | `schedule_apply_chunks` | spawn per-shard apply jobs, send results back |
| `chain/chain/src/chain.rs:1442` | `postprocess_ready_blocks` | drain finished applies |
| `chain/chain/src/chain.rs:1890` | `postprocess_ready_block` | commit, storage maintenance, orphan check, status |
| `chain/chain/src/chain.rs:1853` | `postprocess_block_only` | wraps `ChainUpdate::postprocess_block` + commit |
| `chain/chain/src/chain.rs:1568` | `determine_status` | Next / Fork / Reorg classification |
| `chain/chain/src/chain.rs:1263` | `preprocess_optimistic_block` | optimistic-block path |
| `chain/chain/src/chain.rs:3736` | `check_block_known` | dedup against processed/stored blocks |
| `chain/chain/src/chain_update.rs:225` | `ChainUpdate::postprocess_block` | atomic write of all block results |
| `chain/chain/src/chain_update.rs:114` | `process_apply_chunk_result` | persist chunk extra/trie/flat-state |
| `chain/chain/src/chain_update.rs:431` | `update_head` | highest-height fork choice, body head |
| `chain/chain/src/chain_update.rs:412` | `update_final_head_from_block` | advance persisted final head |
| `chain/chain/src/chain_update.rs:395` | `update_header_head` | advance header head |
| `chain/chain/src/chain_update.rs:95` | `ChainUpdate::commit` | commit the accumulated StoreUpdate |
| `chain/chain/src/chain_update.rs:70` | `check_protocol_version` | panic if network PV > binary PV |
| `chain/chain/src/validate.rs:193` | `validate_block_shard_split` | resharding split header check |
| `chain/chain/src/orphan.rs:390` | `Chain::check_orphans` | unlock orphans whose parent arrived |
| `chain/chain/src/orphan.rs:73` | `OrphanBlockPool` | bounded orphan storage + eviction |
| `chain/chain/src/missing_chunks.rs` | `MissingChunksPool` | blocks awaiting chunk bodies |
| `chain/chain/src/garbage_collection.rs:128` | `ChainStore::clear_data` | GC entry; witnesses then old blocks |
| `chain/chain/src/garbage_collection.rs:445` | `clear_forks_data` | delete fork branches up to gc-stop |
| `chain/chain/src/garbage_collection.rs:630` | `clear_block_data` | canonical-chain clearing, advance tail |
| `chain/chain/src/garbage_collection.rs:424` | `clear_archive_data` | archival-node trimming (keep block/state) |
| `chain/chain/src/store/mod.rs:2273` | `ChainStoreUpdate::commit` | finalize → StoreUpdate::commit (atomicity) |
| `chain/client/src/client.rs:1447` | `Client::start_process_block` | client-side entry into the pipeline |
| `chain/client/src/client_actor.rs:1438` | `try_process_unfinished_blocks` | drives postprocess loop |
| `core/primitives-core/src/version.rs:441` | `ValidateBlockOrdinalAndEpochSyncDataHash` | declared; activates v85 (`version.rs:569`) |
| `core/primitives-core/src/version.rs:356` | `ContinuousEpochSync` | declared; activates v85 (`version.rs:563`) |
| `core/primitives-core/src/version.rs:449` | `EnforcePerReceiptStorageProofLimit` | declared; activates v86 (`version.rs:576`) — runtime change |
| `core/primitives-core/src/version.rs:355` | `Spice` | declared; activation v180 (disabled at v86) |

## Open questions

- A reorg performs no explicit "revert" of the previously-canonical state; canonicity is implicitly re-derived from the per-height index off the new (higher) head, and stale side-fork blocks linger until GC. The exact set of per-height / `NextBlockHash` index updates that re-point the canonical chain on a reorg lives in `ChainStoreUpdate::finalize` (`store/mod.rs:1916`) and was not traced line-by-line here; a verifier needing the precise reorg index mechanics should confirm in that finalize path.
