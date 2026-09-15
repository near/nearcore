# Chain & block-processing pipeline

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `chain/chain/src/chain.rs`, `chain/chain/src/chain_update.rs`, `chain/chain/src/validate.rs`, `chain/chain/src/orphan.rs`, `chain/chain/src/missing_chunks.rs`, `chain/chain/src/block_processing_utils.rs`, `chain/chain/src/blocks_delay_tracker.rs`, `chain/chain/src/approval_verification.rs`, `chain/chain/src/garbage_collection.rs`, `chain/chain/src/store/mod.rs`, `chain/client/src/client.rs`, `chain/client/src/client_actor.rs`, `chain/client/src/gc_actor.rs`

## Role

This component is the orchestration layer that drives a block from "received/produced" to "committed and head updated". It validates a block, schedules chunk application, persists the results atomically, chooses the canonical head on forks, holds blocks whose parent or chunks are missing, and garbage-collects old data. It sits downstream of [networking-p2p](networking-p2p.md) / [sync](sync.md) (which deliver blocks) and ties together [consensus-finality](consensus-finality.md) (approval/finality gating), [epoch-validators-staking](epoch-validators-staking.md) (producer/epoch info, validator proposals), [sharding-chunks](sharding-chunks.md) (chunk headers/bodies), [stateless-validation](stateless-validation.md) (chunk endorsements), and [runtime-execution](runtime-execution.md) (the actual chunk apply). It persists everything through [state-storage](state-storage.md). Block processing is **two-phase and asynchronous**: a synchronous `preprocess` validates and schedules, chunk apply runs on a thread pool, and a separate `postprocess` step commits and updates the head.

## Key data structures

- **`Chain`** — `chain/chain/src/chain.rs:273` — owns `chain_store: ChainStore`, `epoch_manager`, `runtime_adapter`, `shard_tracker`, `doomslug_threshold_mode`, the orphan pools (`orphans`, `blocks_with_missing_chunks`, `blocks_pending_execution`, `optimistic_block_chunks`), `blocks_in_processing`, `blocks_delay_tracker`, `spice_core_reader`, and the `apply_chunks_sender`/`apply_chunks_receiver` channel that carries finished apply results back to postprocessing.
- **`MaybeValidated<Arc<Block>>`** — the block passed into the pipeline; `validate_block` lazily marks the body validated so repeated processing (e.g. after orphan resolution) does not re-run `validate_block_impl` (`chain/chain/src/chain.rs:765` — `Chain::validate_block`).
- **`Provenance`** — `chain/chain/src/types.rs:82` — `NONE`, `PRODUCED` (we built it — skip approval/finality re-checks), or `SYNC`. Gates which validations run (`chain/chain/src/chain.rs:973` — `validate_header`).
- **`BlockPreprocessInfo`** — `chain/chain/src/block_processing_utils.rs:19` — the output of `preprocess_block` carried to postprocessing: `is_caught_up`, `state_sync_info`, `incoming_receipts`, `provenance`, `apply_chunks_done_waiter`, `block_start_processing_time`, `sandbox_patch_generation`.
- **`BlockToApply`** — `core/primitives/src/optimistic_block.rs:193` — `Normal(CryptoHash)` or `Optimistic(BlockHeight)`; tags which kind of block a finished apply-job batch belongs to.
- **`BlocksInProcessing`** — `chain/chain/src/block_processing_utils.rs:55` — bounded set (`MAX_PROCESSING_BLOCKS = 5`, `block_processing_utils.rs:16`) of blocks currently between preprocess and postprocess; `add_dry_run` (`block_processing_utils.rs:153`) rejects with `Error::TooManyProcessingBlocks` once full (`block_processing_utils.rs:158`, `block_processing_utils.rs:70`).
- **`AcceptedBlock`** — `chain/chain/src/types.rs:105` — `{ hash, status: BlockStatus, provenance }`, returned to the client after postprocess.
- **`BlockStatus`** — `chain/chain/src/types.rs:60` — `Next` (extends head), `Fork` (head unchanged), `Reorg(old_hash)` (head switched away from a different prev). `is_new_head()` is true for `Next`/`Reorg` (`types.rs:71`).
- **`BlockProcessingArtifact`** — `chain/chain/src/block_processing_utils.rs:84` — accumulates side effects of a processing round: `orphans_missing_chunks`, `blocks_missing_chunks`, `invalid_chunks`, challenges.
- **`ChainUpdate`** — `chain/chain/src/chain_update.rs:43` — short-lived helper holding a `ChainStoreUpdate` plus `epoch_manager`/`runtime_adapter`. "If rejected nothing will be updated in underlying storage" (`chain_update.rs:41`) — all writes accumulate in one `StoreUpdate` and are committed atomically by `commit` (`chain_update.rs:92`). Since 2.14 it no longer carries `doomslug_threshold_mode`: the orphan-approval helper that used it moved onto `Chain` (`chain_update.rs:43`, `chain.rs:4192`).
- **`Orphan`** — `chain/chain/src/orphan.rs:44` — `{ block, provenance, added }`; `OrphanBlockPool` (`orphan.rs:73`) caps at `MAX_ORPHAN_SIZE = 1024` (`orphan.rs:20`) and evicts by age (`MAX_ORPHAN_AGE_SECS = 300`, `orphan.rs:23`).
- **`BlocksDelayTracker`** — `chain/chain/src/blocks_delay_tracker.rs:31` — monitoring-only bookkeeping of block/chunk timestamps. Since 2.14 it is **bounded**: it tracks a window of heights `[head - BLOCK_DELAY_TRACKING_COUNT, head + BLOCK_HORIZON)` and at most `MAX_TRACKED_BLOCKS_PER_HEIGHT = 8` blocks per height (`blocks_delay_tracker.rs:20`, `blocks_delay_tracker.rs:24`, `blocks_delay_tracker.rs:198`).
- **`Tip`** — `core/primitives/src/block.rs:887` (`Tip::from_header`) — the persisted head pointer (`last_block_hash`, `prev_block_hash`, `height`, `epoch_id`). The chain keeps three tips: **header head**, **body/chain head**, and **final head**.

## Behavior

### End-to-end ordered phases

Driven by `ClientActor`: blocks are fed via `Client::start_process_block` (`chain/client/src/client.rs:1475`), which calls `Chain::start_process_block_async`; finished blocks are drained by `try_process_unfinished_blocks` → `Client::postprocess_ready_blocks` (`chain/client/src/client.rs:1500`, `chain/client/src/client_actor.rs:1485`).

#### Phase 0 — entry & hash/signature gate

`Chain::start_process_block_async` (`chain.rs:1256`) records the receive time, then `start_process_block_impl` (`chain.rs:1678`). Step 0 calls `verify_block_hash_and_signature` (`chain.rs:1071`); an `Incorrect` result returns `Error::InvalidSignature` immediately (`chain.rs:1696`). The block hash and the height are always recorded as "processed" even on failure, so the same unrequested block is not reprocessed (`chain.rs:1282` — `save_block_hash_processed`, `chain.rs:1283` — `save_block_height_processed`).

#### Phase 1 — synchronous preprocess (validation + scheduling)

`preprocess_block` (`chain.rs:2410`) is the validation gate. **No chain state is written here.** It runs these checks in order:

1. `blocks_in_processing.add_dry_run` — bounded concurrency, else `TooManyProcessingBlocks` (`chain.rs:2420`).
2. Epoch known? else `EpochOutOfBounds` — checked before the header lookup so an unknown-epoch block is not treated as an orphan (`chain.rs:2426`).
3. Number of chunk headers == shard count, else `IncorrectNumberOfChunkHeaders` (`chain.rs:2431`).
4. Not already known (`check_block_known`, `chain.rs:3907`), else `BlockKnown` (`chain.rs:2435`).
5. Height not absurdly far ahead: `> head.height + epoch_length * 20` → `InvalidBlockHeight` (`chain.rs:2450`); skipped under the `sandbox` feature.
6. **Orphan check:** if `prev_hash` is neither head nor a stored block, partially verify the header signature (`partial_verify_orphan_header_signature`, `chain.rs:2463`) and the chunk-header/body consistency (`block.check_validity()`), then return `Error::Orphan`. Full approval verification is deliberately *not* run on orphans (TODO at `chain.rs:2468`).
7. First DB I/O — `get_previous_header` (`chain.rs:2473`). Reject old forks: `prev_height < get_gc_stop_height(head)` → `InvalidBlockHeight` (`chain.rs:2481`).
8. Catch-up decision via `get_catchup_and_state_sync_infos` (`chain.rs:2486`, `chain.rs:2644`): if the prev block is the first of a new epoch but the block before it isn't caught up, the block is `Orphan`ed; otherwise computes `is_caught_up` and any `StateSyncInfo`.
9. **Header validation** — `validate_header` (`chain.rs:2491`, see below).
10. VRF: `verify_block_vrf` against the producer's key, then `random_value() == hash(vrf_value)` else `InvalidRandomnessBeaconOutput` (`chain.rs:2495`, `chain.rs:2498`).
11. **Block body validation** — `validate_block` → `validate_block_impl` (`chain.rs:2501`, `chain.rs:778`): per-chunk genesis/new-chunk checks, chunk-header signatures, and `block.check_validity()`. Failure fires `byzantine_assert!` before returning (`chain.rs:2502`).
12. `validate_block_shard_split` — header's shard-split field matches the expected resharding split for the last block of an epoch (`chain.rs:2506`, `validate.rs:194`). The expected value is `None` unless the block is the last of its epoch *and* the epoch config selects a dynamic shard layout; the gate is the epoch config's `ShardLayoutConfig`, not a `ProtocolFeature` (`chain/epoch-manager/src/lib.rs:2629`). See [epoch-validators-staking](epoch-validators-staking.md).
13. (SPICE only) gather newly certified block execution results from the prev block and the block's core statements (`chain.rs:2518`); they feed the gas-price and total-supply checks below.
14. Gas price (`verify_gas_price_checked`, `chain.rs:2527`) and total supply (`verify_total_supply_checked` / `..._spice`, `chain.rs:2551`). Each is a tri-state: `Some(false)` fires `byzantine_assert!` and returns `InvalidGasPrice` / `InvalidTotalSupply`; **`None` (arithmetic overflow) returns `Error::Other("arithmetic overflow when checking gas price"/"…total supply")` rather than panicking** (`chain.rs:2540`, `chain.rs:2563`).
15. `validate_chunk_headers` — new chunks point at `prev_hash`, old (missing) chunks equal the prev block's chunk header, plus `validate_block_proposals` (`chain.rs:2570`, `chain.rs:1105`).
16. Chunk endorsements: `validate_chunk_endorsements_in_block` (non-SPICE only) — >2/3 stake endorses each chunk; see [stateless-validation](stateless-validation.md) (`chain.rs:2573`).
17. **Missing-chunks check** — `ping_missing_chunks` returns `Error::ChunksMissing(headers)` if a chunk we track is absent from the store (`chain.rs:2576`, `chain.rs:1138`).
18. Collect incoming receipts from chunks (non-SPICE; SPICE defers this) (`chain.rs:2582`).
19. `check_if_finalizable` — drop blocks that cannot reach the final head (`Error::CannotBeFinalized`), bounded to `NUM_PARENTS_TO_CHECK_FINALITY = 20` parents (`chain.rs:2590`, `chain.rs:857`, `chain.rs:129`).
20. SPICE-ness cross-check and SPICE core-statement validation (`chain.rs:2593`–`chain.rs:2610`, nightly only — see *Protocol-version-gated behavior*).
21. `apply_chunks_preprocessing` builds the list of per-shard `UpdateShardJob`s (`chain.rs:2613`, `chain.rs:3401`), choosing `ApplyChunksMode::IsCaughtUp` vs `NotCaughtUp`. Returns `(apply_chunk_work, BlockPreprocessInfo, apply_chunks_still_applying)`.

The order is load-bearing: cheap/local checks precede the first DB I/O (`get_previous_header`, `chain.rs:2473`), and the orphan/epoch decisions are made before expensive validation so a block is parked in the right pool.

#### Phase 1b — preprocess error routing

On error, `start_process_block_impl` parks the block instead of dropping it (`chain.rs:1716` — the `Err(e)` match arm):
- `Orphan` → `save_orphan` into `OrphanBlockPool` (only if `height >= tail`), optionally requesting its missing chunks (`chain.rs:1720`, `orphan.rs:285`).
- `ChunksMissing` → `blocks_with_missing_chunks.add_block_with_missing_chunks` plus a `BlockMissingChunks` artifact so the client fetches them (`chain.rs:1739`).
- `BlockPendingOptimisticExecution` → `blocks_pending_execution` pool (`chain.rs:1761`).
- `EpochOutOfBounds` / `BlockKnown` → logged and dropped (`chain.rs:1772`, `chain.rs:1777`).

#### Phase 2 — schedule chunk apply (async)

After a successful preprocess, the block is inserted into `blocks_in_processing` (`chain.rs:1805`) and `schedule_apply_chunks` (`chain.rs:1822`) spawns each shard's job on `apply_chunks_spawner`. The last job to finish sends `(BlockToApply, results)` back on `apply_chunks_sender` and fires the optional `ApplyChunksDoneSender` so the client knows to call postprocess. The actual per-chunk apply (transactions, receipts, state changes) is [runtime-execution](runtime-execution.md).

#### Phase 3 — postprocess & commit

`postprocess_ready_blocks` (`chain.rs:1470`) drains `apply_chunks_receiver` and, for each `Normal` block, calls `postprocess_ready_block` (`chain.rs:1918`):
1. Remove the block from `blocks_in_processing` (panics if absent — invariant, `chain.rs:1928`).
2. Push any chunk whose apply failed with bad data into `invalid_chunks` (`chain.rs:1946`).
3. `postprocess_block_only` (`chain.rs:1881`) → `ChainUpdate::postprocess_block` (see below) — the atomic commit. Returns `Option<Tip>` (Some iff this became the new head, `chain.rs:1950`).
4. `update_optimistic_blocks_pool` and finalize any completed background memtrie loads (`chain.rs:1963`, `chain.rs:1967`).
5. If a new head crossed a protocol-version boundary, notify the runtime `on_protocol_version_update` (`chain.rs:1974`).
6. Per-shard storage maintenance: `start_resharding` and advance flat-storage/memtrie head to the last final block (`chain.rs:2003`, `chain.rs:2017` — `update_flat_storage_and_memtrie`). At an epoch boundary, start the resharding memtrie preload and retain only memtries we care about this/next epoch (`chain.rs:2022`, `chain.rs:2024` — `retain_memtries`).
7. `finish_block_processing` on the delay tracker, which also advances the tracker's window to the new head and reclaims everything below it (`chain.rs:2077`, `blocks_delay_tracker.rs:410`, `blocks_delay_tracker.rs:436`).
8. `check_orphans` — newly accepted block may unlock orphans (`chain.rs:2090`, `orphan.rs:390`).
9. `determine_status` computes `Next`/`Fork`/`Reorg` (`chain.rs:1590`) and returns an `AcceptedBlock` (`chain.rs:2097`).

### `ChainUpdate::postprocess_block` — the atomic commit (`chain_update.rs:222`)

All of the following accumulate in a single `ChainStoreUpdate` and are committed together by `ChainStoreUpdate::commit` → `finalize` → `StoreUpdate::commit` (`chain_update.rs:92`, `store/mod.rs:2090`, `store/mod.rs:1781`):
1. `apply_chunk_postprocessing` — persist each shard's `ShardUpdateResult` (new-chunk: chunk extra, trie changes, flat-state changes, optional state-transition data for witnesses; old-chunk: carry forward chunk extra with new state root) (`chain_update.rs:236`, `chain_update.rs:96`, `chain_update.rs:111`).
2. If not caught up, register the block for catch-up (`add_block_to_catchup`); save incoming receipts and any `StateSyncInfo` (`chain_update.rs:243`, `chain_update.rs:247`, `chain_update.rs:250`).
3. `save_block_header` + `update_header_head` (`chain_update.rs:253`, `chain_update.rs:254`).
4. `add_validator_proposals` to the epoch manager and merge its store update (`chain_update.rs:270`, `chain_update.rs:271`). See [epoch-validators-staking](epoch-validators-staking.md). **Changed in 2.14:** the separate `save_chunk_producers_for_header` call is gone — the `ChunkProducers` column is now seeded inside the epoch manager's `record_block_info_impl` → `seed_chunk_producers` (`chain/epoch-manager/src/lib.rs:1271`, `chain/epoch-manager/src/lib.rs:1297`) (#15944), so the chain pipeline only merges the epoch-manager update.
5. If `ContinuousEpochSync` is enabled and this is the first block of an epoch, `update_epoch_sync_proof` keyed off `prev_hash` (`chain_update.rs:273`, `chain_update.rs:283`).
6. `save_block` (stores the body even on a non-canonical fork) and `inc_block_refcount(prev_hash)` (`chain_update.rs:293`, `chain_update.rs:294`).
7. (SPICE only) record uncertified chunks and spice endorsement stats (`chain_update.rs:299`).
8. `update_head` (`chain_update.rs:312`, fork choice below).
9. If this became head and it is an epoch's first block, save the previous epoch's light-client block (`chain_update.rs:328`).

`postprocess_block_only` then runs `check_protocol_version` when a new head was set, and only afterwards calls `commit` (`chain.rs:1902`, `chain_update.rs:67`).

### Fork choice / head update (`chain_update.rs:440` — `update_head`)

NEAR's fork-choice rule is **highest height wins**. `update_head` first calls `update_final_head_from_block` (advances the persisted **final head** to the header's `last_final_block()` if higher, `chain_update.rs:373`), then, iff `header.height() > current head.height`, saves the new **body head** via `save_body_head` (`chain_update.rs:448`). Because epoch boundaries and fork choice are both by height, the first block to cross an epoch end is guaranteed to become head (comment at `chain_update.rs:321`). A **reorg** is detected post-hoc in `determine_status` (`chain.rs:1590`): if the new head's `prev_block_hash` is not the old head's `last_block_hash`, it is `Reorg(old_hash)` (`chain.rs:1607`). There is no explicit revert step — the canonical chain is whatever the per-height index points to from the (now higher) head; orphaned side-fork blocks remain in the store until GC prunes them.

When the final head advances and the block is a SPICE block, `record_chunk_certifying_blocks` walks back from the new final header and write-once records each newly-final block as the certifying block of the chunks it certifies (`chain_update.rs:393`; nightly only — a non-SPICE header has no `chunk_execution_root` and returns immediately).

### Orphan & missing-chunk handling

- **Orphans** (unknown parent): held in `OrphanBlockPool`. When a block is accepted, `check_orphans(prev_hash, …)` (`orphan.rs:390`) re-submits orphans whose `prev_hash` just arrived via `remove_by_prev_hash` (`orphan.rs:198`), and (within `NUM_ORPHAN_ANCESTORS_CHECK = 3` depth, `orphan.rs:31`) requests missing chunks for near-descendant orphans, bounded by `MAX_ORPHAN_MISSING_CHUNKS = 5` (`orphan.rs:38`, `orphan.rs:323`).
- **Blocks with missing chunks**: held in `MissingChunksPool` (`missing_chunks.rs:48`), entered via `add_block_with_missing_chunks` (`missing_chunks.rs:87`) and drained by `ready_blocks` (`missing_chunks.rs:79`); `check_blocks_with_missing_chunks` (`chain.rs:2683`) re-submits them once their chunks are available (driven by the client when chunks arrive).
- **Optimistic blocks**: an `OptimisticBlock` lets chunk apply start before the full block arrives. `preprocess_optimistic_block` validates and queues it (`chain.rs:1291`); `maybe_process_optimistic_block` drops heights already processed (`chain.rs:1308`); `process_optimistic_block` schedules apply jobs (`chain.rs:1361`); results are cached and reused by the normal pipeline. It returns early (no-op) when SPICE is enabled for the epoch (`chain.rs:1374`). See `docs/architecture/how/optimistic_block.md`.

### Header-only validation (`chain.rs:885` — `validate_header`)

Used by both `preprocess_block` and the header-first / sync path (`process_block_header`, `chain.rs:1056`; `sync_block_headers`, `chain.rs:1518`). Checks, in order: no challenges present (`chain.rs:886`); timestamp not too far in the future (`chain.rs:893`); header signature (`chain.rs:897`); `epoch_id`/`next_epoch_id` match those derived from prev (`chain.rs:907`, `chain.rs:912`); header `latest_protocol_version` ≥ epoch protocol version (`chain.rs:920`); **SPICE-ness of the header must equal `ProtocolFeature::Spice.enabled(epoch_protocol_version)` in both directions** — tightened in 2.14 from a one-way check, and logged before `InvalidProtocolVersion` (`chain.rs:929`); `next_bp_hash` (`chain.rs:941`); chunk-mask length and `verify_chunks_included` (`chain.rs:952`, `chain.rs:956`); `prev_height` matches prev header (`chain.rs:960`); strictly increasing raw timestamp (`chain.rs:968`). For non-`PRODUCED` blocks (`chain.rs:973`) it additionally verifies aggregated **approvals** and the Doomslug 2/3 stake threshold (`chain.rs:976`, `chain.rs:992`), the derived `last_ds_final_block`/`last_final_block` (`chain.rs:1017` — see [consensus-finality](consensus-finality.md)), the block Merkle root (`chain.rs:1023`), and — when `ValidateBlockOrdinalAndEpochSyncDataHash` is enabled for the block's `epoch_protocol_version` — the block ordinal (`InvalidBlockOrdinal`) and `epoch_sync_data_hash` (`InvalidEpochSyncDataHash`) (`chain.rs:1027`). Finally it validates chunk endorsements in the header, choosing the SPICE or non-SPICE variant (`chain.rs:1043`, `chain.rs:1045`).

### Ancestry-free header approval check (`chain.rs:4192` — `verify_header_approvals_without_ancestry`)

New in 2.14 (moved out of the dead `ChainUpdate::verify_orphan_header_approvals` and made live). It verifies a header carries >2/3 of the stake of a validator set the node already knows, without needing the header's ancestry: producer signature first (a tampered approval dies after one signature check instead of ~100), then `verify_approvals_and_threshold_orphan` against the header's epoch info. Headers too old to carry a `prev_height` (V1/V2) are rejected with `Error::Other("header too old to verify approvals without ancestry")`. Its only production caller is the sync-decision path `Client::note_verified_peer_height` (`chain/client/src/client.rs:1233`), which records a peer's height only if the relayed block's approvals verify — see [sync](sync.md).

**Hardening (#16389):** `verify_approvals_and_threshold_orphan` now computes `prev_block_height.checked_add(1)` and returns `Error::InvalidBlockHeight` on overflow (`chain/chain/src/approval_verification.rs:57`). Previously a peer-supplied header with `prev_height = u64::MAX` overflowed; the release profile sets `overflow-checks = true` and `panic = 'abort'` (`Cargo.toml:396`, `Cargo.toml:397`; the dev profile also sets `panic = 'abort'`, `Cargo.toml:393`), so the overflow **aborted the node process** rather than unwinding. Verified against the pre-fix code, which read `prev_block_height + 1`.

### Early transaction preparation (chunk-producer optimization)

When applying a chunk, the chain installs a `PostStateReadyCallback` so the *next* chunk's producer can start selecting transactions as soon as the post-state is ready (`chain.rs:3725` — `get_on_post_state_ready_callback`). This is an optimization only; a failure path returns `None` and the node simply does the work later.

- The next chunk's producer is resolved with `get_chunk_producer_info_anchored`, anchored at the **grandparent** (`prev_block`, already processed) rather than the unprocessed parent (`chain.rs:3746`). Under `ProtocolFeature::EarlyKickout` this reads the `ChunkProducers` column seeded at the anchor; otherwise it falls back to canonical sampling (`chain/epoch-manager/src/adapter.rs:1106`).
- **Gas-overflow hardening (#16390):** `compute_gas_used_checked` / `compute_gas_limit_checked` / `compute_next_gas_price_checked` are now chained with `zip`/`and_then` and an `else` branch that logs "gas overflow in chunk headers; skipping early transaction preparation" and returns `None` (`chain.rs:3765`, `chain.rs:3783`). Previously these were `unwrap()`s (verified against the pre-fix code). Chunk header gas fields are cross-checked against `prev_chunk_extra` only where the node holds one — i.e. for shards it applies (`validate.rs:155`, `validate.rs:159` — `validate_chunk_with_chunk_extra_and_roots`) — while these helpers sum `gas_used`/`gas_limit` across *all* the block's chunk headers, so on the optimistic-block path the sum could overflow before any such check and abort the process.

Transaction selection itself lives in `NightshadeRuntime::prepare_transactions` (`chain/chain/src/runtime/mod.rs:843`); see [sharding-chunks](sharding-chunks.md) for the full selection rules. Two 2.14 bounds are worth recording here because they bound the work a block-processing thread can do:
- `MAX_TXS_PER_GROUP_PER_VISIT = 256` caps how many transactions are examined from one signer group per visit (`runtime/mod.rs:86`, `runtime/mod.rs:994`) (#16074).
- The wall-clock time limit is now checked **inside** the per-group loop, not only between groups, breaking to the outer `'add_txs_loop` label (`runtime/mod.rs:992` — the per-group `while let`, `runtime/mod.rs:998`–`runtime/mod.rs:1009`) (#16409); the between-groups check remains (`runtime/mod.rs:967`). Work is proportional to each *peeked* transaction's payload and a rejected transaction burns no gas and never advances `total_size`, so neither the gas nor the size budget bounded it. Not a protocol change — transaction selection is the chunk producer's own choice.

### Garbage collection (`garbage_collection.rs`)

Driven by `GCActor` (`chain/client/src/gc_actor.rs:51`): a non-archival node, or an archival node whose split-storage migration has finished (`DbKind::Hot`), runs the full `ChainStore::clear_data`; a legacy/mid-migration archival node runs `clear_archive_data` instead (`gc_actor.rs:78`).

`Chain::clear_data` (`garbage_collection.rs:60`) → `ChainStore::clear_data` (`garbage_collection.rs:140`). State-transition data and witnesses are cleared first (they accumulate fast), then `clear_old_blocks_data` (`garbage_collection.rs:165`) GCs blocks from the **tail** up to `gc_stop_height = get_gc_stop_height(head)` (the start of the epoch `gc_num_epochs_to_keep` epochs back), bounded by `gc_config.gc_blocks_limit` per run:
- `gc_stop_height > head.height` is a hard error (`Error::GCError("gc_stop_height cannot be larger than head.height")`, `garbage_collection.rs:180`).
- The computed `gc_stop_height` is **persisted** in `DBCol::BlockMisc` when it changes, and `fork_tail` is bumped to it if it lagged (`garbage_collection.rs:189`, `garbage_collection.rs:198`, `store/mod.rs:1761` — `update_gc_stop_height`).
- **Forks clearing** (`clear_forks_data`, `garbage_collection.rs:471`): for each height from `max(tail, fork_tail - gc_fork_clean_step)` up to `fork_tail`, delete any fork that terminates at that height or earlier, up to (excluding) the ancestor where the fork branched (`garbage_collection.rs:231`).
- **Canonical-chain clearing** (`clear_block_data`, `garbage_collection.rs:690`): delete canonical blocks from `tail + 1` to gc-stop exclusive, advancing the tail; it stops early when the previous block has refcount > 1 (a fork starts there), and errors `GCError("block on canonical chain shouldn't have refcount 0")` on refcount 0 (`garbage_collection.rs:284`, `garbage_collection.rs:277`).
- After canonical clearing of an epoch's last block, `gc_state` (`garbage_collection.rs:1246`) drops trie state for shards we no longer track and won't track next epoch. Which shards those are comes from `ShardTracker`; see the descendant fix under *Protocol-version-gated behavior*.
- **New in 2.14:** the `ChunkProducers` column is GC'd with a prefix scan by block hash (`gc_chunk_producers_for_block`, `garbage_collection.rs:535`), called from three places:
  - `clear_block_data` — once per block, immediately after the per-`ShardUId` `ChunkExtra` loop and deliberately *outside* it, because `ChunkProducers` is keyed by `ShardId` and `get_shard_uids_to_gc` can return two `ShardUId`s sharing one `ShardId` at a reshard boundary; the prefix scan deletes each row exactly once, including next-layout rows (`garbage_collection.rs:753`).
  - `clear_head_block_data` — alongside `BlockInfo`, because the column is seeded together with `BlockInfo` and is insert-only, so a stale row must go before re-processing re-seeds it (`garbage_collection.rs:1027`). This is the **undo-block tool's** head-rollback path (`tools/undo-block/src/lib.rs:30`, `tools/undo-block/src/lib.rs:69`), not the periodic GC loop.
  - `clear_chunk_data_and_headers` — for header-only hashes that never got a body, whose `BlockInfo` persists and which `clear_block_data` therefore never sees; done before `HeaderHashesByHeight` is dropped so the rows cannot be orphaned (`garbage_collection.rs:607`).

  The three sites can visit the same hash: `clear_block_data` calls `clear_chunk_data_and_headers` itself in `GCMode::Canonical` (`garbage_collection.rs:799`), and the sweep's prefix scan reads the committed store (`ChainStoreUpdate::store()` returns the underlying `Store`, `store/mod.rs:1115`), so deletes still pending in the same `ChainStoreUpdate` are invisible to it and can be enqueued twice. That is **harmless**: `DBCol::ChunkProducers`' GC policy is `GcPolicy::Delete` (`core/store/src/columns.rs:784`), so `gc_col` issues a plain `delete` and never a refcount decrement (`garbage_collection.rs:1169`); `StoreUpdate::delete` only asserts `!column.is_rc()` (`core/store/src/store.rs:438`), the no-overwrite assertion applies to inserts only (`core/store/src/db/mod.rs:323`), and two identical `Delete` ops in one write batch are idempotent. Within a single `clear_block_data` call the two ranges are in fact disjoint — the sweep covers `chunk_tail..min_chunk_height` with `min_chunk_height <= tail <=` the cleared block's height (`garbage_collection.rs:793`–`garbage_collection.rs:799`) — so the overlap can only arise across iterations of one `clear_data` run.

`earliest_available` semantics changed in 2.14 (#16100): `get_earliest_block_hash` now scans from `max(gc_stop_height, tail)` rather than from the GC tail, because during normal operation everything below `gc_stop_height` is (or soon will be) collected, while after state sync `gc_stop_height` can fall below `tail` (`chain/chain/src/store/mod.rs:126`, `store/mod.rs:137`).

The full GC contract (genesis kept, tail always on canonical chain, one tail, fork-protection above gc-stop) is documented inline (`garbage_collection.rs:69`) and in `docs/architecture/how/gc.md`. **Archival** nodes on legacy storage skip block deletion; `clear_archive_data` (`garbage_collection.rs:450`) only trims the height-indexed columns the archive does not need, keeping all block/chunk/state data.

**GC config is now validated at startup** (#16158): `validate_gc_config` (`nearcore/src/config_validate.rs:91`) rejects a config where `gc_blocks_limit` per `gc_step_period` cannot keep up with `consensus.min_block_production_delay` — "garbage collection cannot keep up with block production" is a hard config-semantics error (`config_validate.rs:148`) — and warns when the headroom is under `RECOMMENDED_GC_RATE_MULTIPLIER`× (`config_validate.rs:158`). It also warns when `gc_num_epochs_to_keep` is below `MIN_GC_NUM_EPOCHS_TO_KEEP` and is being silently clamped (`config_validate.rs:110`).

### State-sync interaction with the pipeline

`reset_heads_post_state_sync` (`chain.rs:1620`) moves the body head to the sync block's parent, resets the final head to genesis, and sets `tail`/`chunk_tail` from the downloaded blocks — all in one `ChainStoreUpdate` — then explicitly tells the delay tracker the new head (`chain.rs:1661`), because state sync moves the head without processing a block. A known gap is recorded inline: the GC loops start at `tail`/`chunk_tail`, so nothing collects the heights this skip leaves behind (TODO #16264, `chain.rs:1651`).

`set_state_finalize` (`chain.rs:2742`) now cross-checks the reconstructed shard data against the sync block's chunk commitment via `validate_state_sync_chunk_extra` (`chain.rs:2787`). A mismatch in state root / outcomes proof / proposals / gas / balance / receipts / congestion info / bandwidth requests / shard split **panics with a diagnostic** telling the operator to restart from an empty data directory, because the node executed the chunk differently from the network (`chain.rs:2860`). Other errors propagate normally. See [sync](sync.md).

## Interactions

- **Consumes**: blocks/headers from [networking-p2p](networking-p2p.md) and [sync](sync.md); producer/epoch info, validator proposals, shard layout, `ChunkProducers` rows, and `gc_stop_height` from [epoch-validators-staking](epoch-validators-staking.md); approvals + finality fields from [consensus-finality](consensus-finality.md); chunk headers/bodies and incoming receipts from [sharding-chunks](sharding-chunks.md); chunk endorsements from [stateless-validation](stateless-validation.md); per-shard apply results from [runtime-execution](runtime-execution.md).
- **Produces**: the persisted header/body/final heads; stored blocks, chunk extras, trie/flat-state changes, incoming receipts, validator proposals; `AcceptedBlock` events to the client; catch-up and state-sync registrations consumed by [sync](sync.md); state snapshots; the post-state-ready callback that starts early transaction preparation for the next chunk.
- **Persists** everything through [state-storage](state-storage.md) via `ChainStoreUpdate` (atomic `StoreUpdate`).
- **Adjacent 2.14 fixes owned by sibling specs** (listed so a reader does not look for them here): the genesis-chunk partial-chunk-request panic fix lives in `chain/chunks/src/logic.rs` → [sharding-chunks](sharding-chunks.md) (#16141); the `ShardLayoutV3` "a shard is a descendant of itself" fix lives in `chain/epoch-manager/src/shard_tracker.rs` → [epoch-validators-staking](epoch-validators-staking.md) (#16184) — it matters here because `ShardTracker` decides what `gc_state` drops and what `get_catchup_and_state_sync_infos` syncs; resharding boundary-block layout resolution lives in the witness-validation path → [stateless-validation](stateless-validation.md) (#16068, #16123).

## Protocol-version-gated behavior

- **`ValidateBlockOrdinalAndEpochSyncDataHash`** — declared at `core/primitives-core/src/version.rs:437`, activates at **version 85** (voted arm at `version.rs:611`). Before 85, `validate_header` does not check `block_ordinal` or `epoch_sync_data_hash`; from 85 on it rejects a mismatched block ordinal (`Error::InvalidBlockOrdinal`) or epoch-sync-data hash (`Error::InvalidEpochSyncDataHash`) (`chain.rs:1027`). The gate is on the block's `epoch_protocol_version`. Unconditionally on at v87 (`MIN_SUPPORTED_PROTOCOL_VERSION = 84`, `version.rs:652`, so the gate is still evaluated but cannot be false for a supported epoch above 85).
- **`ContinuousEpochSync`** — declared at `version.rs:355`, activates at **version 85** (voted arm at `version.rs:605`). When enabled, `postprocess_block` updates the epoch-sync proof at each epoch's first block (`chain_update.rs:273`), and GC additionally deletes `DBCol::BlockHeader` when clearing a block (`garbage_collection.rs:758`). Note both gates are `enabled(PROTOCOL_VERSION)` (binary-wide, not the block's epoch version) (`chain_update.rs:273`, `garbage_collection.rs:758`). On at v87.
- **`EarlyKickout`** — declared at `version.rs:395`, **moved from nightly 152 to stable 87** in this release (`version.rs:626`). Chain-pipeline consequences: (a) the next chunk's producer for early transaction preparation is resolved from the grandparent-anchored `ChunkProducers` row rather than by canonical sampling (`chain.rs:3746`, `chain/epoch-manager/src/adapter.rs:1106`); (b) the `ChunkProducers` column is populated by the epoch manager's `record_block_info` and must be garbage-collected alongside `BlockInfo` (`garbage_collection.rs:1027`). The kickout/blacklisting rules themselves are [epoch-validators-staking](epoch-validators-staking.md); the V1-vs-V2 partial-witness envelope switch is [stateless-validation](stateless-validation.md).
- **Optimistic block production** — the feature enum is `_DeprecatedProduceOptimisticBlock`, activated at **version 77** (`version.rs:582`) and always on at v87. Enables the optimistic-block path (`preprocess_optimistic_block`, `chain.rs:1291`); does not change finality. See [consensus-finality](consensus-finality.md).
- **`Spice`** — declared at `version.rs:354`, activation version **180** (`version.rs:638`), **not enabled** in the stable v87 build (`STABLE_PROTOCOL_VERSION = 87`, `version.rs:680`; `NIGHTLY_PROTOCOL_VERSION` is 157). **Everything in this bullet is nightly-only and not stable behavior.** When enabled it changes preprocess substantially: chunk-endorsement-in-block validation is skipped (`chain.rs:2573`), incoming-receipt collection is deferred (`chain.rs:2582`), certified-execution-result gathering feeds the gas-price/total-supply checks (`chain.rs:2518`, `chain.rs:2551`), a non-SPICE block in a SPICE epoch (and vice versa) is rejected with `Error::Other` (`chain.rs:2593`, `chain.rs:2606`), the header's `chunk_execution_root` must match the root recomputed from the body's core statements (`validate.rs:235` — `validate_spice_chunk_execution_root`, new in 2.14), and core-statement / endorsement-stats validation is added (`chain.rs:2602`). In `validate_header`, header SPICE-ness must equal the epoch's (`chain.rs:929`) and a SPICE epoch uses `validate_spice_chunk_endorsements_in_header` (`chain.rs:1043`). `postprocess_block` records uncertified chunks and spice endorsement stats (`chain_update.rs:299`), and `update_final_head_from_block` records chunk-certifying blocks (`chain_update.rs:393`). GC gains a `SpiceInvalidChunks` sweep keyed by height (`garbage_collection.rs:814` — `gc_spice_invalid_chunks_at_height`) and gates witness GC on the final head's SPICE-ness rather than the binary's protocol version (`garbage_collection.rs:397`). Optimistic block processing is a no-op under SPICE (`chain.rs:1374`).
- **`EnforcePerReceiptStorageProofLimit`** (v86, `version.rs:466`/`version.rs:618`) and **`EnforceStorageProofLimitForAllActions`** (v87, `version.rs:473`/`version.rs:624`) are [runtime-execution](runtime-execution.md) storage-proof-limit changes, **not** chain-pipeline changes.
- **No new v87 feature changes the block-processing pipeline itself.** The other v87 features (`FixContractLoadingError`, `RejectEmptyMethodName`, `RejectDelegateV2`, `RejectWithdrawFromGasKeyInDelegate`, `RemoveGasRewards`, `FixMlDsaCostCharging`, `GlobalContractSameChunkCallFix`, `UniversalAccounts`, the SHA3 and ML-DSA host functions) are all runtime/action-validation changes — see [runtime-execution](runtime-execution.md) and [accounts-keys](accounts-keys.md).

The Doomslug finality rule itself is **not** version-gated at v87; see [consensus-finality](consensus-finality.md).

## Invariants & failure modes

- **Atomic commit.** A block either commits all of its writes via one `StoreUpdate::commit` (`store/mod.rs:2090`) or none — "if rejected nothing will be updated in underlying storage" (`chain_update.rs:41`). A crash mid-processing leaves the store consistent.
- **Bounded in-flight blocks.** At most `MAX_PROCESSING_BLOCKS = 5` blocks may be between preprocess and postprocess; excess → `TooManyProcessingBlocks` (`block_processing_utils.rs:16`, `block_processing_utils.rs:70`, `chain.rs:2420`).
- **Postprocess pool invariant.** A block reaching `postprocess_ready_block` must be in `blocks_in_processing`, else panic (`chain.rs:1927`).
- **No state below tail.** Blocks with `prev_height < gc_stop_height` are rejected as old forks (`chain.rs:2481`); orphans below tail are not stored (`chain.rs:1723`).
- **Highest-height fork choice.** Head advances only when `header.height() > head.height` (`chain_update.rs:446`); equal-height forks never overtake the head.
- **Finalizability.** Blocks that cannot be reached from the final head are dropped with `CannotBeFinalized`; the walk is bounded to 20 parents and returns `Ok` past that to avoid long delays (`chain.rs:857`).
- **Byzantine assertions.** Block-body, gas-price, and total-supply *mismatches* fire `byzantine_assert!` (debug-fatal) before returning the corresponding `Error` (`chain.rs:2502`, `chain.rs:2536`, `chain.rs:2558`); chunk-signature/body failures likewise in `validate_block_impl` (`chain.rs:829`).
- **No panic on arithmetic overflow in block-level checks.** Overflow in the gas-price or total-supply computation returns `Error::Other`, not a panic (`chain.rs:2540`, `chain.rs:2563`). Overflow on `prev_height + 1` in ancestry-free approval verification returns `Error::InvalidBlockHeight` (`approval_verification.rs:57`); overflow in early transaction preparation's gas math skips the optimization (`chain.rs:3783`).
- **Client protocol-version guard.** If, on an epoch boundary, the network's protocol version exceeds the binary's `PROTOCOL_VERSION`, `check_protocol_version` panics rather than producing invalid blocks (`chain_update.rs:67`). It is called only after a new head is set, before `commit` (`chain.rs:1902`).
- **State-sync divergence is fatal by design.** If the data reconstructed by state sync disagrees with the sync block's chunk commitment, the node panics with an operator-facing message rather than continuing on a divergent state (`chain.rs:2860`).
- **Bounded monitoring state.** `BlocksDelayTracker` refuses blocks outside `[head - 50, head + BLOCK_HORIZON)` and beyond 8 per height, labelling the reason (`below_window` / `above_window` / `height_full`) for metrics (`blocks_delay_tracker.rs:197`). The window is advanced both by `finish_block_processing` and, after state sync, by an explicit `update_head` (`blocks_delay_tracker.rs:380`, `chain.rs:1661`).
- **GC contract.** Genesis is never GC'd; the tail is always on the canonical chain and only one tail exists; the oldest fork branch point is never affected (`garbage_collection.rs:69`). `gc_stop_height > head.height` and a canonical block with refcount 0 are both `GCError`s (`garbage_collection.rs:180`, `garbage_collection.rs:277`).
- **GC must outpace block production.** A config where GC cannot keep up is rejected at startup (`nearcore/src/config_validate.rs:148`).
- **Block-known dedup.** Every attempted block hash/height is recorded as processed even on failure, so unrequested duplicates are filtered (`chain.rs:1282`, `chain.rs:1283`).

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `chain/chain/src/chain.rs:273` | `Chain` | pipeline state: pools, store, spawners, delay tracker |
| `chain/chain/src/chain.rs:1256` | `Chain::start_process_block_async` | pipeline entry; records processed hash/height |
| `chain/chain/src/chain.rs:1678` | `start_process_block_impl` | hash/sig gate, preprocess, error routing, schedule apply |
| `chain/chain/src/chain.rs:1071` | `verify_block_hash_and_signature` | body-hash + header-signature gate |
| `chain/chain/src/chain.rs:2410` | `preprocess_block` | full validation gate, builds apply jobs (no writes) |
| `chain/chain/src/chain.rs:2540` | `preprocess_block` gas-price `None` arm | overflow → `Error::Other`, not a panic |
| `chain/chain/src/chain.rs:2644` | `get_catchup_and_state_sync_infos` | catch-up / orphan-at-epoch-boundary decision |
| `chain/chain/src/chain.rs:885` | `validate_header` | header validity, approvals, finality fields, ordinal/epoch-sync hash |
| `chain/chain/src/chain.rs:929` | `validate_header` spice check | header SPICE-ness must equal the epoch's (both directions) |
| `chain/chain/src/chain.rs:778` | `validate_block_impl` | per-chunk + body validation |
| `chain/chain/src/chain.rs:1105` | `validate_chunk_headers` | new vs old chunk header consistency + block proposals |
| `chain/chain/src/chain.rs:1138` | `ping_missing_chunks` | detect locally-missing chunks → `ChunksMissing` |
| `chain/chain/src/chain.rs:857` | `check_if_finalizable` | drop non-finalizable blocks (20-parent bound) |
| `chain/chain/src/chain.rs:1822` | `schedule_apply_chunks` | spawn per-shard apply jobs, send results back |
| `chain/chain/src/chain.rs:1470` | `postprocess_ready_blocks` | drain finished applies |
| `chain/chain/src/chain.rs:1918` | `postprocess_ready_block` | commit, storage maintenance, orphan check, status |
| `chain/chain/src/chain.rs:1881` | `postprocess_block_only` | `ChainUpdate::postprocess_block` + version check + commit |
| `chain/chain/src/chain.rs:1590` | `determine_status` | Next / Fork / Reorg classification |
| `chain/chain/src/chain.rs:1291` | `preprocess_optimistic_block` | optimistic-block path |
| `chain/chain/src/chain.rs:1361` | `process_optimistic_block` | schedules optimistic apply; no-op under SPICE |
| `chain/chain/src/chain.rs:1620` | `reset_heads_post_state_sync` | head/tail reset after state sync; tells delay tracker |
| `chain/chain/src/chain.rs:2787` | `validate_state_sync_chunk_extra` | panics if synced state diverges from the chunk commitment |
| `chain/chain/src/chain.rs:3725` | `get_on_post_state_ready_callback` | early tx preparation hook; grandparent-anchored producer |
| `chain/chain/src/chain.rs:3783` | gas-overflow guard | skip early tx preparation instead of panicking |
| `chain/chain/src/chain.rs:4192` | `verify_header_approvals_without_ancestry` | >2/3-stake check without ancestry (sync decisions) |
| `chain/chain/src/chain.rs:3907` | `check_block_known` | dedup against processed/stored blocks |
| `chain/chain/src/approval_verification.rs:57` | `verify_approvals_and_threshold_orphan` | `checked_add(1)` on `prev_height`; no abort on `u64::MAX` |
| `chain/chain/src/chain_update.rs:222` | `ChainUpdate::postprocess_block` | atomic write of all block results |
| `chain/chain/src/chain_update.rs:111` | `process_apply_chunk_result` | persist chunk extra/trie/flat-state |
| `chain/chain/src/chain_update.rs:440` | `update_head` | highest-height fork choice, body head |
| `chain/chain/src/chain_update.rs:373` | `update_final_head_from_block` | advance persisted final head |
| `chain/chain/src/chain_update.rs:393` | `record_chunk_certifying_blocks` | SPICE-only: write-once certifying-block index |
| `chain/chain/src/chain_update.rs:356` | `update_header_head` | advance header head |
| `chain/chain/src/chain_update.rs:92` | `ChainUpdate::commit` | commit the accumulated StoreUpdate |
| `chain/chain/src/chain_update.rs:67` | `check_protocol_version` | panic if network PV > binary PV |
| `chain/chain/src/validate.rs:194` | `validate_block_shard_split` | resharding split header check |
| `chain/chain/src/validate.rs:235` | `validate_spice_chunk_execution_root` | SPICE-only: header root vs body core statements |
| `chain/chain/src/orphan.rs:390` | `Chain::check_orphans` | unlock orphans whose parent arrived |
| `chain/chain/src/orphan.rs:73` | `OrphanBlockPool` | bounded orphan storage + eviction |
| `chain/chain/src/orphan.rs:323` | `should_request_chunks_for_orphan` | bounded missing-chunk requests for orphans |
| `chain/chain/src/missing_chunks.rs:48` | `MissingChunksPool` | blocks awaiting chunk bodies |
| `chain/chain/src/blocks_delay_tracker.rs:197` | `refusal_reason` | window/capacity bound on tracked blocks |
| `chain/chain/src/blocks_delay_tracker.rs:380` | `update_head` | move the tracking window, reclaim below it |
| `chain/chain/src/garbage_collection.rs:140` | `ChainStore::clear_data` | GC entry; witnesses then old blocks |
| `chain/chain/src/garbage_collection.rs:165` | `clear_old_blocks_data` | gc_stop_height bookkeeping, fork + canonical loops |
| `chain/chain/src/garbage_collection.rs:471` | `clear_forks_data` | delete fork branches up to fork tail |
| `chain/chain/src/garbage_collection.rs:690` | `clear_block_data` | canonical-chain clearing, advance tail |
| `chain/chain/src/garbage_collection.rs:535` | `gc_chunk_producers_for_block` | prefix-scan delete of `ChunkProducers` rows |
| `chain/chain/src/garbage_collection.rs:947` | `clear_head_block_data` | undo-block head rollback; third `ChunkProducers` GC site |
| `chain/chain/src/garbage_collection.rs:1169` | `gc_col` | per-column GC policy dispatch (delete vs refcount decrement) |
| `chain/chain/src/garbage_collection.rs:561` | `clear_chunk_data_and_headers` | chunk/header sweep below `min_chunk_height` |
| `chain/chain/src/garbage_collection.rs:814` | `gc_spice_invalid_chunks_at_height` | SPICE-only invalid-chunk sweep |
| `chain/chain/src/garbage_collection.rs:450` | `clear_archive_data` | archival-node trimming (keep block/state) |
| `chain/chain/src/garbage_collection.rs:1246` | `gc_state` | drop trie state for untracked shards |
| `chain/chain/src/store/mod.rs:126` | `get_earliest_block_hash` | earliest servable block = `max(gc_stop_height, tail)` |
| `chain/chain/src/store/mod.rs:1761` | `update_gc_stop_height` | persist the last known gc stop height |
| `chain/chain/src/store/mod.rs:2090` | `ChainStoreUpdate::commit` | finalize → StoreUpdate::commit (atomicity) |
| `chain/chain/src/runtime/mod.rs:86` | `MAX_TXS_PER_GROUP_PER_VISIT` | 256-tx cap per signer group per visit |
| `chain/chain/src/runtime/mod.rs:992` | `prepare_transactions` per-group loop | `peek_next` loop, capped and time-checked |
| `chain/chain/src/runtime/mod.rs:1005` | `prepare_transactions` inner time check | wall-clock deadline enforced inside the group loop |
| `Cargo.toml:396` | `[profile.release]` | `overflow-checks = true`, `panic = 'abort'` — why an overflow aborts |
| `core/store/src/columns.rs:784` | `DBCol::ChunkProducers` gc policy | `GcPolicy::Delete` — double-delete is idempotent |
| `chain/client/src/client.rs:1475` | `Client::start_process_block` | client-side entry into the pipeline |
| `chain/client/src/client.rs:1233` | `Client::note_verified_peer_height` | only production caller of the ancestry-free approval check |
| `chain/client/src/client_actor.rs:1485` | `try_process_unfinished_blocks` | drives postprocess loop |
| `chain/client/src/gc_actor.rs:51` | `GCActor::clear_data` | picks full GC vs legacy archival trimming |
| `nearcore/src/config_validate.rs:91` | `validate_gc_config` | reject GC config that cannot keep up |
| `core/primitives-core/src/version.rs:437` | `ValidateBlockOrdinalAndEpochSyncDataHash` | declared; activates v85 (`version.rs:611`) |
| `core/primitives-core/src/version.rs:355` | `ContinuousEpochSync` | declared; activates v85 (`version.rs:605`) |
| `core/primitives-core/src/version.rs:395` | `EarlyKickout` | declared; activates **v87** (`version.rs:626`) |
| `core/primitives-core/src/version.rs:354` | `Spice` | declared; activation v180 (disabled at v87) |
| `core/primitives-core/src/version.rs:680` | `STABLE_PROTOCOL_VERSION` | 87 |

## Open questions

- A reorg performs no explicit "revert" of the previously-canonical state; canonicity is implicitly re-derived from the per-height index off the new (higher) head, and stale side-fork blocks linger until GC. The exact set of per-height / `NextBlockHash` index updates that re-point the canonical chain on a reorg lives in `ChainStoreUpdate::finalize` (`chain/chain/src/store/mod.rs:1781`) and was not traced line-by-line here.
- `reset_heads_post_state_sync` carries an acknowledged leak (TODO #16264, `chain/chain/src/chain.rs:1651`): the GC loops start at `tail`/`chunk_tail`, so the heights that a state sync skips over are never collected — blocks, chunks, partial chunks and their receipt refcounts leak. Whether this is bounded in practice (by how far a sync can jump) was not determined from code alone.
- `ContinuousEpochSync` is gated on the *binary's* `PROTOCOL_VERSION`, not the block's epoch version (`chain/chain/src/chain_update.rs:273`), unlike every other gate in this component. Whether that asymmetry is intentional (the proof is node-local, not consensus-critical) is not stated in the code.
- Whether the `undo-block` tool's `clear_head_block_data` path (`garbage_collection.rs:1027`) is reachable outside that tool was not established; no other caller exists in the tree at HEAD.
