# Chain & block-processing pipeline

> Protocol version: 88 (stable) · Release: 2.14.0 · Derived from commit: bc4e9e8 · Generated: 2026-11-03
> Primary crates/files: `chain/chain/src/chain.rs`, `chain/chain/src/chain_update.rs`, `chain/chain/src/validate.rs`, `chain/chain/src/orphan.rs`, `chain/chain/src/missing_chunks.rs`, `chain/chain/src/block_processing_utils.rs`, `chain/chain/src/blocks_delay_tracker.rs`, `chain/chain/src/approval_verification.rs`, `chain/chain/src/garbage_collection.rs`, `chain/chain/src/store/mod.rs`, `chain/client/src/client.rs`, `chain/client/src/client_actor.rs`, `chain/client/src/gc_actor.rs`

## Role

This component is the orchestration layer that drives a block from "received/produced" to "committed and head updated". It validates a block, schedules chunk application, persists the results atomically, chooses the canonical head on forks, holds blocks whose parent or chunks are missing, and garbage-collects old data. It sits downstream of [networking-p2p](networking-p2p.md) / [sync](sync.md) (which deliver blocks) and ties together [consensus-finality](consensus-finality.md) (approval/finality gating), [epoch-validators-staking](epoch-validators-staking.md) (producer/epoch info, validator proposals), [sharding-chunks](sharding-chunks.md) (chunk headers/bodies), [stateless-validation](stateless-validation.md) (chunk endorsements), and [runtime-execution](runtime-execution.md) (the actual chunk apply). It persists everything through [state-storage](state-storage.md). Block processing is **two-phase and asynchronous**: a synchronous `preprocess` validates and schedules, chunk apply runs on a thread pool, and a separate `postprocess` step commits and updates the head.

## Key data structures

- **`Chain`** — `chain/chain/src/chain.rs:274` — owns `chain_store: ChainStore`, `epoch_manager`, `runtime_adapter`, `shard_tracker`, `doomslug_threshold_mode`, the orphan pools (`orphans`, `blocks_with_missing_chunks`, `blocks_pending_execution`, `optimistic_block_chunks`), `blocks_in_processing`, `blocks_delay_tracker`, `spice_core_reader`, and the `apply_chunks_sender`/`apply_chunks_receiver` channel that carries finished apply results back to postprocessing.
- **`MaybeValidated<Arc<Block>>`** — the block passed into the pipeline; `validate_block` lazily marks the body validated so repeated processing (e.g. after orphan resolution) does not re-run `validate_block_impl` (`chain/chain/src/chain.rs:765` — `Chain::validate_block`). The flag is set **only on success**: a failed validation leaves the payload unvalidated, so a later processing attempt re-runs the check (`core/primitives/src/utils.rs:98` — `MaybeValidated::validate_with`).
- **`Provenance`** — `chain/chain/src/types.rs:82` — `NONE`, `PRODUCED` (we built it — skip approval/finality re-checks), or `SYNC`. Gates which validations run (`chain/chain/src/chain.rs:985` — `validate_header`).
- **`VerifyBlockHashAndSignatureResult`** — `chain/chain/src/chain.rs:358` — `Correct` / `Incorrect` / `CannotVerifyBecauseBlockIsOrphan`; the tri-state returned by the body-hash + header-signature gate. It has been `pub` since the header-V4 change (#9316) — unchanged in 2.14. Only two variants are reachable: `verify_block_hash_and_signature` never constructs `CannotVerifyBecauseBlockIsOrphan` despite its doc comment (`chain.rs:1081`, `chain.rs:1083`), so every caller's handling of that arm is dead code today.
- **`BlockPreprocessInfo`** — `chain/chain/src/block_processing_utils.rs:19` — the output of `preprocess_block` carried to postprocessing: `is_caught_up`, `state_sync_info`, `incoming_receipts`, `provenance`, `apply_chunks_done_waiter`, `block_start_processing_time`, `sandbox_patch_generation`.
- **`BlockToApply`** — `core/primitives/src/optimistic_block.rs:193` — `Normal(CryptoHash)` or `Optimistic(BlockHeight)`; tags which kind of block a finished apply-job batch belongs to.
- **`BlocksInProcessing`** — `chain/chain/src/block_processing_utils.rs:55` — bounded set (`MAX_PROCESSING_BLOCKS = 5`, `block_processing_utils.rs:16`) of blocks currently between preprocess and postprocess; `add_dry_run` (`block_processing_utils.rs:153`) rejects with `Error::TooManyProcessingBlocks` once full (`block_processing_utils.rs:158`, `block_processing_utils.rs:70`).
- **`AcceptedBlock`** — `chain/chain/src/types.rs:105` — `{ hash, status: BlockStatus, provenance }`, returned to the client after postprocess.
- **`BlockStatus`** — `chain/chain/src/types.rs:60` — `Next` (extends head), `Fork` (head unchanged), `Reorg(old_hash)` (head switched away from a different prev). `is_new_head()` is true for `Next`/`Reorg` (`types.rs:71`).
- **`BlockProcessingArtifact`** — `chain/chain/src/block_processing_utils.rs:84` — accumulates side effects of a processing round: `orphans_missing_chunks`, `blocks_missing_chunks`, `invalid_chunks`, challenges.
- **`ChainUpdate`** — `chain/chain/src/chain_update.rs:43` — short-lived helper holding a `ChainStoreUpdate` plus `epoch_manager`/`runtime_adapter`. "If rejected nothing will be updated in underlying storage" (`chain_update.rs:41`) — all writes accumulate in one `StoreUpdate` and are committed atomically by `commit` (`chain_update.rs:92`). It does **not** carry `doomslug_threshold_mode`: the orphan-approval helper that used it moved onto `Chain` in 2.13.2 (not 2.14 — 2.13.0/2.13.1 still had the field) and is unchanged here (`chain_update.rs:43`, `chain.rs:4204`).
- **`Orphan`** — `chain/chain/src/orphan.rs:44` — `{ block, provenance, added }`; `OrphanBlockPool` (`orphan.rs:73`) caps at `MAX_ORPHAN_SIZE = 1024` (`orphan.rs:20`) and evicts by age (`MAX_ORPHAN_AGE_SECS = 300`, `orphan.rs:23`).
- **`BlocksDelayTracker`** — `chain/chain/src/blocks_delay_tracker.rs:31` — monitoring-only bookkeeping of block/chunk timestamps. It is **bounded**: it tracks a window of heights `[head - BLOCK_DELAY_TRACKING_COUNT, head + BLOCK_HORIZON)` and at most `MAX_TRACKED_BLOCKS_PER_HEIGHT = 8` blocks per height (`blocks_delay_tracker.rs:20`, `blocks_delay_tracker.rs:24`, `blocks_delay_tracker.rs:198`).
- **`Tip`** — `core/primitives/src/block.rs:888` (`Tip::from_header`) — the persisted head pointer (`last_block_hash`, `prev_block_hash`, `height`, `epoch_id`). The chain keeps three tips: **header head**, **body/chain head**, and **final head**.

## Behavior

### End-to-end ordered phases

Driven by `ClientActor`: blocks are fed via `Client::start_process_block` (`chain/client/src/client.rs:1475`), which calls `Chain::start_process_block_async`; finished blocks are drained by `try_process_unfinished_blocks` → `Client::postprocess_ready_blocks` (`chain/client/src/client.rs:1500`, `chain/client/src/client_actor.rs:1506`).

#### Phase 0 — entry & hash/signature gate

`Chain::start_process_block_async` (`chain.rs:1268`) records the receive time, then `start_process_block_impl` (`chain.rs:1690`). Step 0 calls `verify_block_hash_and_signature` (`chain.rs:1083`); an `Incorrect` result returns `Error::InvalidSignature` immediately (`chain.rs:1709`, `chain.rs:1711`). That gate checks two things: the body hash recomputed from the block body equals the header's `block_body_hash` (so the body cannot be swapped under a signed header), and the header signature verifies against the epoch's block producer (`chain.rs:1094`, `chain.rs:1099`, `chain.rs:1106`). The block hash and the height are always recorded as "processed" even on failure, so the same unrequested block is not reprocessed (`chain.rs:1294` — `save_block_hash_processed`, `chain.rs:1295` — `save_block_height_processed`).

#### Phase 1 — synchronous preprocess (validation + scheduling)

`preprocess_block` (`chain.rs:2422`) is the validation gate. **No chain state is written here.** It runs these checks in order:

1. `blocks_in_processing.add_dry_run` — bounded concurrency, else `TooManyProcessingBlocks` (`chain.rs:2432`).
2. Epoch known? else `EpochOutOfBounds` — checked before the header lookup so an unknown-epoch block is not treated as an orphan (`chain.rs:2438`).
3. Number of chunk headers == shard count, else `IncorrectNumberOfChunkHeaders` (`chain.rs:2442`).
4. Not already known (`check_block_known`, `chain.rs:3919`), else `BlockKnown` (`chain.rs:2447`).
5. Height not absurdly far ahead: `> head.height + epoch_length * 20` → `InvalidBlockHeight` (`chain.rs:2461`); skipped under the `sandbox` feature.
6. **Orphan check:** if `prev_hash` is neither head nor a stored block, partially verify the header signature (`partial_verify_orphan_header_signature`, `chain.rs:2475`) and the chunk-header/body consistency (`block.check_validity()`, `chain.rs:2478`), then return `Error::Orphan`. Full approval verification is deliberately *not* run on orphans (TODO at `chain.rs:2480`).
7. First DB I/O — `get_previous_header` (`chain.rs:2485`). Reject old forks: `prev_height < get_gc_stop_height(head)` → `InvalidBlockHeight` (`chain.rs:2493`).
8. Catch-up decision via `get_catchup_and_state_sync_infos` (`chain.rs:2497`, `chain.rs:2656`): if the prev block is the first of a new epoch but the block before it isn't caught up, the block is `Orphan`ed; otherwise computes `is_caught_up` and any `StateSyncInfo`.
9. **Header validation** — `validate_header` (`chain.rs:2503`, see below).
10. VRF: `verify_block_vrf` against the producer's key, then `random_value() == hash(vrf_value)` else `InvalidRandomnessBeaconOutput` (`chain.rs:2507`, `chain.rs:2509`).
11. **Block body validation** — `validate_block` → `validate_block_impl` (`chain.rs:2513`, `chain.rs:778`): per-chunk genesis/new-chunk checks, chunk-header signatures, and `block.check_validity()`. Failure fires `byzantine_assert!` before returning (`chain.rs:2514`).
12. `validate_block_shard_split` — header's shard-split field matches the expected resharding split for the last block of an epoch (`chain.rs:2518`, `validate.rs:194`). The expected value is `None` unless the block is the last of its epoch *and* the epoch config selects a dynamic shard layout; the gate is the epoch config's `ShardLayoutConfig`, not a `ProtocolFeature` (`chain/epoch-manager/src/lib.rs:2651`). See [epoch-validators-staking](epoch-validators-staking.md).
13. (SPICE only) gather newly certified block execution results from the prev block and the block's core statements (`chain.rs:2530`); they feed the gas-price and total-supply checks below.
14. Gas price (`verify_gas_price_checked`, `chain.rs:2539`) and total supply (`verify_total_supply_checked` / `..._spice`, `chain.rs:2561`). Each is a tri-state: `Some(false)` fires `byzantine_assert!` and returns `InvalidGasPrice` / `InvalidTotalSupply`; **`None` (arithmetic overflow) returns `Error::Other("arithmetic overflow when checking gas price"/"…total supply")` rather than panicking** (`chain.rs:2552`, `chain.rs:2574`).
15. `validate_chunk_headers` — new chunks point at `prev_hash`, old (missing) chunks equal the prev block's chunk header, plus `validate_block_proposals` (`chain.rs:2582`, `chain.rs:1117`).
16. Chunk endorsements: `validate_chunk_endorsements_in_block` (non-SPICE only) — >2/3 stake endorses each chunk; see [stateless-validation](stateless-validation.md) (`chain.rs:2585`).
17. **Missing-chunks check** — `ping_missing_chunks` returns `Error::ChunksMissing(headers)` if a chunk we track is absent from the store (`chain.rs:2588`, `chain.rs:1150`).
18. Collect incoming receipts from chunks (non-SPICE; SPICE defers this) (`chain.rs:2594`).
19. `check_if_finalizable` — drop blocks that cannot reach the final head (`Error::CannotBeFinalized`), bounded to `NUM_PARENTS_TO_CHECK_FINALITY = 20` parents (`chain.rs:2602`, `chain.rs:857`, `chain.rs:129`).
20. SPICE-ness cross-check and SPICE core-statement validation (`chain.rs:2604`–`chain.rs:2622`, nightly only — see *Protocol-version-gated behavior*).
21. `apply_chunks_preprocessing` builds the list of per-shard `UpdateShardJob`s (`chain.rs:2625`, `chain.rs:3413`), choosing `ApplyChunksMode::IsCaughtUp` vs `NotCaughtUp`. Returns `(apply_chunk_work, BlockPreprocessInfo, apply_chunks_still_applying)`.

The order is load-bearing: cheap/local checks precede the first DB I/O (`get_previous_header`, `chain.rs:2485`), and the orphan/epoch decisions are made before expensive validation so a block is parked in the right pool.

#### Phase 1b — preprocess error routing

On error, `start_process_block_impl` parks the block instead of dropping it (`chain.rs:1732` — the `Err(e)` match arm):
- `Orphan` → `save_orphan` into `OrphanBlockPool` (only if `height >= tail`), optionally requesting its missing chunks (`chain.rs:1735`, `chain.rs:1748`, `orphan.rs:285`).
- `ChunksMissing` → `blocks_with_missing_chunks.add_block_with_missing_chunks` plus a `BlockMissingChunks` artifact so the client fetches them (`chain.rs:1751`).
- `BlockPendingOptimisticExecution` → `blocks_pending_execution` pool (`chain.rs:1773`).
- `EpochOutOfBounds` / `BlockKnown` → logged and dropped (`chain.rs:1784`, `chain.rs:1789`).

#### Phase 2 — schedule chunk apply (async)

After a successful preprocess, the block is inserted into `blocks_in_processing` (`chain.rs:1817`) and `schedule_apply_chunks` (`chain.rs:1834`) spawns each shard's job on `apply_chunks_spawner`. The last job to finish sends `(BlockToApply, results)` back on `apply_chunks_sender` and fires the optional `ApplyChunksDoneSender` so the client knows to call postprocess. The actual per-chunk apply (transactions, receipts, state changes) is [runtime-execution](runtime-execution.md).

#### Phase 3 — postprocess & commit

`postprocess_ready_blocks` (`chain.rs:1482`) drains `apply_chunks_receiver` and, for each `Normal` block, calls `postprocess_ready_block` (`chain.rs:1930`):
1. Remove the block from `blocks_in_processing` (panics if absent — invariant, `chain.rs:1940`).
2. Push any chunk whose apply failed with bad data into `invalid_chunks` (`chain.rs:1958`).
3. `postprocess_block_only` (`chain.rs:1893`) → `ChainUpdate::postprocess_block` (see below) — the atomic commit. Returns `Option<Tip>` (Some iff this became the new head, `chain.rs:1962`).
4. `update_optimistic_blocks_pool` and finalize any completed background memtrie loads (`chain.rs:1975`, `chain.rs:1979`).
5. If a new head crossed a protocol-version boundary, notify the runtime `on_protocol_version_update` (`chain.rs:1986`).
6. Per-shard storage maintenance: `start_resharding` and advance flat-storage/memtrie head to the last final block (`chain.rs:2015`, `chain.rs:2029` — `update_flat_storage_and_memtrie`). At an epoch boundary, start the resharding memtrie preload and retain only memtries we care about this/next epoch (`chain.rs:2034`, `chain.rs:2036` — `retain_memtries`).
7. `finish_block_processing` on the delay tracker, which also advances the tracker's window to the new head and reclaims everything below it (`chain.rs:2089`, `blocks_delay_tracker.rs:410`, `blocks_delay_tracker.rs:436`).
8. `check_orphans` — newly accepted block may unlock orphans (`chain.rs:2102`, `orphan.rs:390`).
9. `determine_status` computes `Next`/`Fork`/`Reorg` (`chain.rs:1602`) and returns an `AcceptedBlock` (`chain.rs:2109`).

### `ChainUpdate::postprocess_block` — the atomic commit (`chain_update.rs:222`)

All of the following accumulate in a single `ChainStoreUpdate` and are committed together by `ChainStoreUpdate::commit` → `finalize` → `StoreUpdate::commit` (`chain_update.rs:92`, `store/mod.rs:2090`, `store/mod.rs:1781`):
1. `apply_chunk_postprocessing` — persist each shard's `ShardUpdateResult` (new-chunk: chunk extra, trie changes, flat-state changes, optional state-transition data for witnesses; old-chunk: carry forward chunk extra with new state root) (`chain_update.rs:236`, `chain_update.rs:96`, `chain_update.rs:111`).
2. If not caught up, register the block for catch-up (`add_block_to_catchup`); save incoming receipts and any `StateSyncInfo` (`chain_update.rs:243`, `chain_update.rs:247`, `chain_update.rs:250`).
3. `save_block_header` + `update_header_head` (`chain_update.rs:253`, `chain_update.rs:254`).
4. `add_validator_proposals` to the epoch manager and merge its store update (`chain_update.rs:270`, `chain_update.rs:271`). See [epoch-validators-staking](epoch-validators-staking.md). The `ChunkProducers` column is seeded inside the epoch manager's `record_block_info_impl` → `seed_chunk_producers` (`chain/epoch-manager/src/lib.rs:1280`, call sites `lib.rs:1306` and `lib.rs:1351`; the helper is `lib.rs:2484`), so the chain pipeline only merges the epoch-manager update.
5. If `ContinuousEpochSync` is enabled and this is the first block of an epoch, `update_epoch_sync_proof` keyed off `prev_hash` (`chain_update.rs:273`, `chain_update.rs:283`).
6. `save_block` (stores the body even on a non-canonical fork) and `inc_block_refcount(prev_hash)` (`chain_update.rs:293`, `chain_update.rs:294`).
7. (SPICE only) record uncertified chunks and spice endorsement stats (`chain_update.rs:299`).
8. `update_head` (`chain_update.rs:312`, fork choice below).
9. If this became head and it is an epoch's first block, save the previous epoch's light-client block (`chain_update.rs:328`).

`postprocess_block_only` then runs `check_protocol_version` when a new head was set, and only afterwards calls `commit` (`chain.rs:1914`, `chain_update.rs:67`).

### Fork choice / head update (`chain_update.rs:440` — `update_head`)

NEAR's fork-choice rule is **highest height wins**. `update_head` first calls `update_final_head_from_block` (advances the persisted **final head** to the header's `last_final_block()` if higher, `chain_update.rs:373`), then, iff `header.height() > current head.height`, saves the new **body head** via `save_body_head` (`chain_update.rs:445`, `chain_update.rs:448`). Because epoch boundaries and fork choice are both by height, the first block to cross an epoch end is guaranteed to become head (comment at `chain_update.rs:321`). A **reorg** is detected post-hoc in `determine_status` (`chain.rs:1602`): if the new head's `prev_block_hash` is not the old head's `last_block_hash`, it is `Reorg(old_hash)` (`chain.rs:1619`). There is no explicit revert step — the canonical chain is whatever the per-height index points to from the (now higher) head; orphaned side-fork blocks remain in the store until GC prunes them.

When the final head advances and the block is a SPICE block, `record_chunk_certifying_blocks` walks back from the new final header and write-once records each newly-final block as the certifying block of the chunks it certifies (`chain_update.rs:393`; nightly only — a non-SPICE header has no `chunk_execution_root` and returns immediately).

### Orphan & missing-chunk handling

- **Orphans** (unknown parent): held in `OrphanBlockPool`. When a block is accepted, `check_orphans(prev_hash, …)` (`orphan.rs:390`) re-submits orphans whose `prev_hash` just arrived via `remove_by_prev_hash` (`orphan.rs:198`), and (within `NUM_ORPHAN_ANCESTORS_CHECK = 3` depth, `orphan.rs:31`) requests missing chunks for near-descendant orphans, bounded by `MAX_ORPHAN_MISSING_CHUNKS = 5` (`orphan.rs:38`, `orphan.rs:323`). Re-submission goes back through `start_process_block_async`, so Phase 0 and Phase 1 run in full on an orphan that is unlocked.
- **Orphan read-back**: `Chain::is_orphan` (`orphan.rs:452`) answers presence, and `Chain::get_orphan` (`orphan.rs:458`, new in 2.14.0) returns the held `Arc<Block>`. `get_orphan` has **no production caller** in the tree — it was added by #16418 purely so a test can observe the pool (`test-loop-tests/src/tests/sync/adversarial_sync_block.rs:104`). The state-sync path parks the sync-hash block in the orphan pool, and `sync_block_status` polls it with `is_orphan`, not `get_orphan` (`chain/client/src/sync/state/mod.rs:198`); see [sync](sync.md).
- **Blocks with missing chunks**: held in `MissingChunksPool` (`missing_chunks.rs:48`), entered via `add_block_with_missing_chunks` (`missing_chunks.rs:87`) and drained by `ready_blocks` (`missing_chunks.rs:79`); `check_blocks_with_missing_chunks` (`chain.rs:2695`) re-submits them once their chunks are available (driven by the client when chunks arrive).
- **Optimistic blocks**: an `OptimisticBlock` lets chunk apply start before the full block arrives. `preprocess_optimistic_block` validates and queues it (`chain.rs:1303`); `maybe_process_optimistic_block` drops heights already processed (`chain.rs:1320`); `process_optimistic_block` schedules apply jobs (`chain.rs:1373`); results are cached and reused by the normal pipeline. It returns early (no-op) when SPICE is enabled for the epoch (`chain.rs:1386`). See `docs/architecture/how/optimistic_block.md`.

### Header-only validation (`chain.rs:885` — `validate_header`)

Used by both `preprocess_block` and the header-first / sync path (`process_block_header`, `chain.rs:1068`; `sync_block_headers`, `chain.rs:1530`). Checks, in order:

| # | Check | Site | Error |
| --- | --- | --- | --- |
| 1 | no challenges present | `chain.rs:886` | `InvalidChallenge` |
| 2 | timestamp not too far in the future | `chain.rs:891` | `InvalidBlockFutureTime` |
| 3 | header signature | `chain.rs:897` | `InvalidSignature` |
| 4 | `epoch_id` / `next_epoch_id` derived from prev | `chain.rs:907`, `chain.rs:912` | `InvalidEpochHash` |
| 5 | header `latest_protocol_version` ≥ epoch protocol version | `chain.rs:920` | `InvalidProtocolVersion` |
| 6 | header SPICE-ness **equals** `ProtocolFeature::Spice.enabled(epoch_protocol_version)` in both directions | `chain.rs:929` | `InvalidProtocolVersion` |
| 7 | **new in 2.14.0:** `BlockHeaderV7` rejected when `RejectBlockHeaderV7BeforeSpice` is on | `chain.rs:941` | `InvalidProtocolVersion` |
| 8 | `next_bp_hash` | `chain.rs:953` | `InvalidNextBPHash` |
| 9 | chunk-mask length, `verify_chunks_included` | `chain.rs:964`, `chain.rs:968` | `InvalidChunkMask` |
| 10 | `prev_height` matches prev header | `chain.rs:972` | `Other("Invalid prev_height")` |
| 11 | strictly increasing raw timestamp | `chain.rs:980` | `InvalidBlockPastTime` |

For non-`PRODUCED` blocks (`chain.rs:985`) it additionally verifies aggregated **approvals** and the Doomslug 2/3 stake threshold (`chain.rs:988`, `chain.rs:1004`), the derived `last_ds_final_block`/`last_final_block` (`chain.rs:1026` — see [consensus-finality](consensus-finality.md)), the block Merkle root (`chain.rs:1035`), and — when `ValidateBlockOrdinalAndEpochSyncDataHash` is enabled for the block's `epoch_protocol_version` — the block ordinal (`InvalidBlockOrdinal`) and `epoch_sync_data_hash` (`InvalidEpochSyncDataHash`) (`chain.rs:1039`). Finally it validates chunk endorsements in the header, choosing the SPICE or non-SPICE variant (`chain.rs:1054`, `chain.rs:1057`).

**Check 7 in detail.** In 2.14.0 the spice header moved from `BlockHeaderV7` to the new `BlockHeaderV8` (`core/primitives/src/block_header.rs:807`), and `is_spice()` now returns `false` for V7 and `true` only for V8 (`block_header.rs:1989`). That makes V7 a dead variant that check 6 no longer rejects outside Spice: without the new gate, `validate_header` would accept a V7 header in a non-Spice epoch and the rest of the node would read it as if it were V6 (`chunk_execution_root()` returns `None` for V7, `block_header.rs:1889`). `RejectBlockHeaderV7BeforeSpice` closes that by rejecting the variant outright from protocol version 87 (`chain.rs:941`). The check is deliberately feature-gated rather than unconditional — the in-code rationale is "honest nodes never produce V7 … the rejection is gated to keep agreement with those nodes" (`chain.rs:939`). Asserted by `test-loop-tests/src/tests/block_header_v7_before_spice.rs` (`block_header_v7_accepted_before_reject_feature:87` / `block_header_v7_rejected_with_reject_feature:108`), which broadcasts a re-encoded V7 copy of a V6 block below and at 87. Header semantics and versioning are [consensus-finality](consensus-finality.md) / [data-structures-serialization](data-structures-serialization.md); only the enforcement site lives here.

### Ancestry-free header approval check (`chain.rs:4204` — `verify_header_approvals_without_ancestry`)

Present since **2.13.2** (moved out of the dead `ChainUpdate::verify_orphan_header_approvals` and made live; not a 2.14 addition) and unchanged at 2.14.0. It verifies a header carries >2/3 of the stake of a validator set the node already knows, without needing the header's ancestry: producer signature first (a tampered approval dies after one signature check instead of ~100), then `verify_approvals_and_threshold_orphan` against the header's epoch info. Headers too old to carry a `prev_height` (V1/V2) are rejected with `Error::Other("header too old to verify approvals without ancestry")`. Its only production caller is the sync-decision path `Client::note_verified_peer_height` (`chain/client/src/client.rs:1225`, call at `client.rs:1233`), which records a peer's height only if the relayed block's approvals verify — see [sync](sync.md).

**Hardening (#16389):** `verify_approvals_and_threshold_orphan` computes `prev_block_height.checked_add(1)` and returns `Error::InvalidBlockHeight` on overflow (`chain/chain/src/approval_verification.rs:57`). Previously a peer-supplied header with `prev_height = u64::MAX` overflowed; the release profile sets `overflow-checks = true` and `panic = 'abort'` (`Cargo.toml:396`, `Cargo.toml:397`; the dev profile also sets `panic = 'abort'`, `Cargo.toml:393`), so the overflow **aborted the node process** rather than unwinding. Still present and unchanged at 2.14.0.

### Block admission on the state-sync path (#16418)

While `SyncStatus::StateSync` is active, `ClientActor`'s `BlockResponse` handler diverts the sync-hash block, its prev, and the "extra" blocks needed for incoming receipts away from this pipeline into `maybe_receive_state_sync_blocks` and **returns without calling `Client::receive_block`** (`chain/client/src/client_actor.rs:628`, `client_actor.rs:2104`, `chain/chain/src/state_sync/utils.rs:374`). Those blocks never reach Phase 0 or Phase 1. They are written by `Chain::save_orphan` (`orphan.rs:285`) or `Chain::save_block` (`chain.rs:722`), and **`save_block` performs no validation whatsoever** — it checks only that the block is not already stored and writes `DBCol::Block`.

Since 2.14.0 final the path re-uses this component's two gates before any write, in `ClientActor::validate_state_sync_block` (`client_actor.rs:2060`), whose `Save`/`Drop` verdict gates the destination `match` (`client_actor.rs:2137`, `client_actor.rs:2092`):
1. `Chain::verify_block_hash_and_signature` (`chain.rs:1083`) — the Phase 0 gate: body hash vs the header's `block_body_hash`, then the producer signature. `Incorrect` additionally **bans the sender** with `ReasonForBan::BadBlockHeader` (`client_actor.rs:2071`, `chain/client/src/client.rs:2648`).
2. then `Chain::validate_block` (`chain.rs:765`) — Phase 1 step 11's `validate_block_impl` per-chunk checks plus `Block::check_validity()` (`client_actor.rs:2083`).

**This fix is ungated**: no `ProtocolFeature` appears anywhere in `validate_state_sync_block` or `maybe_receive_state_sync_blocks` — it is client-side block admission, not a consensus rule, and takes effect the moment the binary runs.

At 2.14.0-rc.1 neither gate was effective on this path. `verify_block_hash_and_signature` was **never called** there at all, and each branch called `validate_block` and **discarded the `Result`** — the error was logged and execution fell straight through to `save_orphan`/`save_block` (`2.14.0-rc.1:chain/client/src/client_actor.rs:2072`→`:2077`, `:2083`→`:2089`, `:2099`→`:2105`). An unvalidated, unauthenticated body therefore reached storage under an honest block hash. Two of this component's own properties are what made that reachable, and both still hold:
- `Chain::validate_block` goes through `MaybeValidated::validate_with` (`core/primitives/src/utils.rs:98`), so a sync-hash block that passes here is parked in the orphan pool already **marked validated**, and the `check_orphans` re-submission after sync skips Phase 1 step 11. This site is the only place that body is ever validated.
- `validate_state_sync_chunk_extra` (`chain.rs:2799`) reads those bodies back during finalization and **panics** on divergence (see below), so a forged body was a remote panic trigger.

The rest of the path — when state sync runs, which blocks are requested, the `StateSyncBlockDestination` classification and its three storage treatments, the drop/ban policy, how `sync_block_status` polls with `is_orphan`, and the adversarial regression test — is owned by [sync](sync.md) §7 and is deliberately not repeated here.

### Early transaction preparation (chunk-producer optimization)

When applying a chunk, the chain installs a `PostStateReadyCallback` so the *next* chunk's producer can start selecting transactions as soon as the post-state is ready (`chain.rs:3737` — `get_on_post_state_ready_callback`). This is an optimization only; a failure path returns `None` and the node simply does the work later.

- The next chunk's producer is resolved with `get_chunk_producer_info_anchored`, anchored at the **grandparent** (`prev_block`, already processed) rather than the unprocessed parent (`chain.rs:3759`). Under `ProtocolFeature::EarlyKickout` this reads the `ChunkProducers` column seeded at the anchor; otherwise it falls back to canonical sampling (`chain/epoch-manager/src/adapter.rs:1093`, gate at `adapter.rs:1106`).
- **Gas-overflow hardening (#16390):** `compute_gas_used_checked` / `compute_gas_limit_checked` / `compute_next_gas_price_checked` are chained with `zip`/`and_then` and an `else` branch that logs "gas overflow in chunk headers; skipping early transaction preparation" and returns `None` (`chain.rs:3777`, `chain.rs:3795`). Previously these were `unwrap()`s. Chunk header gas fields are cross-checked against `prev_chunk_extra` only where the node holds one — i.e. for shards it applies (`validate.rs:100` — `validate_chunk_with_chunk_extra_and_roots`) — while these helpers sum `gas_used`/`gas_limit` across *all* the block's chunk headers, so on the optimistic-block path the sum could overflow before any such check and abort the process. Re-verified unchanged at 2.14.0.

Transaction selection itself lives in `NightshadeRuntime::prepare_transactions` (`chain/chain/src/runtime/mod.rs:843`); see [sharding-chunks](sharding-chunks.md) for the full selection rules. Two bounds are recorded here because they bound the work a block-processing thread can do (both re-verified unchanged at 2.14.0):
- `MAX_TXS_PER_GROUP_PER_VISIT = 256` caps how many transactions are examined from one signer group per visit (`runtime/mod.rs:86`, `runtime/mod.rs:994`) (#16074).
- The wall-clock time limit is checked **inside** the per-group loop, not only between groups, breaking to the outer `'add_txs_loop` label (`runtime/mod.rs:992` — the per-group `while let`, `runtime/mod.rs:1005`–`runtime/mod.rs:1009`) (#16409); the between-groups check remains (`runtime/mod.rs:967`). Work is proportional to each *peeked* transaction's payload and a rejected transaction burns no gas and never advances `total_size`, so neither the gas nor the size budget bounded it. Not a protocol change — transaction selection is the chunk producer's own choice.

### Garbage collection (`garbage_collection.rs`)

Driven by `GCActor` (`chain/client/src/gc_actor.rs:51`): a non-archival node, or an archival node whose split-storage migration has finished (`DbKind::Hot`), runs the full `ChainStore::clear_data`; a legacy/mid-migration archival node runs `clear_archive_data` instead (`gc_actor.rs:68`).

`Chain::clear_data` (`garbage_collection.rs:60`) → `ChainStore::clear_data` (`garbage_collection.rs:140`). State-transition data and witnesses are cleared first (they accumulate fast), then `clear_old_blocks_data` (`garbage_collection.rs:165`) GCs blocks from the **tail** up to `gc_stop_height = get_gc_stop_height(head)` (the start of the epoch `gc_num_epochs_to_keep` epochs back), bounded by `gc_config.gc_blocks_limit` per run:
- `gc_stop_height > head.height` is a hard error (`Error::GCError("gc_stop_height cannot be larger than head.height")`, `garbage_collection.rs:180`).
- The computed `gc_stop_height` is **persisted** in `DBCol::BlockMisc` when it changes, and `fork_tail` is bumped to it if it lagged (`garbage_collection.rs:190`, `garbage_collection.rs:199`, `store/mod.rs:1761` — `update_gc_stop_height`).
- **Forks clearing** (`clear_forks_data`, `garbage_collection.rs:471`): for each height from `max(tail, fork_tail - gc_fork_clean_step)` up to `fork_tail`, delete any fork that terminates at that height or earlier, up to (excluding) the ancestor where the fork branched (`garbage_collection.rs:231`).
- **Canonical-chain clearing** (`clear_block_data`, `garbage_collection.rs:690`): delete canonical blocks from `tail + 1` to gc-stop exclusive, advancing the tail; it stops early when the previous block has refcount > 1 (a fork starts there), and errors `GCError("block on canonical chain shouldn't have refcount 0")` on refcount 0 (`garbage_collection.rs:284`, `garbage_collection.rs:277`).
- After canonical clearing of an epoch's last block, `gc_state` (`garbage_collection.rs:1246`) drops trie state for shards we no longer track and won't track next epoch. Which shards those are comes from `ShardTracker`; see the descendant fix under *Interactions*.
- The `ChunkProducers` column is GC'd with a prefix scan by block hash (`gc_chunk_producers_for_block`, `garbage_collection.rs:535`), called from three places:
  - `clear_block_data` — once per block, immediately after the per-`ShardUId` `ChunkExtra` loop and deliberately *outside* it, because `ChunkProducers` is keyed by `ShardId` and `get_shard_uids_to_gc` can return two `ShardUId`s sharing one `ShardId` at a reshard boundary; the prefix scan deletes each row exactly once, including next-layout rows (`garbage_collection.rs:753`).
  - `clear_head_block_data` — alongside `BlockInfo`, because the column is seeded together with `BlockInfo` and is insert-only, so a stale row must go before re-processing re-seeds it (`garbage_collection.rs:1027`). This is the **undo-block tool's** head-rollback path (`tools/undo-block/src/lib.rs:30`, `tools/undo-block/src/lib.rs:69`), not the periodic GC loop. Re-checked at 2.14.0: `tools/undo-block` holds the only production callers of `clear_head_block_data` (`garbage_collection.rs:947`) in the tree; the only other caller is a unit test (`chain/chain/src/tests/chunk_producers.rs:626`).
  - `clear_chunk_data_and_headers` — for header-only hashes that never got a body, whose `BlockInfo` persists and which `clear_block_data` therefore never sees; done before `HeaderHashesByHeight` is dropped so the rows cannot be orphaned (`garbage_collection.rs:607`).

  The three sites can visit the same hash: `clear_block_data` calls `clear_chunk_data_and_headers` itself in `GCMode::Canonical` (`garbage_collection.rs:800`), and the sweep's prefix scan reads the committed store (`ChainStoreUpdate::store()` returns the underlying `Store`, `store/mod.rs:1115`), so deletes still pending in the same `ChainStoreUpdate` are invisible to it and can be enqueued twice. That is **harmless**: `DBCol::ChunkProducers`' GC policy is `GcPolicy::Delete` (`core/store/src/columns.rs:784`), so `gc_col` issues a plain `delete` and never a refcount decrement (`garbage_collection.rs:1169`); `StoreUpdate::delete` only asserts `!column.is_rc()` (`core/store/src/store.rs:438`), the no-overwrite assertion applies to inserts only (`core/store/src/db/mod.rs:323`), and two identical `Delete` ops in one write batch are idempotent. Within a single `clear_block_data` call the two ranges are in fact disjoint — the sweep covers `chunk_tail..min_chunk_height` with `min_chunk_height <= tail <=` the cleared block's height (`garbage_collection.rs:794`–`garbage_collection.rs:800`) — so the overlap can only arise across iterations of one `clear_data` run.

`earliest_available` semantics (#16100): `get_earliest_block_hash` scans from `max(gc_stop_height, tail)` rather than from the GC tail, because during normal operation everything below `gc_stop_height` is (or soon will be) collected, while after state sync `gc_stop_height` can fall below `tail` (`chain/chain/src/store/mod.rs:127`, `store/mod.rs:137`).

The full GC contract (genesis kept, tail always on canonical chain, one tail, fork-protection above gc-stop) is documented inline (`garbage_collection.rs:69`) and in `docs/architecture/how/gc.md`. **Archival** nodes on legacy storage skip block deletion; `clear_archive_data` (`garbage_collection.rs:450`) only trims the height-indexed columns the archive does not need, keeping all block/chunk/state data.

**GC config is validated at startup** (#16158): `validate_gc_config` (`nearcore/src/config_validate.rs:91`) rejects a config where `gc_blocks_limit` per `gc_step_period` cannot keep up with `consensus.min_block_production_delay` — "garbage collection cannot keep up with block production" is a hard config-semantics error (`config_validate.rs:148`) — and warns when the headroom is under `RECOMMENDED_GC_RATE_MULTIPLIER`× (`config_validate.rs:158`). It also warns when `gc_num_epochs_to_keep` is below `MIN_GC_NUM_EPOCHS_TO_KEEP` and is being silently clamped (`config_validate.rs:110`). All GC claims in this section were re-derived at 2.14.0; `garbage_collection.rs`, `gc_actor.rs` and `config_validate.rs` are byte-identical to 2.14.0-rc.1.

### State-sync interaction with the pipeline

`reset_heads_post_state_sync` (`chain.rs:1632`) moves the body head to the sync block's parent, resets the final head to genesis, and sets `tail`/`chunk_tail` from the downloaded blocks — all in one `ChainStoreUpdate` — then explicitly tells the delay tracker the new head (`chain.rs:1673`), because state sync moves the head without processing a block. A known gap is recorded inline: the GC loops start at `tail`/`chunk_tail`, so nothing collects the heights this skip leaves behind (TODO #16264, `chain.rs:1663`).

`set_state_finalize` (`chain.rs:2754`) cross-checks the reconstructed shard data against the sync block's chunk commitment via `validate_state_sync_chunk_extra` (`chain.rs:2799`). A mismatch in state root / outcomes proof / proposals / gas / balance / receipts / congestion info / bandwidth requests / shard split **panics with a diagnostic** telling the operator to restart from an empty data directory, because the node executed the chunk differently from the network (`chain.rs:2872`). Other errors propagate normally. See [sync](sync.md).

## Interactions

- **Consumes**: blocks/headers from [networking-p2p](networking-p2p.md) and [sync](sync.md); producer/epoch info, validator proposals, shard layout, `ChunkProducers` rows, and `gc_stop_height` from [epoch-validators-staking](epoch-validators-staking.md); approvals + finality fields from [consensus-finality](consensus-finality.md); chunk headers/bodies and incoming receipts from [sharding-chunks](sharding-chunks.md); chunk endorsements from [stateless-validation](stateless-validation.md); per-shard apply results from [runtime-execution](runtime-execution.md).
- **Produces**: the persisted header/body/final heads; stored blocks, chunk extras, trie/flat-state changes, incoming receipts, validator proposals; `AcceptedBlock` events to the client; catch-up and state-sync registrations consumed by [sync](sync.md); state snapshots; the post-state-ready callback that starts early transaction preparation for the next chunk.
- **Persists** everything through [state-storage](state-storage.md) via `ChainStoreUpdate` (atomic `StoreUpdate`).
- **Adjacent fixes owned by sibling specs** (listed so a reader does not look for them here): the `BlockHeaderV8` layout and the header-version enum belong to [data-structures-serialization](data-structures-serialization.md) and [consensus-finality](consensus-finality.md) (#16621) — only the `validate_header` rejection site is documented above; the state-sync request/retry flow that the block-admission gate feeds is [sync](sync.md) (#16418); the `ShardLayoutV3` "a shard is a descendant of itself" fix lives in `chain/epoch-manager/src/shard_tracker.rs` → [epoch-validators-staking](epoch-validators-staking.md) (#16184) — it matters here because `ShardTracker` decides what `gc_state` drops and what `get_catchup_and_state_sync_infos` syncs; the `BlockInfo` eviction fix in the early-kickout seed walk is [epoch-validators-staking](epoch-validators-staking.md) (#16474), and it feeds the `ChunkProducers` rows this pipeline reads and GCs.

## Protocol-version-gated behavior

`STABLE_PROTOCOL_VERSION = 88` (`core/primitives-core/src/version.rs:698`), `MIN_SUPPORTED_PROTOCOL_VERSION = 84` (`version.rs:670`), `NIGHTLY_PROTOCOL_VERSION = 157` (`version.rs:701`). The 2.14.0 release activates in **two steps**: protocol version 87, then 88 two days later (`core/primitives/src/version.rs` upgrade schedule).

- **`RejectBlockHeaderV7BeforeSpice`** — declared at `version.rs:505`, activates at **version 87** (`version.rs:642`). **New in 2.14.0 final; not present at all in 2.14.0-rc.1.** Gated on the block's `epoch_protocol_version`. Before 87, `validate_header` accepts a `BlockHeaderV7` in a non-Spice epoch and the node reads it as a V6 header; from 87 it returns `Error::InvalidProtocolVersion` with the log "block header v7 is not valid in this epoch" (`chain.rs:941`). This is the only change to this component's validation rules in 2.14.0 final. See the detailed note under *Header-only validation*.
- **`ValidateBlockOrdinalAndEpochSyncDataHash`** — declared at `version.rs:437`, activates at **version 85** (voted arm at `version.rs:625`). Before 85, `validate_header` does not check `block_ordinal` or `epoch_sync_data_hash`; from 85 on it rejects a mismatched block ordinal (`Error::InvalidBlockOrdinal`) or epoch-sync-data hash (`Error::InvalidEpochSyncDataHash`) (`chain.rs:1039`). The gate is on the block's `epoch_protocol_version`. Unconditionally on at v88 (`MIN_SUPPORTED_PROTOCOL_VERSION = 84`, so the gate is still evaluated but cannot be false for a supported epoch above 85).
- **`ContinuousEpochSync`** — declared at `version.rs:355`, activates at **version 85** (voted arm at `version.rs:619`). When enabled, `postprocess_block` updates the epoch-sync proof at each epoch's first block (`chain_update.rs:273`), and GC additionally deletes `DBCol::BlockHeader` when clearing a block (`garbage_collection.rs:758`). Note both gates are `enabled(PROTOCOL_VERSION)` (binary-wide, not the block's epoch version). On at v88.
- **`EarlyKickout`** — declared at `version.rs:395`, activates at **version 87** (`version.rs:639`). Chain-pipeline consequences: (a) the next chunk's producer for early transaction preparation is resolved from the grandparent-anchored `ChunkProducers` row rather than by canonical sampling (`chain.rs:3759`, `chain/epoch-manager/src/adapter.rs:1106`); (b) the `ChunkProducers` column is populated by the epoch manager's `record_block_info` and must be garbage-collected alongside `BlockInfo` (`garbage_collection.rs:1027`). The kickout/blacklisting rules themselves are [epoch-validators-staking](epoch-validators-staking.md); the V1-vs-V2 partial-witness envelope switch is [stateless-validation](stateless-validation.md).
- **Optimistic block production** — the feature enum is `_DeprecatedProduceOptimisticBlock`, activated at **version 77** (`version.rs:596`) and always on at v88. Enables the optimistic-block path (`preprocess_optimistic_block`, `chain.rs:1303`); does not change finality. See [consensus-finality](consensus-finality.md).
- **`Spice`** — declared at `version.rs:354`, activation version **180** (`version.rs:656`), **not enabled** in the stable v88 build (`NIGHTLY_PROTOCOL_VERSION` is 157). **Everything in this bullet is nightly-only and not stable behavior.** When enabled it changes preprocess substantially: chunk-endorsement-in-block validation is skipped (`chain.rs:2585`), incoming-receipt collection is deferred (`chain.rs:2594`), certified-execution-result gathering feeds the gas-price/total-supply checks (`chain.rs:2530`, `chain.rs:2561`), a non-SPICE block in a SPICE epoch (and vice versa) is rejected with `Error::Other` (`chain.rs:2604`, `chain.rs:2618`), the header's `chunk_execution_root` must match the root recomputed from the body's core statements (`validate.rs:235` — `validate_spice_chunk_execution_root`), and core-statement / endorsement-stats validation is added (`chain.rs:2614`). In `validate_header`, header SPICE-ness must equal the epoch's (`chain.rs:929`) and a SPICE epoch uses `validate_spice_chunk_endorsements_in_header` (`chain.rs:1054`). `postprocess_block` records uncertified chunks and spice endorsement stats (`chain_update.rs:299`), and `update_final_head_from_block` records chunk-certifying blocks (`chain_update.rs:393`). GC gains a `SpiceInvalidChunks` sweep keyed by height (`garbage_collection.rs:814` — `gc_spice_invalid_chunks_at_height`) and gates witness GC on the final head's SPICE-ness rather than the binary's protocol version (`garbage_collection.rs:397`). Optimistic block processing is a no-op under SPICE (`chain.rs:1386`).
- **`EnforcePerReceiptStorageProofLimit`** (v86, `version.rs:466`/`version.rs:632`) and **`EnforceStorageProofLimitForAllActions`** (v87, `version.rs:473`/`version.rs:638`) are [runtime-execution](runtime-execution.md) storage-proof-limit changes, **not** chain-pipeline changes.
- **No v88 feature changes the block-processing pipeline.** The v88 features — `UniversalAccounts` (`version.rs:491`/`version.rs:643`, **moved from 87 to 88 in this release**), `RefundRepeatedUniversalStateInitDeposit` and `CanonicalUniversalStateInit` — are account/action-validation changes; see [accounts-keys](accounts-keys.md) and [runtime-execution](runtime-execution.md). The other v87 features (`FixContractLoadingError`, `RejectEmptyMethodName`, `RejectDelegateV2`, `RejectWithdrawFromGasKeyInDelegate`, `RemoveGasRewards`, `GlobalContractSameChunkCallFix`, `RejectUniversalAccountTransfers`) are likewise runtime/action-validation changes.
- **De-stabilized in 2.14.0 final:** `ReceiptPromiseInputSizeLimit` (`version.rs:450`) and `FixMlDsaCostCharging` (`version.rs:487`) both moved from 87 to **nightly 157** (`version.rs:652`, `version.rs:653`) and are therefore **not active** on the stable v88 path. Neither touches this component, but 2.14.0-rc.1 documentation (and the `2.14.0` CHANGELOG entry for ML-DSA) lists them as stable; the code is authoritative.

The Doomslug finality rule itself is **not** version-gated at v88; see [consensus-finality](consensus-finality.md).

## Invariants & failure modes

- **Atomic commit.** A block either commits all of its writes via one `StoreUpdate::commit` (`store/mod.rs:2090`) or none — "if rejected nothing will be updated in underlying storage" (`chain_update.rs:41`). A crash mid-processing leaves the store consistent.
- **Nothing is stored before it is verified.** Every path that writes a block — the pipeline (`chain.rs:1709`), the orphan pool, and the state-sync admission path (`client_actor.rs:2060`, gate at `client_actor.rs:2137`) — runs `verify_block_hash_and_signature` and then `validate_block` first. The state-sync path is the one that violated this before 2.14.0 final (#16418). `Chain::save_block` (`chain.rs:722`) itself performs **no** validation — it is a bare `DBCol::Block` write — and must not be called on an unverified block.
- **Bounded in-flight blocks.** At most `MAX_PROCESSING_BLOCKS = 5` blocks may be between preprocess and postprocess; excess → `TooManyProcessingBlocks` (`block_processing_utils.rs:16`, `block_processing_utils.rs:70`, `chain.rs:2432`).
- **Postprocess pool invariant.** A block reaching `postprocess_ready_block` must be in `blocks_in_processing`, else panic (`chain.rs:1940`).
- **No state below tail.** Blocks with `prev_height < gc_stop_height` are rejected as old forks (`chain.rs:2493`); orphans below tail are not stored (`chain.rs:1735`).
- **Highest-height fork choice.** Head advances only when `header.height() > head.height` (`chain_update.rs:445`); equal-height forks never overtake the head.
- **Finalizability.** Blocks that cannot be reached from the final head are dropped with `CannotBeFinalized`; the walk is bounded to 20 parents and returns `Ok` past that to avoid long delays (`chain.rs:857`).
- **Header-version agreement.** From protocol version 87 a `BlockHeaderV7` is invalid in any epoch (Spice is not reachable below 180), so a header that deserializes as V7 is rejected rather than silently read as V6 (`chain.rs:941`).
- **Byzantine assertions.** Block-body, gas-price, and total-supply *mismatches* fire `byzantine_assert!` (debug-fatal) before returning the corresponding `Error` (`chain.rs:2514`, `chain.rs:2548`, `chain.rs:2570`); chunk-signature/body failures likewise in `validate_block_impl` (`chain.rs:829`), and both state-sync admission failures (`client_actor.rs:2069`, `client_actor.rs:2085`).
- **No panic on arithmetic overflow in block-level checks.** Overflow in the gas-price or total-supply computation returns `Error::Other`, not a panic (`chain.rs:2552`, `chain.rs:2574`). Overflow on `prev_height + 1` in ancestry-free approval verification returns `Error::InvalidBlockHeight` (`approval_verification.rs:57`); overflow in early transaction preparation's gas math skips the optimization (`chain.rs:3795`).
- **Client protocol-version guard.** If, on an epoch boundary, the network's protocol version exceeds the binary's `PROTOCOL_VERSION`, `check_protocol_version` panics rather than producing invalid blocks (`chain_update.rs:67`). It is called only after a new head is set, before `commit` (`chain.rs:1914`).
- **State-sync divergence is fatal by design.** If the data reconstructed by state sync disagrees with the sync block's chunk commitment, the node panics with an operator-facing message rather than continuing on a divergent state (`chain.rs:2872`).
- **Bounded monitoring state.** `BlocksDelayTracker` refuses blocks outside `[head - 50, head + BLOCK_HORIZON)` and beyond 8 per height, labelling the reason (`below_window` / `above_window` / `height_full`) for metrics (`blocks_delay_tracker.rs:197`). The window is advanced both by `finish_block_processing` and, after state sync, by an explicit `update_head` (`blocks_delay_tracker.rs:380`, `chain.rs:1673`).
- **GC contract.** Genesis is never GC'd; the tail is always on the canonical chain and only one tail exists; the oldest fork branch point is never affected (`garbage_collection.rs:69`). `gc_stop_height > head.height` and a canonical block with refcount 0 are both `GCError`s (`garbage_collection.rs:180`, `garbage_collection.rs:277`).
- **GC must outpace block production.** A config where GC cannot keep up is rejected at startup (`nearcore/src/config_validate.rs:148`).
- **Block-known dedup.** Every attempted block hash/height is recorded as processed even on failure, so unrequested duplicates are filtered (`chain.rs:1294`, `chain.rs:1295`).

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `chain/chain/src/chain.rs:274` | `Chain` | pipeline state: pools, store, spawners, delay tracker |
| `chain/chain/src/chain.rs:358` | `VerifyBlockHashAndSignatureResult` | tri-state of the body-hash + signature gate; `CannotVerifyBecauseBlockIsOrphan` is never constructed |
| `chain/chain/src/chain.rs:722` | `Chain::save_block` | direct, unvalidated write of a block body (state-sync path only) |
| `chain/chain/src/chain.rs:1268` | `Chain::start_process_block_async` | pipeline entry; records processed hash/height |
| `chain/chain/src/chain.rs:1690` | `start_process_block_impl` | hash/sig gate, preprocess, error routing, schedule apply |
| `chain/chain/src/chain.rs:1083` | `verify_block_hash_and_signature` | body-hash + header-signature gate |
| `chain/chain/src/chain.rs:2422` | `preprocess_block` | full validation gate, builds apply jobs (no writes) |
| `chain/chain/src/chain.rs:2552` | `preprocess_block` gas-price `None` arm | overflow → `Error::Other`, not a panic |
| `chain/chain/src/chain.rs:2656` | `get_catchup_and_state_sync_infos` | catch-up / orphan-at-epoch-boundary decision |
| `chain/chain/src/chain.rs:885` | `validate_header` | header validity, approvals, finality fields, ordinal/epoch-sync hash |
| `chain/chain/src/chain.rs:929` | `validate_header` spice check | header SPICE-ness must equal the epoch's (both directions) |
| `chain/chain/src/chain.rs:941` | `validate_header` V7 check | **new at v88 release:** reject `BlockHeaderV7` from PV 87 |
| `chain/chain/src/chain.rs:765` | `Chain::validate_block` | `MaybeValidated` wrapper; sets the flag only on success |
| `chain/chain/src/chain.rs:778` | `validate_block_impl` | per-chunk + body validation |
| `chain/chain/src/chain.rs:1117` | `validate_chunk_headers` | new vs old chunk header consistency + block proposals |
| `chain/chain/src/chain.rs:1150` | `ping_missing_chunks` | detect locally-missing chunks → `ChunksMissing` |
| `chain/chain/src/chain.rs:857` | `check_if_finalizable` | drop non-finalizable blocks (20-parent bound) |
| `chain/chain/src/chain.rs:1834` | `schedule_apply_chunks` | spawn per-shard apply jobs, send results back |
| `chain/chain/src/chain.rs:1482` | `postprocess_ready_blocks` | drain finished applies |
| `chain/chain/src/chain.rs:1930` | `postprocess_ready_block` | commit, storage maintenance, orphan check, status |
| `chain/chain/src/chain.rs:1893` | `postprocess_block_only` | `ChainUpdate::postprocess_block` + version check + commit |
| `chain/chain/src/chain.rs:1602` | `determine_status` | Next / Fork / Reorg classification |
| `chain/chain/src/chain.rs:1303` | `preprocess_optimistic_block` | optimistic-block path |
| `chain/chain/src/chain.rs:1373` | `process_optimistic_block` | schedules optimistic apply; no-op under SPICE |
| `chain/chain/src/chain.rs:1632` | `reset_heads_post_state_sync` | head/tail reset after state sync; tells delay tracker |
| `chain/chain/src/chain.rs:2799` | `validate_state_sync_chunk_extra` | panics if synced state diverges from the chunk commitment |
| `chain/chain/src/chain.rs:3737` | `get_on_post_state_ready_callback` | early tx preparation hook; grandparent-anchored producer |
| `chain/chain/src/chain.rs:3795` | gas-overflow guard | skip early tx preparation instead of panicking |
| `chain/chain/src/chain.rs:4204` | `verify_header_approvals_without_ancestry` | >2/3-stake check without ancestry (sync decisions) |
| `chain/chain/src/chain.rs:3919` | `check_block_known` | dedup against processed/stored blocks |
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
| `chain/chain/src/validate.rs:100` | `validate_chunk_with_chunk_extra_and_roots` | chunk header vs prev chunk extra (tracked shards only) |
| `chain/chain/src/validate.rs:235` | `validate_spice_chunk_execution_root` | SPICE-only: header root vs body core statements |
| `chain/chain/src/orphan.rs:390` | `Chain::check_orphans` | unlock orphans whose parent arrived |
| `chain/chain/src/orphan.rs:285` | `Chain::save_orphan` | park a block in the orphan pool |
| `chain/chain/src/orphan.rs:458` | `Chain::get_orphan` | **new in 2.14.0:** read a parked orphan's body back; test-only, no production caller |
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
| `chain/chain/src/store/mod.rs:127` | `get_earliest_block_hash` | earliest servable block = `max(gc_stop_height, tail)` |
| `chain/chain/src/store/mod.rs:1761` | `update_gc_stop_height` | persist the last known gc stop height |
| `chain/chain/src/store/mod.rs:2090` | `ChainStoreUpdate::commit` | finalize → StoreUpdate::commit (atomicity) |
| `chain/chain/src/runtime/mod.rs:86` | `MAX_TXS_PER_GROUP_PER_VISIT` | 256-tx cap per signer group per visit |
| `chain/chain/src/runtime/mod.rs:992` | `prepare_transactions` per-group loop | `peek_next` loop, capped and time-checked |
| `chain/chain/src/runtime/mod.rs:1005` | `prepare_transactions` inner time check | wall-clock deadline enforced inside the group loop |
| `chain/chain/src/state_sync/utils.rs:374` | `get_extra_sync_block_hashes` | which extra blocks state sync must hold |
| `Cargo.toml:396` | `[profile.release]` | `overflow-checks = true`, `panic = 'abort'` — why an overflow aborts |
| `core/primitives/src/utils.rs:98` | `MaybeValidated::validate_with` | sets the validated flag only on `Ok` |
| `core/primitives/src/block_header.rs:807` | `BlockHeaderV8` | the Spice header since 2.14.0; V7 is now a dead variant |
| `core/primitives/src/block_header.rs:1989` | `BlockHeader::is_spice` | true only for V8 at 2.14.0 (was V7 at rc.1) |
| `core/store/src/columns.rs:784` | `DBCol::ChunkProducers` gc policy | `GcPolicy::Delete` — double-delete is idempotent |
| `chain/client/src/client.rs:1475` | `Client::start_process_block` | client-side entry into the pipeline |
| `chain/client/src/client.rs:1225` | `Client::note_verified_peer_height` | only production caller of the ancestry-free approval check |
| `chain/client/src/client_actor.rs:628` | `Handler<BlockResponse>` | diverts sync blocks before `receive_block` |
| `chain/client/src/client_actor.rs:2060` | `validate_state_sync_block` | **new in 2.14.0:** verify-before-store gate on the sync path |
| `chain/client/src/client_actor.rs:2104` | `maybe_receive_state_sync_blocks` | decide destination, then validate, then write |
| `chain/client/src/client_actor.rs:1506` | `try_process_unfinished_blocks` | drives postprocess loop |
| `chain/client/src/gc_actor.rs:51` | `GCActor::clear_data` | picks full GC vs legacy archival trimming |
| `nearcore/src/config_validate.rs:91` | `validate_gc_config` | reject GC config that cannot keep up |
| `test-loop-tests/src/tests/sync/adversarial_sync_block.rs` | `test_forged_sync_block_body_is_rejected` | asserts a forged sync-block body never reaches the orphan pool |
| `core/primitives-core/src/version.rs:505` | `RejectBlockHeaderV7BeforeSpice` | declared; activates **v87** (`version.rs:642`) |
| `core/primitives-core/src/version.rs:437` | `ValidateBlockOrdinalAndEpochSyncDataHash` | declared; activates v85 (`version.rs:625`) |
| `core/primitives-core/src/version.rs:355` | `ContinuousEpochSync` | declared; activates v85 (`version.rs:619`) |
| `core/primitives-core/src/version.rs:395` | `EarlyKickout` | declared; activates v87 (`version.rs:639`) |
| `core/primitives-core/src/version.rs:354` | `Spice` | declared; activation v180 (disabled at v88) |
| `core/primitives-core/src/version.rs:698` | `STABLE_PROTOCOL_VERSION` | 88 |

## Open questions

- A reorg performs no explicit "revert" of the previously-canonical state; canonicity is implicitly re-derived from the per-height index off the new (higher) head, and stale side-fork blocks linger until GC. The exact set of per-height / `NextBlockHash` index updates that re-point the canonical chain on a reorg lives in `ChainStoreUpdate::finalize` (`chain/chain/src/store/mod.rs:1781`) and was not traced line-by-line here.
- `reset_heads_post_state_sync` carries an acknowledged leak (TODO #16264, `chain/chain/src/chain.rs:1663`): the GC loops start at `tail`/`chunk_tail`, so the heights that a state sync skips over are never collected — blocks, chunks, partial chunks and their receipt refcounts leak. Whether this is bounded in practice (by how far a sync can jump) was not determined from code alone.
- `ContinuousEpochSync` is gated on the *binary's* `PROTOCOL_VERSION`, not the block's epoch version (`chain/chain/src/chain_update.rs:273`), unlike every other gate in this component. Whether that asymmetry is intentional (the proof is node-local, not consensus-critical) is not stated in the code.
- `RejectBlockHeaderV7BeforeSpice` is gated at 87 rather than applied unconditionally, so in epochs below 87 a 2.14.0 binary reads a `BlockHeaderV7` as a V6 header while a 2.13.x binary reads the same bytes as a Spice header (2.13's `is_spice()` returns true for V7, and its `validate_header` only checked the one-way "Spice epoch requires a Spice header" direction, `2.13.3:chain/chain/src/chain.rs:915`). Honest producers never emit V7 on a non-Spice network, so this should be unreachable, but the code does not state why the gated form was preferred over an unconditional rejection, and the divergence below 87 was not traced to a concrete outcome.
- The state-sync admission gate (`validate_state_sync_block`, owned by [sync](sync.md)) bans on `Incorrect` body-hash/signature but not on a `validate_block` failure, even when `err.is_bad_data()` — a peer that serves a well-signed block with a body that fails `check_validity` is only dropped. The code gives no reason for the asymmetry; it may be deliberate (a bad-data body cannot be produced with a valid `block_body_hash`, so this arm is expected to be unreachable) but that is not stated.
- `VerifyBlockHashAndSignatureResult::CannotVerifyBecauseBlockIsOrphan` is documented at `chain/chain/src/chain.rs:1081` as a possible return of `verify_block_hash_and_signature`, but the function body never constructs it (`chain.rs:1083`–`chain.rs:1113`): the signature check goes through `verify_block_header_signature_with_epoch_manager`, which returns `Err` rather than a "cannot verify" verdict. Whether the variant is vestigial or reserved for a planned orphan path is not determinable from the code.
