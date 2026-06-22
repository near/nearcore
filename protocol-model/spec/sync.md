# Sync (header / block / state / epoch + catchup)

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `chain/client/src/sync/{handler,header,block,epoch}.rs`, `chain/client/src/sync/state/{mod,shard,downloader,network,external,chain_requests}.rs`, `chain/client/src/state_request_actor.rs`, `chain/client/src/sync_jobs_actor.rs`, `nearcore/src/state_sync.rs`, `chain/client/src/client.rs` (`run_catchup`)

## Role
Sync brings a node that is behind (or freshly joined) up to the network tip, and catchup builds the state for shards a node will track in the next epoch while it stays live on its current shards. It sits between [networking-p2p](networking-p2p.md) (transport that delivers headers, blocks, and state responses) and [chain-block-processing](chain-block-processing.md) (which applies the blocks sync produces). Sync drives a single linear pipeline — epoch sync → header sync → state sync → block sync → done — selecting the entry point by how far behind the node is. State sync downloads a shard's state by parts; part *construction* and the trie/part format live in [state-storage](state-storage.md). Shard-tracking decisions come from [epoch-validators-staking](epoch-validators-staking.md).

## Key data structures

- **`SyncHandler`** — `chain/client/src/sync/handler.rs:17` — owns the four sub-syncers (`EpochSync`, `HeaderSync`, `StateSync`, `BlockSync`) plus the current `SyncStatus`. `handle_sync_needed` is the per-tick driver.
- **`SyncStatus`** (in `near_client_primitives::types`) — the state-machine enum: `AwaitingPeers`, `NoSync`, `EpochSync(EpochSyncStatus)`, `HeaderSync{start,current,highest}`, `StateSync(StateSyncStatus)`, `BlockSync{start,current,highest}`. Phase transitions mutate this field — `chain/client/src/sync/handler.rs:89`.
- **`EpochSyncStatus`** — `NotStarted` / `InProgress{source_peer_id, source_peer_height, attempt_time}` / `Done` — `chain/client-primitives/src/types.rs:114` (driven from `EpochSync::run` — `chain/client/src/sync/epoch.rs:128`).
- **`EpochSyncProofV1`** (`near_primitives::epoch_sync`) — `{all_epochs: Vec<EpochSyncProofEpochData>, last_epoch: EpochSyncProofLastEpochData, current_epoch: EpochSyncProofCurrentEpochData}`. One `EpochSyncProofEpochData` per epoch carries `block_producers`, `last_final_block_header`, `this_epoch_endorsements_for_last_final_block`, and `use_versioned_bp_hash_format`; verified in `verify_proof` — `chain/client/src/sync/epoch.rs:315`. Transmitted as `CompressedEpochSyncProof`.
- **`HeaderSync`** — `chain/client/src/sync/header.rs:32` — tracks one in-flight batch (`BatchProgress`), the `syncing_peer`, and stall/ban timing. `MAX_BLOCK_HEADERS = 512` headers per batch — `chain/client/src/sync/header.rs:15`.
- **`BlockSync`** — `chain/client/src/sync/block.rs:25` — `max_block_requests` per batch (config default 10), `BLOCK_REQUEST_TIMEOUT_MS = 2000` — `chain/client/src/sync/block.rs:14`.
- **`StateSync`** — `chain/client/src/sync/state/mod.rs:75` — orchestrates per-shard download tasks. Holds a `StateSyncDownloader` (preferred = peer source, optional fallback = external), two `TaskTracker` concurrency limiters (download, computation), and `shard_syncs: HashMap<(sync_hash, ShardId), StateSyncShardHandle>`.
- **`StateSyncShardHandle`** — `chain/client/src/sync/state/shard.rs:31` — `{status, result: oneshot::Receiver, cancel: CancellationToken}`; `Drop` cancels the task — `chain/client/src/sync/state/shard.rs:43`.
- **`ShardSyncStatus`** — per-shard stage: `StateDownloadHeader` → `StateDownloadParts{done,total}` → `StateApplyInProgress{done,total}` → `StateApplyFinalizing` → `StateSyncDone` — enum at `chain/client-primitives/src/types.rs:42`; set throughout `run_state_sync_for_shard` — `chain/client/src/sync/state/shard.rs:57`.
- **`StatePartKey(sync_hash, shard_id, part_id)`** — `core/primitives/src/state_sync.rs:23` — key into `DBCol::StateParts` (raw part bytes) and `DBCol::StatePartsApplied` (bool marker); used e.g. in `apply_state_part` — `chain/client/src/sync/state/shard.rs:330`.
- **`StateRequestActor`** — `chain/client/src/state_request_actor.rs:22` — serves `StateRequestHeader`/`StateRequestPart` to peers, with a sliding-window rate limiter (`throttle_state_sync_request`).

## Behavior

### 1. Entry decision (once per sync episode)
`handle_sync_needed` calls `decide_initial_phase` only when status is `NoSync`/`AwaitingPeers` — `chain/client/src/sync/handler.rs:85`. The decision is made once and not re-evaluated mid-pipeline. `horizon = epoch_sync_horizon_num_epochs * epoch_length` (default 2 epochs) — `chain/client/src/sync/handler.rs:190`. In order:
1. If `config.archive` **or** block head is within `horizon` of `highest_height` → enter **BlockSync** directly (archival nodes never epoch/state sync; near-horizon nodes catch up by header+block sync alone) — `chain/client/src/sync/handler.rs:196`.
2. Else if an epoch sync proof exists on disk **and** header head is within `horizon` (restart recovery after a mid-pipeline crash) → enter **HeaderSync**; previously downloaded `DBCol::StateParts` are preserved — `chain/client/src/sync/handler.rs:210`.
3. Else (far horizon) → enter **EpochSync(NotStarted)** — `chain/client/src/sync/handler.rs:226`.

The driver `handle_sync_needed` then dispatches on the current `SyncStatus` each tick and advances the pipeline — `chain/client/src/sync/handler.rs:89`.

### 2. Epoch sync
- **Request side**: `EpochSync::run` picks a random peer (from highest-height peers), sets status `InProgress`, and sends `NetworkRequests::EpochSyncRequest`. On `InProgress`, it retries only after `timeout_for_epoch_sync` elapses — `chain/client/src/sync/epoch.rs:128`.
- **Serve side** (`Handler<EpochSyncRequestMessage> for ClientActor`): when `ProtocolFeature::ContinuousEpochSync` is enabled (v85+), it returns the pre-computed compressed proof straight from `epoch_store.get_compressed_epoch_sync_proof()` — no on-the-fly derivation; the proof is extended by one epoch at each epoch boundary — `chain/client/src/sync/epoch.rs:559`. Otherwise it derives the proof on a background spawner via `derive_epoch_sync_proof` (cached by epoch id) — `chain/client/src/sync/epoch.rs:582`. The target epoch is T-2, chosen by `find_target_epoch_to_produce_proof_for` so ~2 epochs of headers exist for `transaction_validity_period`.
- **Receive + validate** (`Handler<EpochSyncResponseMessage>`): drops unsolicited responses (wrong/absent source peer) — `chain/client/src/sync/epoch.rs:605`. `validate_proof` rejects (silently, returns `Ok(false)`) proofs from the wrong peer, **too recent** (`first_block_height + max(epoch_length, tx_validity_period) >= source_peer_height`), or **too old** (`+ EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS(=3) * epoch_length < source_peer_height`) — `chain/client/src/sync/epoch.rs:169`.
- **Cryptographic verification** (`verify_proof`) — `chain/client/src/sync/epoch.rs:315`: requires `all_epochs.len() >= 2`; anchors `all_epochs[0]` to the second epoch after genesis via the local epoch manager; then for each subsequent epoch, by induction, verifies (a) block-producer handoff — `compute_bp_hash_from_validator_stakes(producers, use_versioned_bp_hash_format) == prev_epoch.last_final_block.next_bp_hash` (`verify_block_producer_handoff`, `chain/client/src/sync/epoch.rs:482`), (b) `epoch_id` chains via `next_epoch_id`, and (c) the epoch's last final block carries > 2/3 stake endorsement (`verify_block_endorsements`, `chain/client/src/sync/epoch.rs:504`). Finally `verify_epoch_sync_data_hash` checks the last epoch's data hash against the first-block header's `epoch_sync_data_hash`, and `verify_current_epoch_data` checks the Merkle proof + partial Merkle tree binding the current-epoch first block to the final block — `chain/client/src/sync/epoch.rs:394`. Attacker-controlled heights use `checked_add` to avoid overflow panics — `chain/client/src/sync/epoch.rs:522`.
- **Stale node**: if the proof validates but the node's header head is past genesis, it does not apply the proof — it sends `ShutdownReason::EpochSyncDataReset` to wipe the DB and restart fresh — `chain/client/src/sync/epoch.rs:651`.
- **Apply** (`apply_validated_proof`): stores the proof, writes the 4 boundary headers, sets header head to the current-epoch first block and final head to genesis, and calls `epoch_manager.init_after_epoch_sync` — `chain/client/src/sync/epoch.rs:226`. Status becomes `Done`. Next tick, the handler transitions `Done` → `HeaderSync` — `chain/client/src/sync/handler.rs:90`.

### 3. Header sync
Runs each tick in both the `HeaderSync` and `BlockSync` phases (banning enabled only in the primary HeaderSync phase) — `chain/client/src/sync/handler.rs:107`, `:161`. `HeaderSync::run` — `chain/client/src/sync/header.rs:109`:
1. If a request is in flight: declare the batch **complete** when `header_head.height >= min(header_head_height + MAX_BLOCK_HEADERS - 4, highest_height_of_peers)`; declare **stalling** when no progress past `expected_height` by `timeout`. On stall with `ban_stalling_peers` and a peer still claiming the top height, ban with `ReasonForBan::ProvidedNotEnoughHeaders` after `stall_ban_timeout` — `chain/client/src/sync/header.rs:128`, `:164`.
2. If idle: `start_header_batch` picks a random peer and sends `NetworkRequests::BlockHeadersRequest` with a *locator* — `chain/client/src/sync/header.rs:186`.

The locator (`get_locator`) walks back from the header head by block *ordinal* in exponentially growing steps (2,4,8,… capped at `MAX_BLOCK_HEADERS`), down to the final block, capped at `MAX_BLOCK_HEADER_HASHES` entries — `chain/client/src/sync/header.rs:265`, `get_locator_ordinals` `:298`. The remote returns up to 512 headers from the first locator hash on its canonical chain. After epoch sync, the node has few headers behind the tip, so missing ordinals below the tip are tolerated — `chain/client/src/sync/header.rs:284`.

The `HeaderSync` phase transitions to `StateSync` once `header_head.height >= highest_height - block_header_fetch_horizon` **and** `chain.find_sync_hash()` returns a sync hash — `chain/client/src/sync/handler.rs:115`. `find_sync_hash` resolves the sync hash for the header head's epoch (asserts it is not genesis) — `chain/chain/src/state_sync/utils.rs:284`.

### 4. State sync
`StateSync::run` per tick — `chain/client/src/sync/state/mod.rs:327`:
1. **Stale sync hash**: if `highest_height > sync_block.height + epoch_length + STALE_SYNC_HASH_THRESHOLD` (100, or 5 under `test_features`), the epoch's state parts are no longer servable → return `StaleSyncHash`, which the handler turns into `EpochSyncDataReset` (wipe + restart) — `chain/client/src/sync/state/mod.rs:341`, `STALE_SYNC_HASH_THRESHOLD` `:67`.
2. **Request sync blocks**: needs the sync-hash block (saved as an orphan), its prev block, and the extra blocks back to the last new chunk before the sync hash (`get_extra_sync_block_hashes`). Missing ones are returned as `NeedBlocks` so the client fetches them; re-requested after `block_request_timeout` — `chain/client/src/sync/state/mod.rs:278`, `:353`.
3. **Tracking shards**: `get_shards_cares_about_this_or_next_epoch` (handler path) — `chain/client/src/sync/state/mod.rs:359`.
4. **Per-shard download** (`run_with_shards`): for each tracked shard not already done, spawn `run_state_sync_for_shard` and poll its `oneshot` result; when all shards reach `StateSyncDone`, return `Completed` — `chain/client/src/sync/state/mod.rs:381`.
5. **Finalize**: on `Completed`, `chain.reset_heads_post_state_sync(sync_hash, …)` resets heads to the synced point and the handler advances to `BlockSync` — `chain/client/src/sync/state/mod.rs:364`, `chain/chain/src/chain.rs:1598`.

Per-shard task `run_state_sync_for_shard` — `chain/client/src/sync/state/shard.rs:57`:
1. **Header**: `downloader.ensure_shard_header` (cached on disk if present); yields `state_root = header.chunk_prev_state_root()` and `num_parts` — `chain/client/src/sync/state/shard.rs:77`.
2. **Download parts**: build `0..num_parts`, **shuffle** (so concurrent syncers don't hammer the same host in the same order), then download with bounded concurrency (`per_shard`), retrying only the failed parts after `min_delay_before_reattempt` until none remain — `chain/client/src/sync/state/shard.rs:93`-`155`.
3. **Apply parts**: clear flat storage and `StatePartsApplied` markers before applying (unless resuming a crash where parts were already applied and flat storage is gone), then apply all parts concurrently. Each `apply_state_part` skips parts already marked applied (idempotent restart), loads bytes from `DBCol::StateParts`, calls `runtime.apply_state_part`, and sets the `StatePartsApplied` marker — `chain/client/src/sync/state/shard.rs:158`-`227`, `apply_state_part` `:318`.
4. **Create flat storage + load memtrie** for the shard (skipping if already created / at genesis) — `chain/client/src/sync/state/shard.rs:230`-`258`.
5. **Finalize**: send `ChainFinalizationRequest` to the Chain thread (state validation/finalization must run where the Chain lives), then mark `StateSyncDone` — `chain/client/src/sync/state/shard.rs:263`.

**Where parts come from** (`StateSyncDownloader`) — `chain/client/src/sync/state/downloader.rs:28`: a `preferred_source` (peers) and optional `fallback_source` (external storage). The downloader caches on disk, validates before persisting, and retries. For headers it interleaves sources after `num_attempts_before_fallback` failures; for parts the source for a given attempt is chosen by `num_prior_attempts % (num_attempts_before_fallback + 1)` — `chain/client/src/sync/state/downloader.rs:71`, `:188`. A downloaded **part is validated** with `runtime.validate_state_part(state_root, PartId{idx,total}, part)` before being written to `DBCol::StateParts`; failed validation is treated as a download error — `chain/client/src/sync/state/downloader.rs:207`.
- **Peer source** (`StateSyncDownloadSourcePeer`) — `chain/client/src/sync/state/network.rs:31`: sends `StateRequestHeader`/`StateRequestPart` (keyed by `sync_prev_prev_hash`, the hash peers advertise snapshots by) and awaits a routed `StateResponse` via a `pending_requests` map; an `Ack(Busy|Error)` drops the wait, `Ack(WillRespond)` keeps waiting, `State(...)` completes it. Times out after `request_timeout` — `chain/client/src/sync/state/network.rs:61`, `:130`.
- **External source** (`StateSyncDownloadSourceExternal`) — `chain/client/src/sync/state/external.rs:20`: downloads header/part files from S3/GCS at `external_storage_location(chain_id, epoch_id, epoch_height, shard_id, file_type)`, parses, returns. Times out after `timeout` — `chain/client/src/sync/state/external.rs:69`.

### 5. Block sync
`BlockSync::run` issues a request only when `block_request_due` — head changed or `BLOCK_REQUEST_TIMEOUT_MS` (2 s) elapsed — `chain/client/src/sync/block.rs:200`, `:214`. `block_sync` finds the last processed block on the canonical chain (`get_last_processed_block`), then walks `get_next_block_hash` forward up to `max_block_requests` times, requesting each not-yet-known block (`NetworkRequests::BlockRequest`) from a random peer; for archival nodes requesting blocks below `gc_stop_height`, it prefers archival peers — `chain/client/src/sync/block.rs:100`. Header sync runs alongside, extending the `NextBlockHashes` chain that block sync follows; the loop self-paces because `get_next_block_hash` yields `DBNotFoundErr` when no headers are ahead — `chain/client/src/sync/handler.rs:155`. Sync exit (back to `NoSync`) is decided by the surrounding `run_sync_step` when the node is caught up (the handler comment at `chain/client/src/sync/handler.rs:159`).

### 6. Serving state-part requests
`StateRequestActor` handles `StateRequestHeader`/`StateRequestPart` — `chain/client/src/state_request_actor.rs:215`, `:260`:
1. **Throttle**: a sliding window drops the request if more than `num_state_requests_per_throttle_period` arrived within `throttle_period` — `chain/client/src/state_request_actor.rs:74`.
2. **Validate sync hash**: reject unless the sync hash's block is in the current or immediately-previous epoch (`is_sync_hash_from_known_recent_epoch`) **and** equals the node's own computed sync hash for that epoch (`get_sync_hash`) — `chain/client/src/state_request_actor.rs:140`. Rejected requests get no response.
3. **Build + respond**: build the `ShardStateSyncResponseHeaderV2` / part via `ChainStateSyncAdapter` and return a `StatePartOrHeader`; an empty response is returned when the header/part cannot be built — `chain/client/src/state_request_actor.rs:238`, `:282`.

### 7. Catchup
Driven by `Client::run_catchup`, scheduled ~every 100 ms by the ClientActor — `chain/client/src/client.rs:2519`. For each `(epoch_first_block, state_sync_info)` in `iterate_state_sync_infos()` (populated by block processing at the first block of an epoch when next-epoch shards differ), it derives the catchup `sync_hash`, lazily creates a `CatchupState{state_sync, sync_status, catchup}` (a `StateSync` built with `catchup = true`, which uses the catchup concurrency limits), and runs `state_sync.run_with_shards(status, state_sync_info.shards())` — `chain/client/src/client.rs:2535`, `:2569`. When the shards finish downloading, it steps block catchup (`catchup_blocks_step`) in BFS order over `DBCol::BlocksToCatchup`, and on completion calls `finish_catchup_blocks` — `chain/client/src/client.rs:2571`. The CPU-heavy chunk application is offloaded to `SyncJobsActor` (stateless, returns results to the ClientActor which holds the only chain-write access), one block at a time — `chain/client/src/sync_jobs_actor.rs:39`, `PendingShardJobs::run` `:65`. The three apply modes (`IsCaughtUp`, `CatchingUp`, `NotCaughtUp`) and how `process_block` orphans/forks the first block of an epoch are part of [chain-block-processing](chain-block-processing.md).

### 8. State sync dump (serving external storage)
`StateSyncDumper` (a node configured to dump) runs `state_sync_dump`, which at each epoch boundary computes the sync header, and for each tracked shard uploads the state header and all parts to external storage (GCS/S3), skipping parts/headers already present (possibly uploaded by other nodes) — `nearcore/src/state_sync.rs:948`, `check_head` `:884`, `upload_state_part` `:342`. Snapshot cadence may skip epochs whose `epoch_height` is not a multiple of the configured cadence — `nearcore/src/state_sync.rs:931`. Disabled under the `spice` feature — `nearcore/src/state_sync.rs:1016`.

## Interactions
- **Consumes** from [networking-p2p](networking-p2p.md): `BlockHeaders`, `Block`, `EpochSyncResponse`, and routed `StateResponse` messages; **produces** `BlockHeadersRequest`, `BlockRequest`, `EpochSyncRequest`, `StateRequestHeader`/`StateRequestPart`, and `BanPeer`.
- **Produces** for [chain-block-processing](chain-block-processing.md): downloaded blocks to apply, and `BlockProcessingArtifact` from `reset_heads_post_state_sync` / `finish_catchup_blocks`.
- **Reads** shard-tracking and epoch boundary data from [epoch-validators-staking](epoch-validators-staking.md) (`get_shards_cares_about_this_or_next_epoch`, `init_after_epoch_sync`, BP hashes).
- **Writes/reads** via [state-storage](state-storage.md): `DBCol::StateParts`, `DBCol::StatePartsApplied`, flat storage, memtrie, `DBCol::StateDlInfos`, `DBCol::BlocksToCatchup`. State-part *construction*, the part/header format, and snapshot management live there.
- **Consensus**: epoch sync proof endorsement checks reuse `Approval`/`ApprovalInner::Endorsement` from [consensus-finality](consensus-finality.md).

## Protocol-version-gated behavior
- **`ContinuousEpochSync`** — activates at **v85** — `core/primitives-core/src/version.rs:558`. When enabled, a node serving an epoch sync request returns the pre-computed compressed proof maintained incrementally per epoch, rather than deriving it on demand — `chain/client/src/sync/epoch.rs:561`.
- **`ValidateBlockOrdinalAndEpochSyncDataHash`** — activates at **v85** — `core/primitives-core/src/version.rs:564`. Recomputes `block_ordinal` and `epoch_sync_data_hash` against local chain state when validating received block headers (header validation lives in [chain-block-processing](chain-block-processing.md); the epoch sync proof's `verify_epoch_sync_data_hash` relies on the same `epoch_sync_data_hash` field — `chain/client/src/sync/epoch.rs:464`).
- **`_DeprecatedCurrentEpochStateSync`** — was **v74**, now deprecated — `core/primitives-core/src/version.rs:530`. It coordinated moving the sync hash from the epoch's first block to a few blocks later so the *current* epoch's state is synced; this behavior is now unconditional.
- No dedicated feature flag gates external/cloud state sync — it is selected purely by node config (`SyncConfig::ExternalStorage`) — `chain/client/src/sync/state/mod.rs:147`.
- Otherwise: None known at version 86.

## Invariants & failure modes
- **Archival nodes never epoch/state sync** — they must keep full history; entry always routes to BlockSync — `chain/client/src/sync/handler.rs:196`, `chain/client/src/sync/block.rs:33`.
- **Epoch sync horizon must not exceed GC retention**: `EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS == MIN_GC_NUM_EPOCHS_TO_KEEP == 3`, enforced by a `const` assert — `chain/client/src/sync/epoch.rs:43`. Otherwise a near-horizon node could need GC'd blocks.
- **Stale sync hash** (network moved >1 epoch past the sync point): state parts become unavailable → `StaleSyncHash` → data reset + restart — `chain/client/src/sync/state/mod.rs:341`.
- **Invalid epoch sync proof** → `Error::InvalidEpochSyncProof` and the proof is dropped without mutating the store (validation is side-effect free) — `chain/client/src/sync/epoch.rs:169`, `:315`. Endorsement < 2/3 stake, BP-hash mismatch, bad Merkle proof, or `< 2` epochs all reject.
- **State part validation**: a downloaded part failing `runtime.validate_state_part` against the `state_root` is rejected and retried, never persisted — `chain/client/src/sync/state/downloader.rs:207`.
- **State part application is idempotent**: `DBCol::StatePartsApplied` markers let a crashed sync resume without re-applying (`apply_state_part` skips marked parts — `chain/client/src/sync/state/shard.rs:330`); asserted by `shard::tests::test_apply_state` — `chain/client/src/sync/state/shard.rs:410`.
- **Serving requests is gated**: only sync hashes from the current/previous epoch that match the node's own computed sync hash are served; everything else (and over-rate requests) is silently dropped — `chain/client/src/state_request_actor.rs:140`, `:74`.
- **Unsolicited / wrong-peer state and epoch responses are dropped** — `chain/client/src/sync/state/network.rs:92`, `chain/client/src/sync/epoch.rs:183` (unexpected status in `validate_proof`)/`:186` (wrong peer)/`:609` (unsolicited pre-check in `Handler<EpochSyncResponseMessage>`).
- **Single-writer chain**: catchup offloads CPU work to the stateless `SyncJobsActor` and applies results only on the ClientActor thread, limiting catchup to one block per `run_catchup` tick (~100 ms) — `chain/client/src/sync_jobs_actor.rs:39`.

## Code anchors
| Location | Symbol | What happens here |
|---|---|---|
| `chain/client/src/sync/handler.rs:77` | `SyncHandler::handle_sync_needed` | Per-tick pipeline driver; dispatches on `SyncStatus` |
| `chain/client/src/sync/handler.rs:183` | `decide_initial_phase` | Picks entry phase (archive/near → block, restart → header, else epoch) |
| `chain/client/src/sync/handler.rs:115` | (HeaderSync arm) | HeaderSync → StateSync transition via `find_sync_hash` |
| `chain/client/src/sync/epoch.rs:128` | `EpochSync::run` | Sends/retries `EpochSyncRequest` to a random peer |
| `chain/client/src/sync/epoch.rs:169` | `validate_proof` | Freshness/peer checks, side-effect free |
| `chain/client/src/sync/epoch.rs:315` | `verify_proof` | Inductive BP-handoff + endorsement + data-hash verification |
| `chain/client/src/sync/epoch.rs:226` | `apply_validated_proof` | Writes boundary headers, sets heads, `init_after_epoch_sync` |
| `chain/client/src/sync/epoch.rs:559` | `Handler<EpochSyncRequestMessage>` | Serves proof (precomputed if ContinuousEpochSync, else derived) |
| `chain/client/src/sync/epoch.rs:651` | (stale-node check) | Validated proof + non-genesis head → data reset |
| `chain/client/src/sync/header.rs:109` | `HeaderSync::run` | Batch completion/stall logic, ban stalling peers |
| `chain/client/src/sync/header.rs:265` | `get_locator` / `get_locator_ordinals` | Exponential-step locator by block ordinal |
| `chain/client/src/sync/block.rs:100` | `BlockSync::block_sync` | Walk `get_next_block_hash` forward, request up to `max_block_requests` |
| `chain/client/src/sync/state/mod.rs:327` | `StateSync::run` | Stale check, sync-block requests, finalize + head reset |
| `chain/client/src/sync/state/mod.rs:381` | `run_with_shards` | Spawn/poll per-shard tasks (handler + catchup) |
| `chain/client/src/sync/state/mod.rs:67` | `STALE_SYNC_HASH_THRESHOLD` | 100 (5 under test_features) |
| `chain/client/src/sync/state/shard.rs:57` | `run_state_sync_for_shard` | Header → download parts (shuffled, concurrent) → apply → flat/memtrie → finalize |
| `chain/client/src/sync/state/shard.rs:318` | `apply_state_part` | Idempotent part apply with `StatePartsApplied` marker |
| `chain/client/src/sync/state/downloader.rs:156` | `ensure_shard_part_downloaded_single_attempt` | Source selection, validate against state_root, persist |
| `chain/client/src/sync/state/network.rs:61` | `receive_peer_message` | Match `StateResponse` to a pending request; Ack handling |
| `chain/client/src/sync/state/external.rs:69` | `StateSyncDownloadSourceExternal` | Download header/part files from S3/GCS |
| `chain/client/src/state_request_actor.rs:140` | `validate_sync_hash` | Epoch-recency + computed-sync-hash match check |
| `chain/client/src/state_request_actor.rs:74` | `throttle_state_sync_request` | Sliding-window rate limiter |
| `chain/client/src/client.rs:2519` | `Client::run_catchup` | Drive per-epoch catchup state sync + block catchup |
| `chain/client/src/sync_jobs_actor.rs:39` | `handle_block_catch_up_request` | Offload chunk-apply work, return results to ClientActor |
| `nearcore/src/state_sync.rs:948` | `state_sync_dump` | Upload epoch state headers/parts to external storage |
| `core/primitives-core/src/version.rs:558` | `ContinuousEpochSync` → 85 | Precomputed epoch sync proof serving |
| `core/primitives-core/src/version.rs:564` | `ValidateBlockOrdinalAndEpochSyncDataHash` → 85 | Recompute ordinal + epoch_sync_data_hash on header validation |

## Open questions
- The exact `NoSync` exit condition during the BlockSync phase ("AlreadyCaughtUp") is decided in the caller `run_sync_step` (referenced in `handler.rs:159`), which was not read; the handler itself never transitions out of BlockSync.
- `nomicon` doc `docs/architecture/how/sync.md` is broadly accurate but slightly stale on numbers: it states header sync continues "within ~50 blocks" of the tip, whereas the code uses `block_header_fetch_horizon` (config-driven) — `chain/client/src/sync/handler.rs:115`. It also describes the catchup `run_catchup`/`SyncJobsActor` interaction in an "Improvements" section that no longer matches the current `PendingShardJobs`-based scheduling.
