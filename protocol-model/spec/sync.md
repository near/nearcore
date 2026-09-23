# Sync (header / block / state / epoch + catchup)

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `chain/client/src/sync/{handler,header,block,epoch}.rs`, `chain/client/src/sync/state/{mod,shard,downloader,network,chain_requests,util,task_tracker}.rs`, `chain/client/src/verified_peer_heights.rs`, `chain/client/src/client_actor.rs`, `chain/client/src/state_request_actor.rs`, `chain/client/src/sync_jobs_actor.rs`, `chain/chain/src/state_sync/{adapter,utils}.rs`, `nearcore/src/state_sync.rs`

## Role

Sync is how a node that is behind (crashed, newly joined, or slow) catches its
block head up to the network tip, and catchup is how a node that will track new
shards next epoch builds their state while staying live on its current shards.
`ClientActor::run_sync_step` (`chain/client/src/client_actor.rs:1943`) decides
whether sync is needed and what height to aim for; `SyncHandler`
(`chain/client/src/sync/handler.rs:17`) then drives a single linear pipeline —
EpochSync → HeaderSync → StateSync → BlockSync → NoSync — each client tick. It
consumes headers/blocks/state parts from the network
([networking-p2p](networking-p2p.md)), state-part *construction* and the trie
part format live in [state-storage](state-storage.md), shard-tracking decisions
come from the epoch manager (see [epoch-validators-staking](epoch-validators-staking.md)),
and its output feeds block processing (see [chain-block-processing](chain-block-processing.md)).

**As of 2.14.0, state sync is peer-to-peer only.** Centralized
(external-storage / S3 / GCS) state sync was deleted in `chore(state-sync):
remove centralized (external-storage) state sync (#16009)`: the external
download source, the peers→external fallback, `SyncConfig::ExternalStorage`,
`ExternalStorageConfig`, `StateSyncConfig::gcs_with_bucket`,
`state_sync_external_backoff`, the `--state-sync-bucket` CLI flag, and the
`state-parts-dump-check` tool are all gone. This is a **breaking config
change**: a `config.json` that still sets
`"state_sync": {"sync": {"ExternalStorage": …}}` no longer deserializes and the
node fails to start — `SyncConfig` has no such variant to parse into.
`SyncConfig` now has a single variant, `Peers`
(`core/chain-configs/src/client_config.rs:328`), and
`chain/client/src/sync/state/external.rs` — the external download source — is
gone. The unrelated `chain/client/src/sync/external.rs` survives: it holds the
bucket-layout helpers (`StateSyncConnection`, `external_storage_location`) used
by the dumper and by `tools/state-viewer`, whose
`download_and_apply_state_parts_sequentially` (`external.rs:274`) is documented
as "a simple helper for tests and CLI tools", not a node path. The state *dump*
to external storage survives (`nearcore/src/state_sync.rs:56`) but only because
cloud archival uses it — no nearcore node consumes dumped parts for state sync
any more.

## Key data structures

- **`SyncHandler`** — `chain/client/src/sync/handler.rs:17` — owns `sync_status:
  SyncStatus` plus the four phase drivers (`EpochSync`, `HeaderSync`,
  `StateSync`, `BlockSync`). One instance per client.
- **`SyncStatus`** — `chain/client-primitives/src/types.rs:130` (variants:
  `AwaitingPeers`, `NoSync`, `EpochSync(EpochSyncStatus)`, `HeaderSync{..}`,
  `StateSync(StateSyncStatus)`, `BlockSync{..}`) — the phase state machine.
  `HeaderSync`/`BlockSync` carry `start_height/current_height/highest_height`.
- **`SyncRequirement`** — `chain/client/src/client_actor.rs:1004` —
  `SyncNeeded{source, highest_height, head} | AlreadyCaughtUp{..} | NoPeers |
  AdvHeaderSyncDisabled`. `HighestHeightSource`
  (`client_actor.rs:989`) is `Peer(PeerId)` or `OwnHeaderHead`.
- **`VerifiedPeerHeights`** — `chain/client/src/verified_peer_heights.rs:11` —
  highest block height each peer is *proved* to have reached, plus a 32-entry
  LRU of already-verified header hashes so one header costs one approval pass
  however many peers relay it.
- **`SyncHandlerRequest`** — `chain/client/src/sync/handler.rs:31` — what the
  handler asks the client to do out-of-band: `NeedRequestBlocks`,
  `NeedProcessBlockArtifact`, `EpochSyncDataReset` (data-wipe + restart).
- **`HeaderSync`** — `chain/client/src/sync/header.rs:32` — tracks the in-flight
  header batch (`BatchProgress` at `header.rs:20`), the syncing peer, and stall
  timeouts. `MAX_BLOCK_HEADERS = 512` (`header.rs:15`) headers per batch.
- **`BlockSync`** — `chain/client/src/sync/block.rs:25` — tracks the last block
  request; `BLOCK_REQUEST_TIMEOUT_MS = 2000` (`block.rs:14`); requests up to
  `max_block_requests` blocks per batch.
- **`EpochSync`** — `chain/client/src/sync/epoch.rs:52` — holds genesis header,
  config, and a cached compressed proof. `EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS =
  3` (`epoch.rs:44`, `const`-asserted equal to `MIN_GC_NUM_EPOCHS_TO_KEEP`).
- **`EpochSyncProofV1`** — `core/primitives/src/epoch_sync.rs:53` — `{all_epochs:
  Vec<EpochSyncProofEpochData>, last_epoch: EpochSyncProofLastEpochData,
  current_epoch: EpochSyncProofCurrentEpochData}`; transmitted compressed as
  `CompressedEpochSyncProof` (`epoch_sync.rs:81`).
- **`StateSync`** — `chain/client/src/sync/state/mod.rs:71` — one per handler and
  one per catchup sync-hash. Owns the `StateSyncDownloader`, task trackers,
  `shard_syncs: HashMap<(sync_hash, ShardId), StateSyncShardHandle>`, and the
  peer shared-state. `STALE_SYNC_HASH_THRESHOLD = 100` (`mod.rs:63`; `5` under
  `test_features`).
- **`StateSyncResult`** — `mod.rs:417` — `NeedBlocks | InProgress | Completed |
  StaleSyncHash`; **`StateSyncShardResult`** — `mod.rs:431` — `InProgress |
  Completed` (shard-download-only, used by catchup).
- **`ShardSyncStatus`** — `chain/client-primitives/src/types.rs:42` — per-shard
  phase: `StateDownloadHeader → StateDownloadParts{done,total} →
  StateApplyInProgress{done,total} → StateApplyFinalizing → StateSyncDone`.
  A sixth variant `StateApplyScheduling` (`types.rs:46`, metric repr `2`) is
  declared but never assigned by any code path — the only writers are in
  `shard.rs` and they skip it.
- **`StateSyncDownloadSource`** (trait) — `mod.rs:440` — abstracts a header/part
  source. It now has exactly **one** implementation, `StateSyncDownloadSourcePeer`
  (`network.rs:31`); `state/external.rs` was deleted.
- **`StatePartIndex`** — `core/primitives/src/state_part.rs:40` — type alias for
  `u64`, introduced by `refactor(state-sync): replace plain u64 part ids with an
  alias StatePartIndex (#16199)` so a part *index* is never confused with the
  `StatePartId{index,total}` pair (`state_part.rs:44`).
- **`StatePart`** — `core/primitives/src/state_part.rs:69` — `V0` (borsh trie
  nodes) or `V1` (zstd-compressed borsh). `V1` is what is produced today
  (`from_partial_state`, `state_part.rs:116`). Decoding enforces
  `PART_ENTRY_LIMIT` (`state_part.rs:36`) on both variants and, for `V1` only,
  `PART_SIZE_LIMIT` on the decompressed stream (`state_part.rs:17`).
- **`StatePartKey(sync_hash, shard_id, part_idx)`** —
  `core/primitives/src/state_sync.rs:23` — key for `DBCol::StateParts`
  (downloaded parts) and `DBCol::StatePartsApplied` (applied markers).
- **`StateRequestActor`** — `chain/client/src/state_request_actor.rs:22` — serves
  header/part requests from other nodes, with a sliding-window rate limiter.
- **`SyncConcurrency`** — `core/chain-configs/src/client_config.rs:343` —
  `{apply: 4, apply_during_catchup: 1, peer_downloads: 10, per_shard: 6}` by
  default (`client_config.rs:360`).

## Behavior

### 0. Is sync needed, and to what height? (`syncing_info`, `client_actor.rs:1714`)

New in 2.14: a peer's *claimed* height is no longer trusted on its own
(`feat(sync): verify target height block approvals when node is not lagging
(#16133)`).

1. `head_is_stale` (`client_actor.rs:1755`): true when
   `now - head_header.timestamp() >= min_block_production_delay *
   epoch_length` — i.e. the head has not advanced in ~1 epoch of wall clock.
   The timestamp is producer-signed, so a peer cannot forge this condition.
2. **Stale head** → `sync_requirement_from_claimed_peers`
   (`client_actor.rs:1769`): pick a random `highest_height_peers` entry whose
   `highest_block_hash` is not known-invalid and take its claimed height,
   clamped by `expected_shutdown`. This is the pre-2.14 behavior, kept because
   the node's known validator sets no longer cover the tip.
3. **Fresh head** → `sync_requirement_from_verified_peers`
   (`client_actor.rs:1793`): over `connected_peers`, each peer's height is
   `min(last_block.height, verified_height)`, where `verified_height` is
   `VerifiedPeerHeights::get_above(peer, head.height)`
   (`verified_peer_heights.rs:56`). A peer with nothing proved above our head
   reads as *level with our head* (`client_actor.rs:1826`) — still a download
   candidate, never a sync target. The max by `(height, claimed_height)` wins.
4. **Own header head fallback** (`client_actor.rs:1735`, from `fix(sync):
   continue syncing on our own header head (#16256)`): if the peer-derived
   answer is not `SyncNeeded` **and** the status is `BlockSync`, the node takes
   `min(header_head.height, expected_shutdown)` and, only if that exceeds
   `head.height` (`client_actor.rs:1742`), returns it as the target with source
   `OwnHeaderHead`. This path bypasses `peer_height_requires_sync`, so
   `sync_height_threshold` does not apply to it. Without this fallback, the head
   jump at the end of state sync leaves every verified height at or below the
   new head and block sync stalls.
5. `sync_requirement` (`client_actor.rs:1840`) calls
   `Client::peer_height_requires_sync` (`chain/client/src/client.rs:1209`):
   `peer_height > head_height + threshold`, where `threshold` is
   `sync_height_threshold` when not syncing and `0` while syncing.
6. `run_sync_step` (`client_actor.rs:1943`) maps the verdict: `AlreadyCaughtUp |
   NoPeers | AdvHeaderSyncDisabled` → set `SyncStatus::NoSync` (and announce the
   account) if we were syncing; `SyncNeeded` → `handle_sync_needed`. **This is
   the `NoSync` exit** for the whole pipeline, including `BlockSync`.

**Recording a verified height** (`perf(sync): verify peer heights only when they
can trigger sync (#16266)`): `Handler<BlockResponse>`
(`client_actor.rs:627`) calls `Client::note_verified_peer_height`
(`client.rs:1225`) only when `!was_requested` (`client_actor.rs:634`) — a block
we asked for proves nothing new. `note_verified_peer_height` prunes entries at
or below the head, skips heights that cannot trigger sync
(`peer_height_requires_sync`, `client.rs:1229`), and otherwise verifies via
`Chain::verify_header_approvals_without_ancestry`
(`chain/chain/src/chain.rs:4192`), which checks the **producer signature first**
(one signature, since the header hash covers the approvals) and only then runs
`verify_approvals_and_threshold_orphan` over the ~100 approvals against the
epoch's stake (`chain.rs:4210`). Headers lacking `prev_height` (V1/V2) are
rejected outright (`chain.rs:4203`).

### 1. Entry decision (once per sync start)

`handle_sync_needed` (`handler.rs:77`) calls `decide_initial_phase`
(`handler.rs:183`) only when `sync_status` is `NoSync` or `AwaitingPeers`
(`handler.rs:85`). The horizon is `epoch_sync_horizon_num_epochs *
epoch_length` (`handler.rs:190`). Checks in order:

1. **Archival OR near horizon** (`self.config.archive || head_within_horizon`,
   `handler.rs:196`): enter `BlockSync` directly. Archival nodes always take
   this path — epoch/state sync would leave gaps they cannot have.
2. **Restart recovery** (`handler.rs:210`): an epoch sync proof exists on disk
   (`get_epoch_sync_proof().is_some()`) and `header_head` is within the horizon
   → enter `HeaderSync` (headers already downloaded; previously downloaded state
   parts in `DBCol::StateParts` are preserved).
3. **Far horizon** (`handler.rs:226`): everything else → `EpochSync(NotStarted)`.

The decision uses **block head** for the horizon check, not header head
(`handler.rs:188-192`). It is not re-evaluated mid-pipeline.

### 2. Phase transitions (`handle_sync_needed`, `handler.rs:89`)

- `EpochSync(Done)` → `HeaderSync` seeded at the current header head
  (`handler.rs:90`).
- `EpochSync(_)` still in progress → `epoch_sync.run(..)` requests/awaits the
  proof (`handler.rs:100`).
- `HeaderSync` → runs header sync with peer-banning enabled
  (`ban_stalling_peers=true`, `handler.rs:107`). Once `header_head.height >=
  highest_height - block_header_fetch_horizon` (`handler.rs:117`) it calls
  `find_sync_hash()` and, if it returns a hash, transitions to
  `StateSync(StateSyncStatus::new(sync_hash))` (`handler.rs:121`).
- `StateSync` → `state_sync.run(..)`; the `StateSyncResult` maps to handler
  requests (`handler.rs:133`): `NeedBlocks`→`NeedRequestBlocks`;
  `Completed`→transition to `BlockSync` + `NeedProcessBlockArtifact`;
  `StaleSyncHash`→`EpochSyncDataReset`.
- `BlockSync` → runs both header sync (banning **disabled**, `handler.rs:161`)
  and block sync each tick. Exit to `NoSync` happens in `run_sync_step`
  (§0 step 6), not here.

### 3. Header sync (`header.rs:109`, `HeaderSync::run`)

Each tick checks two things in order:
1. If a request is in flight (`syncing_peer.is_some()`, `header.rs:120`):
   compute `batch_complete` (`header_head.height >= min(header_head_height +
   512 - 4, highest_height_of_peers)`, `header.rs:128`) and `stalling`
   (`header_head.height <= expected_height && now > timeout`, `header.rs:130`).
   On complete or stall, clear the peer; if stalling and banning is enabled and
   the stall exceeds `stall_ban_timeout`, ban the peer with
   `ProvidedNotEnoughHeaders` (`try_ban_stalling_peer`, `header.rs:164`). If
   making enough progress, extend the timeout (`header.rs:143`).
2. If idle, pick a random peer (`start_header_batch`, `header.rs:186`) and send
   `BlockHeadersRequest` with a locator (`request_headers`, `header.rs:239`).

The **locator** (`get_locator`, `header.rs:265`) is a list of block hashes
stepping back from the header-head ordinal to the final-head ordinal in
powers-of-2 steps (`get_locator_ordinals`, `header.rs:298`), capped at
`MAX_BLOCK_HEADER_HASHES` entries and `MAX_BLOCK_HEADERS`-sized steps. The peer
replies with up to 512 headers starting from the first locator hash on its
canonical chain (`header.rs:255-264` doc). Ordinals (not heights) are used
because they count blocks rather than heights, and stopping at the final block
is safe because consensus guarantees all nodes' final blocks are on the same
fork (`header.rs:262-264`). An ordinal lookup that misses is tolerated unless it
is the tip ordinal — normal right after epoch sync, when few headers precede the
tip (`header.rs:283-287`).

### 4. Block sync (`block.rs:200`, `BlockSync::run`)

A request is due when the head changed or `BLOCK_REQUEST_TIMEOUT_MS` elapsed
(`block_request_due`, `block.rs:214`). `block_sync` (`block.rs:100`) finds the
last processed canonical block (`get_last_processed_block`, `block.rs:51`: walk
back to a common ancestor, then forward while the next block exists), then walks
forward `max_block_requests` steps via `get_next_block_hash` (`block.rs:131`),
skipping already-known blocks (`check_block_known`, `block.rs:145`) and sending
`BlockRequest` to a random peer for each missing block (`block.rs:178`). For
blocks below the GC stop height an **archival** peer is preferred
(`request_from_archival`, `block.rs:156`). Header sync runs alongside block sync
each tick (`handler.rs:161`), extending the `NextBlockHashes` chain that block
sync follows; when no headers exist ahead, `get_next_block_hash` returns
`DBNotFoundErr` and block sync self-paces (`handler.rs:64` doc, `block.rs:134`).

### 5. Epoch sync (`epoch.rs`)

**Requesting** (`EpochSync::run`, `epoch.rs:129`): on `NotStarted` (or
`InProgress` past `timeout_for_epoch_sync`) pick a random peer, set
`InProgress{source_peer_id, source_peer_height, attempt_time}`, and send
`EpochSyncRequest` (`epoch.rs:159`).

**Serving** (`Handler<EpochSyncRequestMessage> for ClientActor`, `epoch.rs:577`):
when `ContinuousEpochSync` is enabled (PV 85; see gated-behavior), return the
pre-computed compressed proof from `epoch_store` directly (`epoch.rs:580`) — no
on-the-fly derivation. Otherwise, derive it on a spawned computation thread
(`derive_epoch_sync_proof`, `epoch.rs:84`), which targets a finalized recent
epoch via `find_target_epoch_to_produce_proof_for` and caches the result keyed
by `EpochId`. Either way the response is sent as a `NetworkRequestWithPermit`
carrying `msg.response_permit` (`epoch.rs:592`, `epoch.rs:616`), so the reply
is charged against the network layer's outgoing-memory semaphore
(see [networking-p2p](networking-p2p.md)).

**Receiving / validating** (`Handler<EpochSyncResponseMessage>`, `epoch.rs:629`):
1. Drop the response unless we are `InProgress` from exactly `msg.from_peer`
   (`epoch.rs:633`).
2. Decode, then `validate_proof` (`epoch.rs:170`): confirm the source peer,
   reject proofs that are too recent (target within
   `max(epoch_length, transaction_validity_period)` of the peer's height,
   `epoch.rs:191`) or too old (older than `3 * epoch_length`, `epoch.rs:205`),
   then `verify_proof`.
3. `verify_proof` (`epoch.rs:333`) checks, inductively per epoch: the genesis+2
   epoch's block producers match the local epoch info; each epoch's block
   producers hash to the previous epoch's `next_bp_hash`
   (`verify_block_producer_handoff`, `epoch.rs:500`); each epoch's last final
   block is endorsed by >2/3 stake (`verify_block_endorsements`, `epoch.rs:522`);
   the `epoch_sync_data_hash` matches the current epoch's first block header
   (`verify_epoch_sync_data_hash`, `epoch.rs:482`); and current-epoch merkle
   proofs / partial trees are well-formed (`verify_current_epoch_data`,
   `epoch.rs:412`).
4. **Stale-node check** (`epoch.rs:675`): if the proof is valid but the local
   `header_head.height != genesis_height`, the node has incompatible stale data;
   send `ShutdownReason::EpochSyncDataReset` and return (no proof applied).
5. Otherwise `apply_validated_proof` (`epoch.rs:227`): store the proof, write the
   4 boundary headers, set header head to the current-epoch first block and final
   head to genesis, `init_after_epoch_sync` on the epoch manager
   (`epoch.rs:265`), and then — new in 2.14 — seed two columns that
   `record_block_info` would normally write but epoch sync bypasses:
   - `seed_chunk_producers_after_epoch_sync` (`epoch.rs:300`) writes the
     `ChunkProducers` rows for the synced epoch's first block, which is the
     grandparent anchor for chunks at epoch-start + 2.
   - `set_epoch_start(epoch_id, height)` (`epoch.rs:310`), from
     `fix(early-kickout): write EpochStart on epoch sync (#16125)`. Readers that
     key on `EpochStart` (`get_validator_info`, `compare_epoch_id`, the
     epoch-sync-proof migration, `find_target_epoch_to_produce_proof_for`)
     would error on the synced epoch without it.

   Finally sets status `EpochSync(Done)`.

**Migration note**: the DB 48→49 migration regenerates the epoch sync proof
anchored at the *current* epoch (head−2) rather than at
`find_target_epoch_to_produce_proof_for`'s anchor, falling back to head−1
mid-epoch (`nearcore/src/migrations.rs`, `update_epoch_sync_proof`). The old
anchor reached ~4 epochs back and crashed nodes running
`gc_num_epochs_to_keep = 3` with "DB Not Found Error: epoch block" (`fix: build
48->49 epoch sync proof within the gc window (#15978)`).

### 6. State sync — handler path (`StateSync::run`, `mod.rs:269`)

1. Read the sync-hash block header. If `highest_height > sync_block_height +
   epoch_length + STALE_SYNC_HASH_THRESHOLD` (`mod.rs:283`), return
   `StaleSyncHash` (network moved past the epoch; parts unavailable).
2. `request_sync_blocks` (`mod.rs:220`): the sync-hash block, its prev, and the
   `get_extra_sync_block_hashes` (`chain/chain/src/state_sync/utils.rs:374`)
   must all be present; missing ones are returned as `NeedBlocks`
   (`mod.rs:297`). The sync hash block is stored as an orphan, so its presence
   is checked via `is_orphan` (`sync_block_status`, `mod.rs:188`).
3. `get_shards_cares_about_this_or_next_epoch` determines tracked shards
   (`mod.rs:301`), then `run_with_shards`.
4. On all-shards-`Completed`: `reset_heads_post_state_sync`
   (`chain/chain/src/chain.rs:1620`) sets body head to the sync-hash prev block,
   final head to genesis, adjusts tail/chunk-tail to the earliest downloaded
   sync block, tells `blocks_delay_tracker` about the head jump
   (`chain.rs:1661`), and unlocks orphans. Returns `Completed(artifacts)`.

### 7. State sync — per-shard download (`run_with_shards`, `mod.rs:323`; `run_state_sync_for_shard`, `shard.rs:56`)

For each tracked shard `run_with_shards` polls or spawns a
`StateSyncShardHandle`. A shard already marked `StateSyncDone` is skipped
(`mod.rs:354`). The per-shard future does:

1. **Header** (`shard.rs:75`): `ensure_shard_header` — return the on-disk header
   if present, else download + validate with unbounded retries backing off by
   `retry_backoff` (`downloader.rs:44`). `num_state_parts` comes from the
   header. Every 30th consecutive failure logs a Tier3-connectivity diagnostic
   (`downloader.rs:99`) — with external storage gone, an unreachable node has
   no fallback source.
2. **Download parts** (`shard.rs:102`): shuffle part indices (`shard.rs:100`, so
   concurrent nodes don't hammer the same hosts in the same order), then
   download with `concurrency_limit` (`SyncConcurrency::per_shard`) in flight
   using **`buffer_unordered`** (`shard.rs:124`). This is the change from
   `perf(state-sync): download parts unordered to avoid head-of-line blocking
   (#16299)`: `buffered` holds a completed future in its slot until all earlier
   ones yield, so a single part stuck to the p2p timeout collapsed the shard's
   parallelism to one. Download order carries no meaning, so the part index is
   carried in the error arm only (`shard.rs:122`). Each round retains just the
   failed indices and repeats (`shard.rs:140`); the per-attempt
   `retry_backoff` sleep already lives inside the downloader
   (`downloader.rs:198`), so no extra delay is added here.
   Each part is validated against the state root (`validate_state_part_impl`,
   `chain/chain/src/runtime/mod.rs:541`; trait entry `validate_state_part`,
   `runtime/mod.rs:1517`) before being written to
   `DBCol::StateParts` (`downloader.rs:183`).
3. **Apply parts** (`shard.rs:145`): unload memtrie, then decide whether to keep
   the existing `StatePartsApplied` markers via `keep_applied_state_parts`
   (`shard.rs:256`) — markers are kept **only while an apply is unfinished**,
   i.e. `apply_parts_started && flat_storage_status != Ready`. This is
   `fix(state-sync): re-apply state parts after a finished apply (#16306)`: a
   `Ready` status means an earlier sync applied every part and then finalized,
   which applies a chunk on top and refcount-deletes base nodes the new root
   does not share, so every part must be applied again. Otherwise flat storage
   is cleared and all `StatePartsApplied` markers are deleted
   (`shard.rs:149-166`). Then `apply_state_part` runs for each index
   (`shard.rs:316`), idempotent via the markers (`shard.rs:330`), again with
   `buffer_unordered(concurrency_limit)` (`shard.rs:191`).
4. **Finalize** (`shard.rs:201`): create flat storage for the shard
   (`create_flat_storage_for_shard`, `shard.rs:274`), load memtrie
   (`load_memtrie_on_catchup`, `shard.rs:224`), then send a
   `ChainFinalizationRequest` to the Chain thread (`shard.rs:236`) and mark
   `StateSyncDone`.

**Finalization on the Chain thread** (`Chain::set_state_finalize`,
`chain/chain/src/chain.rs:2742`) replays heights from the header's
`chunk_height_included` up to the sync hash, then — new in 2.14 —
`validate_state_sync_chunk_extra` (`chain.rs:2787`) compares the reconstructed
`ChunkExtra` at the sync block's prev hash against the sync block's own new
chunk header using `validate_chunk_with_chunk_extra` (`chain.rs:2839`). A
repeated (missing) chunk is skipped because it commits no boundary post-state
(`chain.rs:2810`). A commitment mismatch (`InvalidStateRoot`,
`InvalidOutcomesProof`, `InvalidValidatorProposals`, `InvalidGasLimit`,
`InvalidGasUsed`, `InvalidBalanceBurnt`, `InvalidReceiptsProof`,
`InvalidCongestionInfo`, `InvalidBandwidthRequests`,
`InvalidChunkHeaderShardSplit`) **panics** with an operator-facing message
(`chain.rs:2860`) rather than letting the divergence surface later as a
"bad canonical chunk" during block sync (`fix(state-sync): validate finalized
chunk commitments (#16152)`).

### 8. State-sync source — peers only (`downloader.rs`, `network.rs`)

`StateSyncDownloader` (`downloader.rs:27`) holds a single
`source: Arc<dyn StateSyncDownloadSource>`, constructed from
`StateSyncDownloadSourcePeer` (`mod.rs:126`). There is no
`preferred_source`/`fallback_source` split and no `num_attempts_before_fallback`
cycling any more — both were removed with #16009.

- **Peer source** (`StateSyncDownloadSourcePeer::try_download`, `network.rs:130`):
  computes `sync_prev_prev_hash` (peers advertise snapshots by it,
  `network.rs:145`), sends `StateRequestHeader`/`StateRequestPart`
  (`network.rs:161`), registers a pending request keyed by `(shard_id,
  sync_hash, part_or_header)` (`network.rs:227`), and awaits the peer's
  `StateResponse` (delivered via `apply_peer_message` (`mod.rs:172`) →
  `receive_peer_message`, `network.rs:61`) with a timeout. An `Ack(Busy|Error)`
  drops the pending request (`network.rs:106`); `Ack(WillRespond)` is a no-op.
  Routing failures (`NoDestinationsAvailable`, `RouteNotFound`,
  `MyPublicAddrNotKnown`, `NoResponse`) each become a distinct
  `STATE_SYNC_DOWNLOAD_RESULT` label (`network.rs:200-215`).
- A `RemoveKeyUponDrop` guard (`network.rs:259`) clears the pending-request
  entry on any exit path.

### 9. Serving state requests (`StateRequestActor`, `state_request_actor.rs`)

`Handler<StateRequestHeader>` (`state_request_actor.rs:215`) and
`<StateRequestPart>` (`:260`): first `throttle_state_sync_request`
(sliding-window limiter, `:74`) drops the request if the rate exceeds
`num_state_requests_per_throttle_period`; then `validate_sync_hash` (`:140`)
rejects sync hashes not from the current or immediately-previous epoch
(`is_sync_hash_from_known_recent_epoch`, `:107`) or that don't match the node's
own computed sync hash (`:165`). Valid requests are answered from
`ChainStateSyncAdapter::get_state_response_header` / `get_state_response_part`
(`:238`, `:282`), always as a V2 response (`new_header_response`, `:177`).
A rejected request returns `None` (no response at all); a *failed build*
returns an empty V2 response (`:244`, `:286`).

**Ingest-side proof validation** (`ChainStateSyncAdapter::get_state_header`,
`chain/chain/src/state_sync/adapter.rs:367`) checks the downloaded header's
incoming-receipt proofs step by step: continuous block sequence (`:460`),
`receipt_proofs.len() == root_proofs[i].len() == block.chunks_included()`
(`:473`), uniqueness of `from_shard_id` (`:489`), the receipt merkle path
(`:499`), and — new in 2.14 — that `from_shard_id` actually names a chunk this
block included and that the outgoing-receipts-root path verifies *at that
index* (`verify_path_with_index`, `:507-517`). No merkle root covers the
`from_shard_id` field, so the index check is what binds it; a shard with no new
chunk repeats an older root (`fix(state-sync): bind from_shard_id to receipt
proof merkle index (#16379)`).

### 10. State-part decoding limits

`StatePart::to_partial_state` (`state_part.rs:120`) enforces, before any trie
value is allocated:
- **byte size**: `PART_SIZE_LIMIT` — the zstd decoder is `take`-limited to
  `PART_SIZE_LIMIT + 1` and rejected past it (`state_part.rs:94`, `:105`).
- **entry count**: `PART_ENTRY_LIMIT = 2 * STATE_PART_MEMORY_LIMIT /
  MIN_MEMORY_USAGE_PER_PART_ENTRY` (`state_part.rs:36`), checked from the borsh
  length prefix alone by `PartialState::check_entry_limit`
  (`core/primitives/src/state.rs:50`), which errors with "state part entry
  limit exceeded". For the compressed variant the header is read and checked
  *before* the rest of the stream is decompressed (`state_part.rs:98-100`).
  From `fix(state-sync): bound the number of entries in a state part (#16388)`:
  a byte limit alone permits >100M four-byte entries, each an `Arc<[u8]>`
  heap allocation. `MIN_MEMORY_USAGE_PER_PART_ENTRY = 50`
  (`state_part.rs:24`) mirrors `TRIE_COSTS.node_cost`
  (`core/store/src/trie/mod.rs:158`).

### 11. State-sync dump (external storage upload, `nearcore/src/state_sync.rs`)

A node configured with `state_sync.dump` runs `StateSyncDumper::start`
(`state_sync.rs:56`), which spawns a loop (`state_sync_dump`, `:956`) that on
each tick detects new epochs (`check_head`, `:884`), re-checks which parts
already exist remotely (`check_stored_parts`, `:870`, every 20 s —
`CHECK_STORED_PARTS_INTERVAL`, `:952`), and drives
per-shard part uploads (`PartUploader::upload_state_part`, `:342`). Progress is
persisted in `STATE_SYNC_DUMP_KEY` DB entries (`:590`). Snapshot cadence can
skip epochs, but never a resharding epoch (`should_dump_epoch`, `:931`). Dump is
disabled under the `protocol_feature_spice` build (`:1025`).

**This is no longer a state-sync path.** No nearcore node downloads state from
external storage; the dump survives because cloud archival is built on the same
external-storage plumbing — `DumpConfig` / `ExternalStorageLocation`
(`core/chain-configs/src/client_config.rs:305`, `:173`) and
`near_external_storage::ExternalConnection`
(`core/store/src/archive/cloud_storage/opener.rs:44`). The dumper's own
`StateSyncConnection` wrapper (`chain/client/src/sync/external.rs:48`) is used
only by the dumper and by `tools/state-viewer`. Part indices
here are `StatePartIndex` (`state_sync.rs:21`), and the helper names were
renamed accordingly (`get_missing_part_indices_for_epoch`, `:136`;
`extract_part_idx_from_part_file_name`, `:131`).

### 12. Catchup (`Client::run_catchup`, `chain/client/src/client.rs:2551`)

`run_catchup` iterates `iterate_state_sync_infos()` (`client.rs:2557`) — the
shards a node must build state for before the next epoch (persisted in
`DBCol::StateDlInfos` when the first block of an epoch is processed). For each,
it derives the catchup sync hash (`get_catchup_sync_hash`, `client.rs:2564`),
gets-or-creates a **separate** `StateSync` (constructed with `catchup=true`,
`client.rs:2585`, which selects `apply_during_catchup` concurrency instead of
`apply`, `mod.rs:146`), and runs `run_with_shards` (shard-download only; no head
reset, `client.rs:2599`). Once shards `Completed`, it runs `catchup_blocks_step`
(applies already-processed blocks from `DBCol::BlocksToCatchup` for the
next-epoch shards) and, when finished, `finish_catchup_blocks`
(`client.rs:2612`). Heavy work (applying chunks for caught-up blocks) is
offloaded to `SyncJobsActor` (`sync_jobs_actor.rs:39`), which schedules the
shard jobs on `apply_chunks_spawner` via `PendingShardJobs::run`
(`sync_jobs_actor.rs:65`) and returns a `BlockCatchUpResponse` to the client;
the client alone writes to the chain store.

## Interactions

- **Consumes**: `HighestHeightPeerInfo`, `connected_peers` chain info, and
  network responses (headers, blocks, epoch proofs, state headers/parts) from
  [networking-p2p](networking-p2p.md); tracked-shard sets and epoch info from
  the epoch manager ([epoch-validators-staking](epoch-validators-staking.md)); the
  trie/state-part format and `apply_state_part`/`validate_state_part` from
  [state-storage](state-storage.md); approval/stake verification from consensus
  ([consensus-finality](consensus-finality.md)).
- **Produces**: `NetworkRequests::{BlockHeadersRequest, BlockRequest,
  EpochSyncRequest, EpochSyncResponse, StateRequestHeader, StateRequestPart,
  BanPeer}` to the network; validated state written to `DBCol::StateParts` /
  `DBCol::StatePartsApplied` and flat storage / memtrie; head/tail updates via
  [state-storage](state-storage.md); `BlockProcessingArtifact`s handed to block
  processing ([chain-block-processing](chain-block-processing.md)).
- **Serves**: state header/part requests to peers via `StateRequestActor`; epoch
  sync proofs via the `ClientActor` epoch-sync handlers; an optional
  external-storage dump used by **cloud archival**, not by state sync.
- **Requires state snapshots**: `chore: remove the option to disable state
  snapshots (#16120)` deleted `StoreConfig::state_snapshot_config` /
  `StateSnapshotType`; snapshots are always on for regular nodes because serving
  state parts to peers and cloud archival both need them
  (`NightshadeRuntime::from_config`, `nearcore/src/config.rs:940`). Offline
  tools opt out via `from_config_with_state_snapshot`
  (`nearcore/src/config.rs:953`). Old `config.json` files still load and the
  field is ignored — neither `StoreConfig` nor `Config` sets
  `serde(deny_unknown_fields)`.

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs` in this tree
(`STABLE_PROTOCOL_VERSION = 87`, `version.rs:680`;
`MIN_SUPPORTED_PROTOCOL_VERSION = 84`, `version.rs:652`).

**Almost nothing in this component is protocol-version gated.** Sync is
`neard` node behavior: peers negotiate it out-of-band of block validity, so
changing it does not fork the chain. Every 2.14 change listed in this spec —
external-storage removal, unordered part downloads, the state-part entry bound,
the receipt-proof index binding, finalized chunk-commitment validation, part
re-apply after a finished apply, verified peer heights, the own-header-head
fallback, `EpochStart` on epoch sync — is **ungated** and takes effect as soon
as the binary runs. The two gates below are the whole list.

| Feature | Activates | Effect on sync |
| --- | --- | --- |
| `ContinuousEpochSync` | PV **85** (`version.rs:355` decl, `:605` mapping) | Epoch sync proofs are maintained incrementally and stored compressed; the request handler returns the pre-computed proof directly (`epoch.rs:580`) instead of deriving it on demand. Checked with `PROTOCOL_VERSION` (the binary's max version), not per-block. |
| `ValidateBlockOrdinalAndEpochSyncDataHash` | PV **85** (`version.rs:437` decl, `:611` mapping) | Received block headers have their `block_ordinal` and `epoch_sync_data_hash` recomputed against local chain state during header validation (`Chain::validate_header`, `chain/chain/src/chain.rs:1027`). Gated on the *epoch's* protocol version, unlike `ContinuousEpochSync`. Strengthens the trust in headers pulled during header sync. |

Not gated at PV 87:
- **State-sync source selection** was a *node configuration* (`SyncConfig`) and
  is now not even a choice — `SyncConfig::Peers` is the only variant
  (`core/chain-configs/src/client_config.rs:328`).
- **Epoch-sync data-hash validation** *within the proof* is unconditional
  (`verify_epoch_sync_data_hash`, `epoch.rs:482`).
- `_DeprecatedCurrentEpochStateSync` (PV 74, `version.rs:279`/`:577`) and
  `_DeprecatedStatePartsCompression` (PV 82, `version.rs:337`/`:589`) are
  deprecated: "sync to current epoch's state" and compressed state parts are
  baseline, below `MIN_SUPPORTED_PROTOCOL_VERSION = 84`, so no branch remains.
- The PV-87 features (`FixContractLoadingError`, `RejectEmptyMethodName`,
  `RejectDelegateV2`, `RemoveGasRewards`, `EnforceStorageProofLimitForAllActions`,
  `UniversalAccounts`, `EarlyKickout`, …, `version.rs:619-630`) are runtime /
  validator-selection features; none change sync. `EarlyKickout` touches epoch
  sync only indirectly, via the `EpochStart`/`ChunkProducers` rows that
  `apply_validated_proof` now seeds (§5).
- **Config rename**: `state_sync_external_timeout` → `block_request_timeout`
  (`chore(state-sync): rename config option (#16128)`), with the old name kept
  as a serde alias. It is the state-sync *block* request timeout
  (`mod.rs:79`), never an external-storage timeout.

## Invariants & failure modes

- **Archival nodes never epoch/state sync** — enforced at the entry decision
  (`handler.rs:196`) and `BlockSync.archive` (`block.rs:34`); they would
  otherwise create unfixable history gaps.
- **Epoch sync horizon ≤ GC epochs** — `epoch_sync_horizon_num_epochs` (default
  2, `client_config.rs:414`) must not exceed `gc_num_epochs_to_keep` or a
  near-horizon node could need GC'd blocks. This relation holds by construction
  of the defaults — `GCConfig::gc_num_epochs_to_keep()` clamps up to
  `MIN_GC_NUM_EPOCHS_TO_KEEP = 3` (`client_config.rs:165`, `:33`), above the
  horizon default of 2 — but nothing validates it: `config_validate.rs` never
  compares the two (it only warns about a clamped GC value,
  `nearcore/src/config_validate.rs:110`), so raising the horizon in
  `config.json` is unchecked. The proof-age bound *is* enforced:
  `EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS` is `const`-asserted
  `== MIN_GC_NUM_EPOCHS_TO_KEEP == 3` (`epoch.rs:44`).
- **Epoch proof validity** — every proof is verified (BP handoff via
  `next_bp_hash`, >2/3 endorsement stake, epoch_sync_data_hash, merkle proofs)
  before any store write (`verify_proof`, `epoch.rs:333`); failure returns
  `Error::InvalidEpochSyncProof` and the response is dropped. Attacker-controlled
  heights use `checked_add` to avoid panics (`epoch.rs:540`, `:445`; asserted by
  the regression test `verify_block_endorsements_rejects_max_height`,
  `epoch.rs:705`).
- **A peer's claimed height is not a sync trigger while the head is fresh** —
  the target comes from `min(advertised, approval-verified)`
  (`client_actor.rs:1823`); only a head that has not advanced for ~1 epoch
  (producer-signed timestamps, which cannot be forged) re-admits raw claims
  (`head_is_stale`, `client_actor.rs:1755`).
- **Unsolicited / wrong-peer responses dropped** — epoch responses
  (`epoch.rs:633`), state responses (wrong sender → `Error::Other("Unexpected
  state response (wrong sender)")`, `network.rs:98`).
- **State part validity** — a part failing `validate_state_part` is not
  persisted and the attempt errors ("Part data failed validation",
  `downloader.rs:190`); the index is retried in the next round.
- **A state part cannot exhaust memory on decode** — byte limit and entry
  limit are both enforced pre-allocation (§10).
- **Receipt-proof indices are bound to the block's chunk mask** — a permuted
  `from_shard_id` label is rejected at header ingest
  (`adapter.rs:507-521`). No consensus effect (`collect_receipts` discards the
  `ShardProof`), but it corrupts a stored index.
- **Reconstructed state must match the chain's commitment** — a mismatch at
  finalization stops the process rather than corrupting the node
  (`validate_state_sync_chunk_extra`, `chain.rs:2860`).
- **Stale sync hash** — if the network advances more than
  `epoch_length + STALE_SYNC_HASH_THRESHOLD` past the sync hash, state sync
  returns `StaleSyncHash` → `EpochSyncDataReset` (`mod.rs:283`, `handler.rs:150`).
- **Idempotent apply / crash resume** — applied parts are marked in
  `DBCol::StatePartsApplied`, but the markers are honored **only while the
  apply is unfinished**; a `FlatStorageStatus::Ready` means a previous sync
  already finalized on top of those parts, so everything is re-applied
  (`keep_applied_state_parts`, `shard.rs:256`, asserted by
  `applied_parts_kept_only_while_apply_is_unfinished`, `shard.rs:409`).
- **Flat storage must not exist when it is created** — `assert!` in
  `create_flat_storage_for_shard` (`shard.rs:282`), because leftover keys
  corrupt it.
- **Serve-side rate limiting & epoch check** — requests are throttled
  (`state_request_actor.rs:74`) and rejected for unknown/mismatched sync hashes
  (`validate_sync_hash`, `:140`), returning `None` (no response).
- **Single writer** — catchup's heavy apply work runs on `SyncJobsActor`, which
  holds no chain write access; only the client applies results
  (`sync_jobs_actor.rs:65`, and `docs/architecture/how/sync.md` "How catchup
  works").
- **Known leak** — `reset_heads_post_state_sync` moves tail/chunk-tail forward,
  but the GC loops start at tail, so the skipped heights' blocks, chunks,
  partial chunks and receipt refcounts are never collected
  (`chain.rs:1651`, TODO #16264).

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `chain/client/src/client_actor.rs:1714` | `syncing_info` | Decides whether sync is needed and picks the target height |
| `chain/client/src/client_actor.rs:1755` | `head_is_stale` | ~1-epoch stale-head test using producer-signed timestamps |
| `chain/client/src/client_actor.rs:1769` | `sync_requirement_from_claimed_peers` | Unverified claimed heights (stale-head path) |
| `chain/client/src/client_actor.rs:1793` | `sync_requirement_from_verified_peers` | `min(advertised, approval-verified)` per peer |
| `chain/client/src/client_actor.rs:1735` | own-header-head fallback | Keeps block sync alive after the state-sync head jump (#16256) |
| `chain/client/src/client_actor.rs:1943` | `run_sync_step` | Enables/disables sync; the `NoSync` exit |
| `chain/client/src/client_actor.rs:634` | `Handler<BlockResponse>` | Records a verified peer height for unrequested blocks only |
| `chain/client/src/client.rs:1209` | `peer_height_requires_sync` | Single definition of the sync-trigger bar |
| `chain/client/src/client.rs:1225` | `note_verified_peer_height` | Prune, gate, then verify approvals |
| `chain/client/src/verified_peer_heights.rs:31` | `record_if_verified` | Per-header LRU so approvals are checked once |
| `chain/chain/src/chain.rs:4192` | `verify_header_approvals_without_ancestry` | Producer signature first, then >2/3 approval stake |
| `chain/client/src/sync/handler.rs:77` | `SyncHandler::handle_sync_needed` | Drives the linear phase pipeline each tick |
| `chain/client/src/sync/handler.rs:183` | `decide_initial_phase` | Chooses entry phase from archival/horizon/proof state |
| `chain/client/src/sync/header.rs:109` | `HeaderSync::run` | One tick of header batch download / stall handling |
| `chain/client/src/sync/header.rs:265` | `get_locator` | Builds powers-of-2 ordinal locator |
| `chain/client/src/sync/block.rs:100` | `BlockSync::block_sync` | Requests missing blocks forward from last processed |
| `chain/client/src/sync/block.rs:51` | `get_last_processed_block` | Finds canonical processed reference point |
| `chain/client/src/sync/epoch.rs:129` | `EpochSync::run` | Sends/retries epoch sync request |
| `chain/client/src/sync/epoch.rs:170` | `validate_proof` | Peer identity + freshness/staleness window |
| `chain/client/src/sync/epoch.rs:333` | `EpochSync::verify_proof` | Inductive proof verification |
| `chain/client/src/sync/epoch.rs:227` | `apply_validated_proof` | Writes proof, inits epoch manager, seeds ChunkProducers + EpochStart |
| `chain/client/src/sync/epoch.rs:577` | `Handler<EpochSyncRequestMessage>` | Serves proof (precomputed under ContinuousEpochSync), with response permit |
| `chain/client/src/sync/epoch.rs:629` | `Handler<EpochSyncResponseMessage>` | Validates, stale-check, applies proof |
| `chain/client/src/sync/state/mod.rs:269` | `StateSync::run` | Handler-path state sync incl. block requesting + head reset |
| `chain/client/src/sync/state/mod.rs:323` | `StateSync::run_with_shards` | Per-shard spawn/poll (also catchup) |
| `chain/client/src/sync/state/mod.rs:126` | `StateSync::new` peer source | Only download source; no fallback since #16009 |
| `chain/client/src/sync/state/shard.rs:56` | `run_state_sync_for_shard` | Header→parts→apply→finalize per shard |
| `chain/client/src/sync/state/shard.rs:124` | `buffer_unordered` (download) | Avoids head-of-line blocking on a stuck part (#16299) |
| `chain/client/src/sync/state/shard.rs:256` | `keep_applied_state_parts` | Re-apply parts after a *finished* apply (#16306) |
| `chain/client/src/sync/state/shard.rs:316` | `apply_state_part` | Idempotent apply via `StatePartsApplied` |
| `chain/client/src/sync/state/downloader.rs:44` | `ensure_shard_header` | Download+validate header with unbounded retry |
| `chain/client/src/sync/state/downloader.rs:138` | `ensure_shard_part_downloaded_single_attempt` | One part download+validate attempt |
| `chain/client/src/sync/state/network.rs:130` | `StateSyncDownloadSourcePeer::try_download` | p2p header/part request + await response |
| `chain/client/src/sync/state/network.rs:61` | `receive_peer_message` | Routes `StateResponse`/`Ack` to the waiting future |
| `chain/client/src/state_request_actor.rs:215` | `Handler<StateRequestHeader>` | Serves header requests (throttle+validate) |
| `chain/client/src/state_request_actor.rs:74` | `throttle_state_sync_request` | Sliding-window rate limiter |
| `chain/client/src/state_request_actor.rs:140` | `validate_sync_hash` | Recent-epoch + own-computation match |
| `chain/chain/src/state_sync/adapter.rs:367` | `get_state_header` | Ingest-side header/receipt-proof validation |
| `chain/chain/src/state_sync/adapter.rs:507` | `from_shard_id` index binding | Binds the shard label to the merkle index (#16379) |
| `chain/chain/src/chain.rs:2742` | `Chain::set_state_finalize` | Replays heights then validates the chunk commitment |
| `chain/chain/src/chain.rs:2787` | `validate_state_sync_chunk_extra` | Panics on a commitment mismatch (#16152) |
| `chain/chain/src/chain.rs:1620` | `reset_heads_post_state_sync` | Resets body/final head + tail after state sync |
| `chain/chain/src/state_sync/utils.rs:348` | `Chain::find_sync_hash` | Chooses sync hash from header head |
| `chain/chain/src/state_sync/utils.rs:374` | `get_extra_sync_block_hashes` | Extra blocks needed to finalize state sync |
| `chain/chain/src/state_sync/utils.rs:262` | `update_sync_hashes` | Records the epoch's sync hash once final |
| `chain/chain/src/state_sync/utils.rs:215` | `derive_epoch_sync_hash` | Re-derives an epoch's sync block when no row was stored |
| `chain/chain/src/state_sync/utils.rs:122` | `NEW_CHUNKS_PER_SHARD = 2` | Threshold defining the sync block |
| `chain/chain/src/runtime/mod.rs:541` | `validate_state_part_impl` | Decodes (limit-checked) then `Trie::validate_state_part` |
| `core/primitives/src/state_part.rs:36` | `PART_ENTRY_LIMIT` | Entry-count bound on a decoded part (#16388) |
| `core/primitives/src/state.rs:50` | `PartialState::check_entry_limit` | Rejects from the length prefix, pre-allocation |
| `core/primitives/src/state_part.rs:40` | `StatePartIndex` | Alias replacing bare `u64` part ids (#16199) |
| `chain/client/src/client.rs:2551` | `Client::run_catchup` | Per-next-epoch-shard state sync + block catchup |
| `chain/client/src/sync_jobs_actor.rs:39` | `handle_block_catch_up_request` | Offloads catchup chunk-apply work |
| `nearcore/src/state_sync.rs:956` | `state_sync_dump` | External-storage dump loop (cloud archival only) |
| `core/chain-configs/src/client_config.rs:328` | `SyncConfig` | Single `Peers` variant; ExternalStorage removed |
| `core/chain-configs/src/client_config.rs:343` | `SyncConcurrency` | apply / apply_during_catchup / peer_downloads / per_shard |
| `core/primitives-core/src/version.rs:605` | `ContinuousEpochSync` (PV 85) | Precomputed epoch proof serving |
| `core/primitives-core/src/version.rs:611` | `ValidateBlockOrdinalAndEpochSyncDataHash` (PV 85) | Header ordinal/data-hash revalidation |
| `chain/chain/src/chain.rs:1027` | `validate_header` gate | Where that revalidation actually runs |
| `chain/client/src/sync/external.rs:48` | `StateSyncConnection` | Bucket-layout helpers; dumper + state-viewer only, not a sync path |
| `chain/network/src/peer_manager/peer_manager_actor.rs:901` | `StateRequestHeader` routing | Requires `my_public_addr`; returns `MyPublicAddrNotKnown` otherwise |

## Open questions

- `docs/architecture/how/sync.md` was refreshed this release (#16266 added the
  "Picking the target height" section, #16009 removed the external-storage
  prose) and now matches the code, including the linear-pipeline diagram. The
  previously noted staleness is resolved. `docs/misc/state_sync_from_external_storage.md`
  was deleted; `docs/misc/state_sync_dump.md` survives and should be read as
  cloud-archival documentation, not state-sync documentation.
- **Resolved (was: NAT'd nodes have no fallback part source).** The code
  supports the first half and answers the second. Inbound Tier3 reachability is
  now a *hard requirement* for state sync: a state request carries the
  requester's own public address and the server dials back over Tier3
  (`NetworkRequests::StateRequestHeader`/`Part` handling,
  `chain/network/src/peer_manager/peer_manager_actor.rs:901`, `:950`), and if
  `my_public_addr` is unset the request is never even sent —
  `NetworkResponses::MyPublicAddrNotKnown` (`peer_manager_actor.rs:902`,
  `:951`), which the downloader counts as a distinct failure label
  (`network.rs:208`). With external storage gone there is no alternative
  source, so `ensure_shard_header` retries forever (`downloader.rs:44`). The
  recovery path *is* in the code, in the diagnostic itself
  (`downloader.rs:100`, logged every 30th consecutive failure ≈ 5 min at the
  default backoff): open the listening port for inbound TCP, or set
  `network.experimental.tier3_public_addr` (`chain/network/src/config_json.rs:319`
  → `chain/network/src/config.rs:389` → `my_public_addr`,
  `network_state/mod.rs:335`; logged at startup,
  `peer_manager_actor.rs:385`). What remains genuinely unanswerable from code
  is only the operational policy question — whether operators are *told* this is
  a hard requirement anywhere outside that log line.
- `run_sync_step` does not check `is_syncing()` before `handle_sync_needed`, and
  `handle_sync_needed`'s final `match` arm is `unreachable!`
  (`handler.rs:168`). Reaching `AwaitingPeers`/`NoSync` there is prevented by
  `decide_initial_phase` running first, but no type-level invariant enforces it.
- `validate_state_sync_chunk_extra` panics rather than erroring. The panic
  message tells the operator to restart with an empty data directory; there is
  no automated recovery path, and the underlying divergence (issue #15994) has
  no established root cause.
