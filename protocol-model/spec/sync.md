# Sync (header / block / state / epoch + catchup)

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `chain/client/src/sync/{handler,header,block,epoch}.rs`, `chain/client/src/sync/state/{mod,shard,downloader,network,external,chain_requests}.rs`, `chain/client/src/state_request_actor.rs`, `chain/client/src/sync_jobs_actor.rs`, `nearcore/src/state_sync.rs`, `chain/chain/src/state_sync/utils.rs`

## Role

Sync is how a node that is behind (crashed, newly joined, or slow) catches its
block head up to the network tip, and catchup is how a node that will track new
shards next epoch builds their state while staying live on its current shards.
The `SyncHandler` (`chain/client/src/sync/handler.rs:17`) drives a single linear
pipeline — EpochSync → HeaderSync → StateSync → BlockSync → NoSync — invoked
each client tick. It consumes headers/blocks/state parts from the network
([networking-p2p](networking-p2p.md)), state-part *construction* and the trie
part format live in [state-storage](state-storage.md), shard-tracking decisions
come from the epoch manager (see [validator-selection](validator-selection.md)),
and its output feeds block processing (see [chain-block-processing](chain-block-processing.md)).

## Key data structures

- **`SyncHandler`** — `chain/client/src/sync/handler.rs:17` — owns `sync_status:
  SyncStatus` plus the four phase drivers (`EpochSync`, `HeaderSync`,
  `StateSync`, `BlockSync`). One instance per client.
- **`SyncStatus`** — `chain/client-primitives/src/types.rs:129` (variants:
  `AwaitingPeers`, `NoSync`, `EpochSync(EpochSyncStatus)`, `HeaderSync{..}`,
  `StateSync(StateSyncStatus)`, `BlockSync{..}`) — the phase state machine.
  `HeaderSync`/`BlockSync` carry `start_height/current_height/highest_height`.
- **`SyncHandlerRequest`** — `chain/client/src/sync/handler.rs:31` — what the
  handler asks the client to do out-of-band: `NeedRequestBlocks`,
  `NeedProcessBlockArtifact`, `EpochSyncDataReset` (data-wipe + restart).
- **`HeaderSync`** — `chain/client/src/sync/header.rs:32` — tracks the in-flight
  header batch (`BatchProgress` at `header.rs:20`), the syncing peer, and stall
  timeouts. `MAX_BLOCK_HEADERS = 512` (`header.rs:15`) headers per batch.
- **`BlockSync`** — `chain/client/src/sync/block.rs:25` — tracks the last block
  request; `BLOCK_REQUEST_TIMEOUT_MS = 2000` (`block.rs:14`); requests up to
  `max_block_requests` blocks per batch.
- **`EpochSync`** — `chain/client/src/sync/epoch.rs:51` — holds genesis header,
  config, and a cached compressed proof. `EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS =
  3` (`epoch.rs:43`, pinned to `MIN_GC_NUM_EPOCHS_TO_KEEP`).
- **`EpochSyncProofV1`** — `core/primitives/src/epoch_sync.rs:53` — `{all_epochs:
  Vec<EpochSyncProofEpochData>, last_epoch: EpochSyncProofLastEpochData,
  current_epoch: EpochSyncProofCurrentEpochData}`; transmitted compressed as
  `CompressedEpochSyncProof`.
- **`StateSync`** — `chain/client/src/sync/state/mod.rs:75` — one per handler and
  one per catchup sync-hash. Owns the `StateSyncDownloader`, task trackers,
  `shard_syncs: HashMap<(sync_hash, ShardId), StateSyncShardHandle>`, and the
  peer shared-state. `STALE_SYNC_HASH_THRESHOLD = 100` (`mod.rs:67`; `5` under
  `test_features`).
- **`StateSyncResult`** — `mod.rs:476` — `NeedBlocks | InProgress | Completed |
  StaleSyncHash`; **`StateSyncShardResult`** — `mod.rs:490` — `InProgress |
  Completed` (shard-download-only, used by catchup).
- **`ShardSyncStatus`** — `chain/client-primitives/src/types.rs:42` — per-shard
  phase: `StateDownloadHeader → StateDownloadParts{done,total} →
  StateApplyInProgress{done,total} → StateApplyFinalizing → StateSyncDone`.
- **`StateSyncDownloadSource`** (trait) — `mod.rs:499` — abstracts a header/part
  source; implemented by `StateSyncDownloadSourcePeer` (`network.rs:31`) and
  `StateSyncDownloadSourceExternal` (`external.rs:20`).
- **`StatePartKey(sync_hash, shard_id, part_id)`** — `core/primitives/src/state_sync.rs:23` — key for
  `DBCol::StateParts` (downloaded parts) and `DBCol::StatePartsApplied` (applied
  markers).
- **`StateRequestActor`** — `chain/client/src/state_request_actor.rs:22` — serves
  header/part requests from other nodes, with a sliding-window rate limiter.

## Behavior

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
  highest_height - block_header_fetch_horizon` (`handler.rs:115`) it calls
  `find_sync_hash()` and, if it returns a hash, transitions to
  `StateSync(StateSyncStatus::new(sync_hash))` (`handler.rs:118`).
- `StateSync` → `state_sync.run(..)`; the `StateSyncResult` maps to handler
  requests (`handler.rs:125`): `NeedBlocks`→`NeedRequestBlocks`;
  `Completed`→transition to `BlockSync` + `NeedProcessBlockArtifact`;
  `StaleSyncHash`→`EpochSyncDataReset`.
- `BlockSync` → runs both header sync (banning **disabled**, `handler.rs:161`)
  and block sync each tick. Exit to `NoSync` is decided elsewhere (in the
  client's `run_sync_step`, on detecting the head caught up), not here.

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
canonical chain. Ordinals (not heights) are used because they count blocks, and
stopping at the final block is safe since finality is common across forks.

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

**Requesting** (`EpochSync::run`, `epoch.rs:128`): on `NotStarted` (or
`InProgress` past `timeout_for_epoch_sync`) pick a random peer, set
`InProgress{source_peer_id, source_peer_height, attempt_time}`, and send
`EpochSyncRequest` (`epoch.rs:158`).

**Serving** (`Handler<EpochSyncRequestMessage> for ClientActor`, `epoch.rs:559`):
when `ContinuousEpochSync` is enabled (PV 85; see gated-behavior), return the
pre-computed compressed proof from `epoch_store` directly (`epoch.rs:561`) — no
on-the-fly derivation. Otherwise, derive it on a spawned computation thread
(`derive_epoch_sync_proof`, `epoch.rs:83`), which targets a finalized recent
epoch via `find_target_epoch_to_produce_proof_for` and caches the result keyed
by `EpochId` (`epoch.rs:98`).

**Receiving / validating** (`Handler<EpochSyncResponseMessage>`, `epoch.rs:605`):
1. Drop the response unless we are `InProgress` from exactly `msg.from_peer`
   (`epoch.rs:609`).
2. Decode, then `validate_proof` (`epoch.rs:169`): confirm the source peer,
   reject proofs that are too recent (target within
   `max(epoch_length, transaction_validity_period)` of the peer's height,
   `epoch.rs:190`) or too old (older than `3 * epoch_length`, `epoch.rs:204`),
   then `verify_proof`.
3. `verify_proof` (`epoch.rs:315`) checks, inductively per epoch: the genesis+2
   epoch's block producers match the local epoch info (`epoch.rs:331`); each
   epoch's block producers hash to the previous epoch's `next_bp_hash`
   (`verify_block_producer_handoff`, `epoch.rs:482`); each epoch's last final
   block is endorsed by >2/3 stake (`verify_block_endorsements`, `epoch.rs:504`);
   the `epoch_sync_data_hash` matches the current epoch's first block header
   (`verify_epoch_sync_data_hash`, `epoch.rs:464`); and current-epoch merkle
   proofs / partial trees are well-formed (`verify_current_epoch_data`,
   `epoch.rs:394`).
4. **Stale-node check** (`epoch.rs:643`): if the proof is valid but the local
   `header_head.height != genesis_height`, the node has incompatible stale data;
   send `ShutdownReason::EpochSyncDataReset` and return (no proof applied).
5. Otherwise `apply_validated_proof` (`epoch.rs:226`): store the proof, write the
   4 boundary headers, set header head to the current-epoch first block and final
   head to genesis, and `init_after_epoch_sync` on the epoch manager
   (`epoch.rs:264`). Set status `EpochSync(Done)`.

### 6. State sync — handler path (`StateSync::run`, `mod.rs:327`)

1. Read the sync-hash block header. If `highest_height > sync_block_height +
   epoch_length + STALE_SYNC_HASH_THRESHOLD` (`mod.rs:341`), return
   `StaleSyncHash` (network moved past the epoch; parts unavailable).
2. `request_sync_blocks` (`mod.rs:278`): the sync-hash block, its prev, and the
   `get_extra_sync_block_hashes` (chain/chain `utils.rs:310`) must all be
   present; missing ones are returned as `NeedBlocks` (`mod.rs:355`). The sync
   hash block is stored as an orphan, so its presence is checked via `is_orphan`
   (`sync_block_status`, `mod.rs:255`).
3. `get_shards_cares_about_this_or_next_epoch` determines tracked shards
   (`mod.rs:359`), then `run_with_shards`.
4. On all-shards-`Completed`: `reset_heads_post_state_sync`
   (`chain/chain/src/chain.rs:1598`) sets body head to the sync-hash prev block,
   final head to genesis, and adjusts tail/chunk-tail to the earliest downloaded
   sync block; unlocks orphans. Returns `Completed(artifacts)`.

### 7. State sync — per-shard download (`run_with_shards`, `mod.rs:381`; `run_state_sync_for_shard`, `shard.rs:57`)

For each tracked shard `run_with_shards` polls or spawns a
`StateSyncShardHandle`. A shard already marked `StateSyncDone` is skipped
(`mod.rs:411`). The per-shard future does:
1. **Header** (`shard.rs:76`): `ensure_shard_header` — return the on-disk header
   if present, else download+validate with retries (`downloader.rs:47`).
   `num_state_parts` comes from the header.
2. **Download parts** (`shard.rs:105`): shuffle part IDs (`shard.rs:102`, so
   concurrent nodes don't hammer the same hosts in order), then download with
   `concurrency_limit` in-flight (`buffered`, `shard.rs:120`); failed parts are
   retried after `min_delay_before_reattempt` (`shard.rs:140`). Each part is
   validated against the state root (`validate_state_part`, `downloader.rs:207`)
   before being written to `DBCol::StateParts`.
3. **Apply parts** (`shard.rs:199`): unload memtrie, clear flat storage unless
   resuming a partial apply (`shard.rs:170-196`), then `apply_state_part` for
   each part (`shard.rs:318`), idempotent via `DBCol::StatePartsApplied`
   markers (`shard.rs:332`).
4. **Finalize** (`shard.rs:230`): create flat storage for the shard, load
   memtrie on catchup (`load_memtrie_on_catchup`, `shard.rs:253`), then send a
   `ChainFinalizationRequest` to the Chain thread (`shard.rs:264`) and mark
   `StateSyncDone`.

### 8. State-sync sources (`downloader.rs`)

`StateSyncDownloader` (`downloader.rs:28`) tries `preferred_source` (peer,
`network.rs`) and, if configured, a `fallback_source` (external storage,
`external.rs`). For headers it interleaves: after `num_attempts_before_fallback`
preferred attempts, one fallback attempt (`downloader.rs:75`). For parts the
source is chosen per attempt by cycling `num_prior_attempts %
(num_attempts_before_fallback + 1)` (`downloader.rs:188`).

- **Peer source** (`StateSyncDownloadSourcePeer::try_download`, `network.rs:130`):
  computes `sync_prev_prev_hash` (peers advertise snapshots by it, `network.rs:145`),
  sends `StateRequestHeader`/`StateRequestPart` (`network.rs:161`), registers a
  pending request keyed by `(shard_id, sync_hash, part_id_or_header)`
  (`network.rs:227`), and awaits the peer's `StateResponse` (delivered via
  `apply_peer_message`→`receive_peer_message`, `network.rs:61`) with a timeout.
  An `Ack(Busy|Error)` drops the pending request (`network.rs:106`).
- **External source** (`external.rs:69`): builds an
  `external_storage_location(chain_id, epoch_id, epoch_height, shard_id, type)`
  and downloads with a timeout; parses header/part bytes.

### 9. Serving state requests (`StateRequestActor`, `state_request_actor.rs`)

`Handler<StateRequestHeader>` (`state_request_actor.rs:215`) and
`<StateRequestPart>` (`:260`): first `throttle_state_sync_request`
(sliding-window limiter, `:74`) drops the request if the rate exceeds
`num_state_requests_per_throttle_period`; then `validate_sync_hash` (`:140`)
rejects sync hashes not from the current or immediately-previous epoch
(`is_sync_hash_from_known_recent_epoch`, `:107`) or that don't match the node's
own computed sync hash (`:165`). Valid requests are answered from
`ChainStateSyncAdapter::get_state_response_header` / `get_state_response_part`
(`:238`, `:282`), always as a V2 response.

### 10. State-sync dump (external storage upload, `nearcore/src/state_sync.rs`)

A node configured with `state_sync.dump` runs `StateSyncDumper::start`
(`state_sync.rs:56`), which spawns a loop (`state_sync_dump`, `:948`) that
detects new epochs (`check_head`, `:884`), uploads the state header then parts
(shuffled, `dump_shard_state`, `:422`) for each tracked shard to external
storage, skipping parts/headers already present (`set_missing_parts`,
`check_stored_headers`), retrying forever on error. Progress is persisted in
`STATE_SYNC_DUMP_KEY` DB entries (`InProgress`/`AllDumped`). Snapshot cadence
can skip epochs (`should_dump_epoch`, `:931`). Dump is disabled under the
`protocol_feature_spice` build (`:1017`).

### 11. Catchup (`Client::run_catchup`, `chain/client/src/client.rs:2519`)

`run_catchup` iterates `iterate_state_sync_infos()` (`client.rs:2525`) — the
shards a node must build state for before the next epoch (persisted in
`DBCol::StateDlInfos` when the first block of an epoch is processed). For each,
it derives the catchup sync hash, gets-or-creates a **separate** `StateSync`
(constructed with `catchup=true`, `client.rs:2555`, giving catchup-specific
concurrency), and runs `run_with_shards` (shard-download only; no head reset,
`client.rs:2569`). Once shards `Completed`, it runs `catchup_blocks_step`
(applies already-processed blocks from `DBCol::BlocksToCatchup` in BFS order for
the next-epoch shards) and, when finished, `finish_catchup_blocks`
(`client.rs:2582`). Heavy work (applying chunks for caught-up blocks) is
offloaded to `SyncJobsActor` (`sync_jobs_actor.rs:39`), which runs the shard
jobs on `apply_chunks_spawner` and returns results to the client; the client
alone writes to the chain store.

## Interactions

- **Consumes**: `HighestHeightPeerInfo` and network responses (headers, blocks,
  epoch proofs, state headers/parts) from [networking-p2p](networking-p2p.md);
  tracked-shard sets and epoch info from the epoch manager
  ([validator-selection](validator-selection.md)); the trie/state-part format
  and `apply_state_part`/`validate_state_part` from
  [state-storage](state-storage.md).
- **Produces**: `NetworkRequests::{BlockHeadersRequest, BlockRequest,
  EpochSyncRequest, StateRequestHeader, StateRequestPart, BanPeer}` to the
  network; validated state written to `DBCol::StateParts` /
  `DBCol::StatePartsApplied` and flat storage / memtrie; head/tail updates via
  [state-storage](state-storage.md); `BlockProcessingArtifact`s handed to block
  processing ([chain-block-processing](chain-block-processing.md)).
- **Serves**: state header/part requests to peers via `StateRequestActor`; epoch
  sync proofs via the `ClientActor` epoch-sync handlers; optional external-storage
  dump for other nodes' external state sync.

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs` in this tree
(`STABLE_PROTOCOL_VERSION = 86`).

| Feature | Activates | Effect on sync |
| --- | --- | --- |
| `ContinuousEpochSync` | PV **85** (`version.rs:563`) | Epoch sync proofs are maintained incrementally and stored compressed; the request handler returns the pre-computed proof directly (`epoch.rs:561`) instead of deriving it on demand. Checked with `PROTOCOL_VERSION` (the binary's max version), not per-block. |
| `ValidateBlockOrdinalAndEpochSyncDataHash` | PV **85** (`version.rs:569`) | Received block headers have their `block_ordinal` and `epoch_sync_data_hash` recomputed against local chain state during header validation. Strengthens the trust in headers pulled during header sync. |

Not gated in this component at PV 86:
- **External / cloud state sync** is a **node configuration** (`SyncConfig`),
  not a `ProtocolFeature`; no version gate applies (`mod.rs:147`).
- **Epoch-sync data-hash validation** *within the proof* is unconditional
  (`verify_epoch_sync_data_hash`, `epoch.rs:464`) — always run.
- `_DeprecatedCurrentEpochStateSync` (PV 74) and
  `_DeprecatedStatePartsCompression` (PV 82) are deprecated in this tree
  (`version.rs:279`, `:547`): the "sync to current epoch's state" and
  compressed-state-parts behaviors are now baseline, below
  `MIN_SUPPORTED_PROTOCOL_VERSION = 83` (`version.rs:600`), so no branch remains.

Note: an older baseline would list `CurrentEpochStateSync` and
`StatePartsCompression` as *active* named features and would show a PV-86 feature
of `FixContractLoadingError`; in **this 2.13.0 tree** the PV-86 feature is
`EnforcePerReceiptStorageProofLimit` (`version.rs:576`), which does not touch
sync, and there is no `FixContractLoadingError` (the loading-cost feature is the
nightly-only `FixContractLoadingCost` at PV 129, `version.rs:579`).

## Invariants & failure modes

- **Archival nodes never epoch/state sync** — enforced at the entry decision
  (`handler.rs:196`) and `BlockSync.archive` (`block.rs:33`); they would
  otherwise create unfixable history gaps.
- **Epoch sync horizon ≤ GC epochs** — `epoch_sync_horizon_num_epochs` must not
  exceed `gc_num_epochs_to_keep` or a near-horizon node could need
  GC'd blocks; `EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS` is asserted `== 3 ==
  MIN_GC_NUM_EPOCHS_TO_KEEP` at compile time (`epoch.rs:43`).
- **Epoch proof validity** — every proof is verified (BP handoff via
  `next_bp_hash`, >2/3 endorsement stake, epoch_sync_data_hash, merkle proofs)
  before any store write (`verify_proof`, `epoch.rs:315`); failure returns
  `Error::InvalidEpochSyncProof` and the response is dropped. Attacker-controlled
  heights use `checked_add` to avoid panics (`epoch.rs:522`, asserted by the
  regression test `verify_block_endorsements_rejects_max_height`, `epoch.rs:681`).
- **Unsolicited / wrong-peer responses dropped** — epoch responses
  (`epoch.rs:609`), state responses (wrong sender → `Error::Other("Unexpected
  state response (wrong sender)")`, `network.rs:97`).
- **State part validity** — a part failing `validate_state_part` is not
  persisted and the attempt errors ("Part data failed validation",
  `downloader.rs:222`); the part is retried.
- **Stale sync hash** — if the network advances more than
  `epoch_length + STALE_SYNC_HASH_THRESHOLD` past the sync hash, state sync
  returns `StaleSyncHash` → `EpochSyncDataReset` (`mod.rs:341`, `handler.rs:150`).
- **Idempotent apply / crash resume** — applied parts marked in
  `DBCol::StatePartsApplied`; on resume, flat storage cleanup is skipped when a
  partial apply exists but flat storage is absent (`shard.rs:177`).
- **Serve-side rate limiting & epoch check** — requests are throttled
  (`state_request_actor.rs:74`) and rejected for unknown/mismatched sync hashes
  (`validate_sync_hash`, `:140`), returning `None` (no response).
- **Single writer** — catchup's heavy apply work runs on `SyncJobsActor`, which
  holds no chain write access; only the client applies results
  (`sync_jobs_actor.rs`, and `docs/architecture/how/sync.md` "How catchup works").

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `chain/client/src/sync/handler.rs:77` | `SyncHandler::handle_sync_needed` | Drives the linear phase pipeline each tick |
| `chain/client/src/sync/handler.rs:183` | `decide_initial_phase` | Chooses entry phase from archival/horizon/proof state |
| `chain/client/src/sync/header.rs:109` | `HeaderSync::run` | One tick of header batch download / stall handling |
| `chain/client/src/sync/header.rs:265` | `get_locator` | Builds powers-of-2 ordinal locator |
| `chain/client/src/sync/block.rs:100` | `BlockSync::block_sync` | Requests missing blocks forward from last processed |
| `chain/client/src/sync/block.rs:51` | `get_last_processed_block` | Finds canonical processed reference point |
| `chain/client/src/sync/epoch.rs:128` | `EpochSync::run` | Sends/retries epoch sync request |
| `chain/client/src/sync/epoch.rs:315` | `EpochSync::verify_proof` | Inductive proof verification |
| `chain/client/src/sync/epoch.rs:226` | `apply_validated_proof` | Writes proof + inits epoch manager |
| `chain/client/src/sync/epoch.rs:559` | `Handler<EpochSyncRequestMessage>` | Serves proof (precomputed under ContinuousEpochSync) |
| `chain/client/src/sync/epoch.rs:605` | `Handler<EpochSyncResponseMessage>` | Validates, stale-check, applies proof |
| `chain/client/src/sync/state/mod.rs:327` | `StateSync::run` | Handler-path state sync incl. block requesting + head reset |
| `chain/client/src/sync/state/mod.rs:381` | `StateSync::run_with_shards` | Per-shard spawn/poll (also catchup) |
| `chain/client/src/sync/state/shard.rs:57` | `run_state_sync_for_shard` | Header→parts→apply→finalize per shard |
| `chain/client/src/sync/state/downloader.rs:47` | `ensure_shard_header` | Download+validate header with retry/fallback |
| `chain/client/src/sync/state/downloader.rs:156` | `ensure_shard_part_downloaded_single_attempt` | One part download+validate attempt |
| `chain/client/src/sync/state/network.rs:130` | `StateSyncDownloadSourcePeer::try_download` | p2p header/part request + await response |
| `chain/client/src/sync/state/external.rs:69` | `StateSyncDownloadSourceExternal` | External-storage header/part fetch |
| `chain/client/src/state_request_actor.rs:215` | `Handler<StateRequestHeader>` | Serves header requests (throttle+validate) |
| `chain/client/src/state_request_actor.rs:74` | `throttle_state_sync_request` | Sliding-window rate limiter |
| `chain/client/src/client.rs:2519` | `Client::run_catchup` | Per-next-epoch-shard state sync + block catchup |
| `chain/client/src/sync_jobs_actor.rs:39` | `handle_block_catch_up_request` | Offloads catchup chunk-apply work |
| `nearcore/src/state_sync.rs:948` | `state_sync_dump` | External-storage dump loop |
| `chain/chain/src/state_sync/utils.rs:284` | `Chain::find_sync_hash` | Chooses sync hash from header head |
| `chain/chain/src/state_sync/utils.rs:310` | `get_extra_sync_block_hashes` | Extra blocks needed to finalize state sync |
| `chain/chain/src/chain.rs:1598` | `reset_heads_post_state_sync` | Resets body/final head + tail after state sync |
| `core/primitives-core/src/version.rs:563` | `ContinuousEpochSync` (PV 85) | Precomputed epoch proof serving |
| `core/primitives-core/src/version.rs:569` | `ValidateBlockOrdinalAndEpochSyncDataHash` (PV 85) | Header ordinal/data-hash revalidation |

## Open questions

- The exact `NoSync` exit condition lives in the client's `run_sync_step`
  (referenced at `handler.rs:159`), which is outside the files read for this
  spec; the transition out of `BlockSync` is asserted only by comment here.
- `docs/architecture/how/sync.md` still describes a branching "two horizons"
  decision tree; the code (`handler.rs`) implements a single linear pipeline
  entered at different phases. The doc is stale on structure but accurate on
  intent; it should be refreshed on the next docs pass.
