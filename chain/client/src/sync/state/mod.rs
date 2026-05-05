pub mod chain_requests;
mod downloader;
mod external;
mod network;
mod shard;
mod task_tracker;
mod util;

use crate::metrics;
use crate::sync::external::StateSyncConnection;
use chain_requests::ChainSenderForStateSync;
use downloader::StateSyncDownloader;
use external::StateSyncDownloadSourceExternal;
use futures::future::BoxFuture;
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{AsyncSender, IntoAsyncSender};
use near_async::time::{Clock, Duration, Utc};
use near_chain::chain::ApplyChunksDoneSender;
use near_chain::types::RuntimeAdapter;
use near_chain::{BlockHeader, BlockProcessingArtifact, Chain};
use near_chain_configs::{ExternalStorageConfig, StateSyncConfig, SyncConcurrency, SyncConfig};
use near_chunks::logic::get_shards_cares_about_this_or_next_epoch;
use near_client_primitives::types::{ShardSyncStatus, StateSyncStatus};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_external_storage::S3AccessConfig;
use near_network::client::StateResponse;
use near_network::types::{
    HighestHeightPeerInfo, PeerManagerMessageRequest, PeerManagerMessageResponse,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_part::StatePart;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::ShardId;
use near_store::Store;
use network::{StateSyncDownloadSourcePeer, StateSyncDownloadSourcePeerSharedState};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::thread_rng;
use shard::{StateSyncShardHandle, run_state_sync_for_shard};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use task_tracker::{TaskHandle, TaskTracker};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio_util::sync::CancellationToken;

/// Number of blocks past epoch_length that triggers stale sync hash detection.
///
/// During state sync, if the network's highest height exceeds the sync hash
/// block's height + epoch_length + this threshold, the sync hash is considered
/// stale and the node triggers a data reset + restart.
///
/// Must be large enough to account for epoch stretching due to missing blocks
/// and finality delays. Epoch boundaries require `last_finalized_height + 3
/// >= estimated_next_epoch_start`, so with sparse block production, epochs
/// can extend beyond epoch_length. A 100-block threshold is safe because a
/// false positive would require 100+ blocks without finality — a catastrophic
/// consensus failure, not normal missing blocks.
///
/// Under `test_features`, the threshold is lowered to 5 so tests with
/// epoch_length=10 can trigger stale sync hash detection without needing
/// hundreds of blocks.
#[cfg(not(feature = "test_features"))]
pub const STALE_SYNC_HASH_THRESHOLD: u64 = 100;
#[cfg(feature = "test_features")]
pub const STALE_SYNC_HASH_THRESHOLD: u64 = 5;

/// Module that manages state sync. Internally, it spawns multiple tasks to download state sync
/// headers and parts in parallel for the requested shards, but externally, all that it exposes
/// is a single `run` method that should be called periodically, returning that we're either
/// done or still in progress, while updating the externally visible status.
pub struct StateSync {
    clock: Clock,
    store: Store,
    future_spawner: Arc<dyn FutureSpawner>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,

    /// Timeout for block requests during state sync.
    block_request_timeout: Duration,
    /// A map storing the last time a block was requested for state sync.
    last_time_sync_block_requested: HashMap<CryptoHash, Utc>,

    /// We keep a reference to this so that peer messages received about state sync can be
    /// given to the StateSyncDownloadSourcePeer.
    peer_source_state: Arc<Mutex<StateSyncDownloadSourcePeerSharedState>>,

    /// The main downloading logic.
    downloader: Arc<StateSyncDownloader>,

    /// Internal parallelization limiters as well as status tracker. We need a handle here to
    /// export statuses of the workers to the debug page.
    downloading_task_tracker: TaskTracker,
    computation_task_tracker: TaskTracker,

    /// Multi-sender to handle requests that must be performed on the thread that owns the Chain.
    chain_requests_sender: ChainSenderForStateSync,

    /// There is one entry in this map for each shard that is being synced.
    shard_syncs: HashMap<(CryptoHash, ShardId), StateSyncShardHandle>,

    /// Concurrency limits.
    concurrency_config: SyncConcurrency,

    /// A minimum delay between attempts for the same state header/part.
    /// Specifically important in the scenario that a node is configured
    /// to sync only from external storage (no p2p requests). Usually a
    /// failure there indicates the file is yet to be uploaded, in which
    /// case we want to avoid spamming requests aggressively.
    min_delay_before_reattempt: Duration,
}

impl StateSync {
    /// Note: `future_spawner` is used to spawn futures that perform state sync tasks.
    /// However, there is internal limiting of parallelization as well (to make sure
    /// that we do not overload rocksdb, peers, or external storage), so it is
    /// preferred to pass in a spawner that has a lot of concurrency.
    pub fn new(
        clock: Clock,
        store: Store,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
        network_adapter: AsyncSender<PeerManagerMessageRequest, PeerManagerMessageResponse>,
        external_timeout: Duration,
        p2p_timeout: Duration,
        retry_backoff: Duration,
        external_backoff: Duration,
        chain_id: &str,
        sync_config: &StateSyncConfig,
        chain_requests_sender: ChainSenderForStateSync,
        future_spawner: Arc<dyn FutureSpawner>,
        catchup: bool,
    ) -> Self {
        let block_request_timeout = external_timeout;
        let peer_source_state =
            Arc::new(Mutex::new(StateSyncDownloadSourcePeerSharedState::default()));
        let peer_source = Arc::new(StateSyncDownloadSourcePeer {
            clock: clock.clone(),
            store: store.clone(),
            request_sender: network_adapter,
            request_timeout: p2p_timeout,
            state: peer_source_state.clone(),
        }) as Arc<dyn StateSyncDownloadSource>;
        let (fallback_source, num_attempts_before_fallback, num_concurrent_requests) =
            if let SyncConfig::ExternalStorage(ExternalStorageConfig {
                location,
                num_concurrent_requests,
                num_concurrent_requests_during_catchup,
                external_storage_fallback_threshold,
            }) = &sync_config.sync
            {
                let s3_access_config = S3AccessConfig {
                    timeout: external_timeout.max(Duration::ZERO).unsigned_abs(),
                    is_readonly: true,
                };
                let external = StateSyncConnection::new(location, None, s3_access_config);
                let num_concurrent_requests = if catchup {
                    *num_concurrent_requests_during_catchup
                } else {
                    *num_concurrent_requests
                };
                let fallback_source = Arc::new(StateSyncDownloadSourceExternal {
                    clock: clock.clone(),
                    store: store.clone(),
                    epoch_manager: epoch_manager.clone(),
                    chain_id: chain_id.to_string(),
                    conn: external,
                    timeout: external_timeout,
                }) as Arc<dyn StateSyncDownloadSource>;
                (
                    Some(fallback_source),
                    *external_storage_fallback_threshold as usize,
                    num_concurrent_requests.min(sync_config.concurrency.peer_downloads),
                )
            } else {
                (None, 0, sync_config.concurrency.peer_downloads)
            };

        let downloading_task_tracker = TaskTracker::new(usize::from(num_concurrent_requests));
        let downloader = Arc::new(StateSyncDownloader {
            clock: clock.clone(),
            store: store.clone(),
            preferred_source: peer_source,
            fallback_source,
            num_attempts_before_fallback,
            header_validation_sender: chain_requests_sender.clone().into_async_sender(),
            runtime: runtime.clone(),
            retry_backoff,
            task_tracker: downloading_task_tracker.clone(),
        });

        let num_concurrent_computations = if catchup {
            sync_config.concurrency.apply_during_catchup
        } else {
            sync_config.concurrency.apply
        };
        let computation_task_tracker = TaskTracker::new(usize::from(num_concurrent_computations));

        let has_fallback = downloader.fallback_source.is_some();
        let min_delay_before_reattempt = if has_fallback && num_attempts_before_fallback == 0 {
            // Avoid aggressively checking the external storage for requests which just failed
            external_backoff
        } else {
            // No need to wait if p2p attempts are enabled
            Duration::ZERO
        };

        Self {
            clock,
            store,
            block_request_timeout,
            last_time_sync_block_requested: HashMap::new(),
            peer_source_state,
            downloader,
            downloading_task_tracker,
            computation_task_tracker,
            future_spawner,
            epoch_manager,
            runtime,
            chain_requests_sender,
            shard_syncs: HashMap::new(),
            concurrency_config: sync_config.concurrency,
            min_delay_before_reattempt,
        }
    }

    /// Apply a state sync message received from a peer.
    pub fn apply_peer_message(
        &self,
        peer_id: PeerId,
        msg: StateResponse,
    ) -> Result<(), near_chain::Error> {
        self.peer_source_state.lock().receive_peer_message(peer_id, msg)?;
        Ok(())
    }

    /// Verifies if the node possesses the given block. If the block is absent,
    /// the node should request it from peers.
    ///
    /// the return value is a tuple (request_block, have_block)
    ///
    /// the return value (false, false) means that the node already requested
    /// the block but hasn't received it yet
    fn sync_block_status(
        &self,
        chain: &Chain,
        sync_hash: &CryptoHash,
        block_hash: &CryptoHash,
        now: Utc,
    ) -> (bool, bool) {
        // The sync hash block is saved as an orphan. The other blocks are saved
        // as regular blocks. Check if block exists depending on that.
        let block_exists = if sync_hash == block_hash {
            chain.is_orphan(block_hash)
        } else {
            chain.block_exists(block_hash)
        };

        if block_exists {
            return (false, true);
        }
        let Some(last_time) = self.last_time_sync_block_requested.get(block_hash) else {
            return (true, false);
        };

        let timeout = self.block_request_timeout;
        if (now - *last_time) >= timeout {
            tracing::error!(target: "sync", ?block_hash, ?timeout, "state sync: block request timed out");
            (true, false)
        } else {
            (false, false)
        }
    }

    /// Checks if the sync blocks are available and requests them if needed.
    fn request_sync_blocks(
        &mut self,
        chain: &Chain,
        block_header: &BlockHeader,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Vec<(CryptoHash, PeerId)> {
        let now = self.clock.now_utc();

        let sync_hash = *block_header.hash();
        let prev_hash = *block_header.prev_hash();

        let mut needed_block_hashes = vec![prev_hash, sync_hash];
        let mut extra_block_hashes = chain.get_extra_sync_block_hashes(&prev_hash);
        tracing::trace!(target: "sync", ?needed_block_hashes, ?extra_block_hashes, "request_sync_blocks: block hashes for state sync");
        needed_block_hashes.append(&mut extra_block_hashes);
        let mut blocks_to_request = vec![];

        let mut rng = thread_rng();
        for hash in needed_block_hashes {
            let (request_block, have_block) = self.sync_block_status(chain, &sync_hash, &hash, now);
            tracing::trace!(target: "sync", ?hash, ?request_block, ?have_block, "request_sync_blocks");

            if have_block {
                self.last_time_sync_block_requested.remove(&hash);
            }

            if !request_block {
                tracing::trace!(target: "sync", ?hash, ?have_block, "request_sync_blocks: skipping - no request");
                continue;
            }

            let peer_info = highest_height_peers.choose(&mut rng);
            let Some(peer_info) = peer_info else {
                tracing::trace!(target: "sync", ?hash, "request_sync_blocks: skipping - no peer");
                continue;
            };
            let peer_id = peer_info.peer_info.id.clone();
            self.last_time_sync_block_requested.insert(hash, now);
            blocks_to_request.push((hash, peer_id));
        }

        tracing::trace!(target: "sync", num_blocks_to_request = blocks_to_request.len(), "request_sync_blocks: done");

        blocks_to_request
    }

    /// Main loop for the normal sync handler path. Requests sync blocks,
    /// computes tracking shards, runs shard downloading, and finalizes
    /// by resetting heads when all shards complete.
    pub fn run(
        &mut self,
        sync_status: &mut StateSyncStatus,
        shard_tracker: &ShardTracker,
        chain: &mut Chain,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
        apply_chunks_done_sender: Option<ApplyChunksDoneSender>,
    ) -> Result<StateSyncResult, near_chain::Error> {
        let sync_hash = sync_status.sync_hash;
        let block_header = chain.get_block_header(&sync_hash)?;

        // If the network has moved past this epoch, state parts are no longer
        // available. Trigger a data reset so the node can restart fresh.
        if highest_height > block_header.height() + chain.epoch_length + STALE_SYNC_HASH_THRESHOLD {
            tracing::warn!(
                target: "sync",
                ?block_header,
                highest_height,
                "stale sync hash detected, triggering data reset"
            );
            return Ok(StateSyncResult::StaleSyncHash);
        }

        // Waiting for all the sync blocks to be available because they are
        // needed to finalize state sync.
        let blocks_to_request =
            self.request_sync_blocks(chain, &block_header, highest_height_peers);
        if !blocks_to_request.is_empty() {
            return Ok(StateSyncResult::NeedBlocks(blocks_to_request));
        }

        let tracking_shards = get_shards_cares_about_this_or_next_epoch(
            &block_header,
            shard_tracker,
            self.epoch_manager.as_ref(),
        );
        match self.run_with_shards(sync_status, &tracking_shards)? {
            StateSyncShardResult::Completed => {
                tracing::info!(target: "sync", "state sync: all shards are done");
                let mut block_processing_artifacts = BlockProcessingArtifact::default();
                chain.reset_heads_post_state_sync(
                    sync_hash,
                    &mut block_processing_artifacts,
                    apply_chunks_done_sender,
                )?;
                Ok(StateSyncResult::Completed(block_processing_artifacts))
            }
            StateSyncShardResult::InProgress => Ok(StateSyncResult::InProgress),
        }
    }

    /// Main loop that should be called periodically with explicit tracking shards.
    /// Used by the catchup path which computes shards differently.
    pub fn run_with_shards(
        &mut self,
        sync_status: &mut StateSyncStatus,
        tracking_shards: &[ShardId],
    ) -> Result<StateSyncShardResult, near_chain::Error> {
        let sync_hash = sync_status.sync_hash;
        let _span =
            tracing::debug_span!(target: "sync", "run_sync", sync_type = "StateSync").entered();
        tracing::debug!(target: "sync", %sync_hash, ?tracking_shards, "syncing state");

        let mut all_done = true;
        for shard_id in tracking_shards {
            let key = (sync_hash, *shard_id);
            let status = match self.shard_syncs.entry(key) {
                Entry::Occupied(mut entry) => match entry.get_mut().result.try_recv() {
                    Ok(result) => {
                        entry.remove();
                        if let Err(err) = result {
                            tracing::error!(target: "sync", %shard_id, ?err, "state sync failed for shard");
                            return Err(err);
                        }
                        ShardSyncStatus::StateSyncDone
                    }
                    Err(TryRecvError::Closed) => {
                        return Err(near_chain::Error::Other(
                            "Shard result channel somehow closed".to_owned(),
                        ));
                    }
                    Err(TryRecvError::Empty) => entry.get().status(),
                },
                Entry::Vacant(entry) => {
                    if sync_status
                        .sync_status
                        .get(&shard_id)
                        .is_some_and(|status| *status == ShardSyncStatus::StateSyncDone)
                    {
                        continue;
                    }
                    let status = Arc::new(Mutex::new(ShardSyncStatus::StateDownloadHeader));
                    let cancel = CancellationToken::new();
                    let shard_sync = run_state_sync_for_shard(
                        self.store.clone(),
                        *shard_id,
                        sync_hash,
                        self.downloader.clone(),
                        self.runtime.clone(),
                        self.epoch_manager.clone(),
                        self.computation_task_tracker.clone(),
                        status.clone(),
                        self.chain_requests_sender.clone().into_async_sender(),
                        cancel.clone(),
                        self.future_spawner.clone(),
                        self.concurrency_config.per_shard,
                        self.min_delay_before_reattempt,
                    );
                    let (sender, receiver) = oneshot::channel();

                    self.future_spawner.spawn("shard sync", async move {
                        sender.send(shard_sync.await).ok();
                    });
                    let handle = StateSyncShardHandle { status, result: receiver, cancel };
                    let ret = handle.status();
                    entry.insert(handle);
                    ret
                }
            };
            sync_status.sync_status.insert(*shard_id, status);
            metrics::STATE_SYNC_STAGE
                .with_label_values(&[&shard_id.to_string()])
                .set(status.repr() as i64);
            if status != ShardSyncStatus::StateSyncDone {
                all_done = false;
            }
        }

        // If a shard completed syncing, we just remove it. We will not be syncing it again the next time around,
        // because we would've marked it as completed in the status for that shard.
        self.shard_syncs.retain(|(existing_sync_hash, existing_shard_id), _v| {
            tracking_shards.contains(existing_shard_id) && existing_sync_hash == &sync_hash
        });

        sync_status.download_tasks = self.downloading_task_tracker.statuses();
        sync_status.computation_tasks = self.computation_task_tracker.statuses();
        if all_done {
            // Clean up block request tracking for next round now that state sync is done.
            self.last_time_sync_block_requested.clear();
            Ok(StateSyncShardResult::Completed)
        } else {
            Ok(StateSyncShardResult::InProgress)
        }
    }
}

/// Result of `StateSync::run()` — the full handler path including block
/// requesting and head resetting.
pub enum StateSyncResult {
    /// Need to request blocks from peers before state sync can proceed.
    NeedBlocks(Vec<(CryptoHash, PeerId)>),
    /// State sync still in progress. No action needed by the caller.
    InProgress,
    /// State sync completed and heads have been reset.
    Completed(BlockProcessingArtifact),
    /// The sync hash is stale — the network has moved past this epoch and
    /// state parts are no longer available. The node should reset and restart.
    StaleSyncHash,
}

/// Result of `StateSync::run_with_shards()` — shard downloading only,
/// used by the catchup path.
pub enum StateSyncShardResult {
    /// State sync still in progress. No action needed by the caller.
    InProgress,
    /// The state for all shards was downloaded.
    Completed,
}

/// Abstracts away the source of state sync headers and parts. Only one instance is kept per
/// state sync, NOT per shard.
pub(self) trait StateSyncDownloadSource: Send + Sync + 'static {
    fn download_shard_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<ShardStateSyncResponseHeader, near_chain::Error>>;

    fn download_shard_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<StatePart, near_chain::Error>>;
}

/// Find the hash of the first block on the same epoch (and chain) of block with hash `sync_hash`.
pub fn get_epoch_start_sync_hash(
    chain: &Chain,
    sync_hash: &CryptoHash,
) -> Result<CryptoHash, near_chain::Error> {
    let mut header = chain.get_block_header(sync_hash)?;
    let mut epoch_id = *header.epoch_id();
    let mut hash = *header.hash();
    let mut prev_hash = *header.prev_hash();
    loop {
        if prev_hash == CryptoHash::default() {
            return Ok(hash);
        }
        header = chain.get_block_header(&prev_hash)?;
        if &epoch_id != header.epoch_id() {
            return Ok(hash);
        }
        epoch_id = *header.epoch_id();
        hash = *header.hash();
        prev_hash = *header.prev_hash();
    }
}
