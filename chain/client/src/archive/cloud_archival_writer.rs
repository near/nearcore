//! Cloud archival writer: moves finalized data from the hot store to the cloud storage.
//! Runs in a loop until the cloud head catches up with the hot final head.
use futures::FutureExt;
use near_async::futures::FutureSpawner;
use near_async::time::Clock;
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain_configs::{CloudArchivalWriterConfig, InterruptHandle};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, EpochId, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::archive::cloud_storage::archive::CloudArchivingError;
use near_store::archive::cloud_storage::retrieve::CloudRetrievalError;
use near_store::archive::cloud_storage::{BatchRange, compute_next_batch};
use near_store::db::{
    CLOUD_BLOCK_HEAD_KEY, CLOUD_MIN_HEAD_KEY, CLOUD_PREV_EPOCH_END_KEY, DBTransaction,
    cloud_shard_head_key,
};
use near_store::{DBCol, FINAL_HEAD_KEY, Store};
use std::io;
use std::sync::Arc;
use time::Duration;

/// Result of a single initialization attempt.
enum InitializationAttempt {
    /// Initialized successfully, ready to archive.
    Initialized,
    /// Node hasn't synced past genesis yet, retry later.
    WaitingForGenesis,
    /// Initialization failed.
    Error(CloudArchivalInitializationError),
}

/// Outcome of a single archiving attempt.
#[derive(Debug)]
enum CloudArchivingOutcome {
    /// Cloud head is at least the previous hot final head; nothing to do.
    Idle { cloud_head: BlockHeight },
    /// Archived a batch near the previous final head; nothing to archive until
    /// a new batch's worth of blocks is finalized.
    Recent { batch_end: BlockHeight },
    /// Archived a batch below the previous final head; more batches are
    /// immediately available.
    Old { batch_end: BlockHeight, target_height: BlockHeight },
}

/// Error surfaced while initializing cloud archive or writer.
#[derive(thiserror::Error, Debug)]
pub enum CloudArchivalInitializationError {
    #[error("IO error while initializing cloud archival: {message}")]
    IOError { message: String },
    #[error(
        "GC tail: {gc_tail}, exceeds GC stop height: {gc_stop_height:?} for the cloud min head: {min_head}"
    )]
    CloudHeadTooOld {
        min_head: BlockHeight,
        gc_stop_height: Option<BlockHeight>,
        gc_tail: BlockHeight,
    },
    #[error("Chain error: {error}")]
    ChainError { error: near_chain_primitives::Error },
    #[error("Error when updating cloud archival during initialization: {error}")]
    UpdateError { error: CloudArchivingError },
    #[error("Error when retrieving from cloud archival during initialization: {error}")]
    RetrievalError { error: CloudRetrievalError },
    #[error("batch_size ({batch_size}) must be < epoch_length ({epoch_length})")]
    InvalidBatchSize { batch_size: u64, epoch_length: u64 },
}

impl From<std::io::Error> for CloudArchivalInitializationError {
    fn from(error: std::io::Error) -> Self {
        CloudArchivalInitializationError::IOError { message: error.to_string() }
    }
}

impl From<near_chain_primitives::Error> for CloudArchivalInitializationError {
    fn from(error: near_chain_primitives::Error) -> Self {
        CloudArchivalInitializationError::ChainError { error }
    }
}

impl From<CloudArchivingError> for CloudArchivalInitializationError {
    fn from(error: CloudArchivingError) -> Self {
        CloudArchivalInitializationError::UpdateError { error }
    }
}

impl From<CloudRetrievalError> for CloudArchivalInitializationError {
    fn from(error: CloudRetrievalError) -> Self {
        CloudArchivalInitializationError::RetrievalError { error }
    }
}

/// A handle that allows the main process to interrupt cloud archival writer if needed.
#[derive(Clone)]
pub struct CloudArchivalWriterHandle(pub InterruptHandle);

impl CloudArchivalWriterHandle {
    pub fn new() -> Self {
        Self(InterruptHandle::new())
    }
}

/// Responsible for copying finalized blocks to cloud storage.
struct CloudArchivalWriter {
    clock: Clock,
    config: CloudArchivalWriterConfig,
    genesis_height: BlockHeight,
    hot_store: Store,
    cloud_storage: Arc<CloudStorage>,
    shard_tracker: ShardTracker,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    handle: CloudArchivalWriterHandle,
}

/// State resolved during writer initialization: the block head (if blocks are
/// archived), each tracked shard's head, the minimum across them, and the
/// previous epoch's end used as the default for any missing component.
struct ResolvedInitState {
    block_head: Option<BlockHeight>,
    shard_heads: Vec<(ShardId, BlockHeight)>,
    min_height: BlockHeight,
    prev_epoch_end: BlockHeight,
}

/// Information about a resharding the writer archives across.
struct ReshardingInfo {
    /// The last block of the pre-resharding epoch, where the layout changes.
    resharding_block_height: BlockHeight,
    /// The epoch that takes effect after the resharding.
    new_epoch_id: EpochId,
    /// The shard layout that takes effect after the resharding.
    new_layout: ShardLayout,
}

/// Creates the cloud archival writer if it is configured.
pub fn create_cloud_archival_writer(
    clock: Clock,
    future_spawner: Arc<dyn FutureSpawner>,
    writer_config: Option<CloudArchivalWriterConfig>,
    genesis_height: BlockHeight,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    hot_store: Store,
    cloud_storage: Option<&Arc<CloudStorage>>,
    shard_tracker: ShardTracker,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
) -> anyhow::Result<Option<CloudArchivalWriterHandle>> {
    let Some(config) = writer_config else {
        tracing::debug!(target: "cloud_archival", "not creating the cloud archival writer because it is not configured");
        return Ok(None);
    };
    let cloud_storage = cloud_storage
        .expect("Cloud archival writer is configured but cloud storage was not initialized.");
    assert!(
        config.archive_block_data || shard_tracker.tracks_non_empty_subset_of_shards(),
        "cloud archival writer must track at least one component (block data or shards)"
    );
    let writer = CloudArchivalWriter::new(
        clock,
        config,
        genesis_height,
        hot_store,
        cloud_storage.clone(),
        shard_tracker,
        epoch_manager,
    );
    let handle = writer.handle.clone();
    tracing::info!(target: "cloud_archival", "starting the cloud archival writer");
    future_spawner
        .spawn_boxed("cloud_archival_writer", writer.cloud_archival_loop(runtime_adapter).boxed());
    Ok(Some(handle))
}

impl CloudArchivalWriter {
    fn new(
        clock: Clock,
        config: CloudArchivalWriterConfig,
        genesis_height: BlockHeight,
        hot_store: Store,
        cloud_storage: Arc<CloudStorage>,
        shard_tracker: ShardTracker,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        let handle = CloudArchivalWriterHandle::new();
        Self {
            clock,
            config,
            genesis_height,
            hot_store,
            cloud_storage,
            shard_tracker,
            epoch_manager,
            handle,
        }
    }

    async fn cloud_archival_loop(self, runtime_adapter: Arc<dyn RuntimeAdapter>) {
        let mut initialized = false;
        while !self.handle.0.is_cancelled() {
            let sleep_duration = if !initialized {
                match self.try_initialize(&runtime_adapter).await {
                    InitializationAttempt::Initialized => {
                        initialized = true;
                        Duration::ZERO
                    }
                    InitializationAttempt::WaitingForGenesis => self.config.polling_interval,
                    InitializationAttempt::Error(error) => {
                        tracing::error!(
                            target: "cloud_archival",
                            error = ?error,
                            "cloud archival initialization failed; stopping cloud archival loop",
                        );
                        return;
                    }
                }
            } else {
                match self.try_archive_data().await {
                    Ok(CloudArchivingOutcome::Old { .. }) => Duration::ZERO,
                    _ => self.config.polling_interval,
                }
            };
            self.clock.sleep(sleep_duration).await;
        }
        tracing::debug!(target: "cloud_archival", "stopping the cloud archival loop");
    }

    /// Checks if the node is ready, then initializes the cloud archive writer.
    async fn try_initialize(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
    ) -> InitializationAttempt {
        let hot_final_height = match self.get_hot_final_head_height() {
            Ok(h) => h,
            Err(error) => {
                return InitializationAttempt::Error(error.into());
            }
        };
        if hot_final_height <= self.genesis_height {
            tracing::debug!(
                target: "cloud_archival",
                hot_final_height,
                genesis_height = self.genesis_height,
                "waiting for node to sync past genesis",
            );
            return InitializationAttempt::WaitingForGenesis;
        }
        match self.initialize(runtime_adapter).await {
            Ok(()) => {
                tracing::info!(target: "cloud_archival", "cloud archival initialized");
                InitializationAttempt::Initialized
            }
            Err(error) => InitializationAttempt::Error(error),
        }
    }

    /// Tries to archive one batch and logs the outcome.
    async fn try_archive_data(&self) -> Result<CloudArchivingOutcome, CloudArchivingError> {
        // TODO(cloud_archival) Add metrics
        let result = self.try_archive_data_impl().await;

        let Ok(outcome) = result else {
            tracing::error!(target: "cloud_archival", ?result, "archiving data to cloud failed");
            return result;
        };

        match outcome {
            CloudArchivingOutcome::Idle { cloud_head } => {
                tracing::trace!(
                    target: "cloud_archival",
                    cloud_head,
                    "no data was archived - cloud archival head is up to date"
                );
            }
            CloudArchivingOutcome::Recent { batch_end } => {
                tracing::trace!(
                    target: "cloud_archival",
                    batch_end,
                    "recent batch was archived"
                );
            }
            CloudArchivingOutcome::Old { batch_end, target_height } => {
                tracing::trace!(
                    target: "cloud_archival",
                    batch_end,
                    target_height,
                    "older batch was archived - more archiving needed"
                );
            }
        }
        Ok(outcome)
    }

    /// If the min cloud head lags the hot final head, archive the next height.
    /// Only archives components whose individual heads are behind.
    async fn try_archive_data_impl(&self) -> Result<CloudArchivingOutcome, CloudArchivingError> {
        let min_head = self.get_local_min_head()?;
        let batch_range = self.next_batch_after(min_head);
        let hot_final_height = self.get_hot_final_head_height()?;
        tracing::trace!(target: "cloud_archival", ?batch_range, hot_final_height, "try_archive");

        // The entire batch must be below hot_final_height: the last block in
        // the batch needs NextBlockHashes, which requires the next block to
        // be finalized.
        if batch_range.end() >= hot_final_height {
            return Ok(CloudArchivingOutcome::Idle { cloud_head: min_head });
        }

        self.archive_lagging_components(&batch_range).await?;

        let next_batch = self.next_batch_after(batch_range.end());
        let outcome = if next_batch.end() >= hot_final_height {
            CloudArchivingOutcome::Recent { batch_end: batch_range.end() }
        } else {
            CloudArchivingOutcome::Old {
                batch_end: batch_range.end(),
                target_height: hot_final_height - 1,
            }
        };
        tracing::trace!(target: "cloud_archival", ?outcome, "ending");
        Ok(outcome)
    }

    /// Returns the batch that follows `height`. Aligned to `batch_size`;
    /// may be partial when `height` is unaligned (first batch after genesis
    /// or fresh init).
    fn next_batch_after(&self, height: BlockHeight) -> BatchRange {
        compute_next_batch(height, self.cloud_storage.batch_size())
    }

    /// Archives all lagging components for the given batch and advances local heads.
    async fn archive_lagging_components(
        &self,
        batch_range: &BatchRange,
    ) -> Result<(), CloudArchivingError> {
        let prev_epoch_end = self.get_local_prev_epoch_end()?;
        let epoch_id = self.epoch_manager.get_next_epoch_id(&prev_epoch_end)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let tracked_shards =
            self.shard_tracker.get_tracked_shards_for_non_validator_in_epoch(&epoch_id)?;
        let epoch_ending_block_hash = self.find_epoch_ending_in_batch(batch_range)?;

        let block_advanced = if self.config.archive_block_data {
            self.archive_block_batch_if_lagging(batch_range).await?
        } else {
            false
        };
        let advanced_shards = self
            .archive_shard_batches_if_lagging(
                batch_range,
                &shard_layout,
                &tracked_shards,
                epoch_ending_block_hash,
            )
            .await?;
        if self.config.archive_block_data {
            if let Some(last_block_hash) = epoch_ending_block_hash {
                self.archive_ending_epoch_data(last_block_hash).await?;
            }
        }
        self.advance_local_heads(
            batch_range.end(),
            block_advanced,
            &advanced_shards,
            epoch_ending_block_hash,
        )?;
        Ok(())
    }

    /// The resharding at the given block, if any.
    fn resharding_info(
        &self,
        block_hash: CryptoHash,
    ) -> Result<Option<ReshardingInfo>, near_chain_primitives::Error> {
        // A resharding boundary is an epoch boundary where the shard layout changes.
        if !self.epoch_manager.is_resharding_boundary(&block_hash)? {
            return Ok(None);
        }
        let resharding_block_height = self.epoch_manager.get_block_info(&block_hash)?.height();
        let new_epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&block_hash)?;
        let new_layout = self.epoch_manager.get_shard_layout(&new_epoch_id)?;
        let resharding_info = ReshardingInfo { resharding_block_height, new_epoch_id, new_layout };
        Ok(Some(resharding_info))
    }

    /// Uploads epoch data for the epoch whose last block is `last_block_hash`.
    async fn archive_ending_epoch_data(
        &self,
        last_block_hash: CryptoHash,
    ) -> Result<(), CloudArchivingError> {
        let epoch_id = self.epoch_manager.get_epoch_id(&last_block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        self.cloud_storage.archive_epoch_data(&self.hot_store, &shard_layout, epoch_id).await
    }

    /// Returns the hash of the last block of the (at most one) epoch ending
    /// in the batch, or `None` if no epoch ends in the batch.
    fn find_epoch_ending_in_batch(
        &self,
        batch_range: &BatchRange,
    ) -> Result<Option<CryptoHash>, near_chain_primitives::Error> {
        let chain_store = self.hot_store.chain_store();
        for height in batch_range.start()..=batch_range.end() {
            let block_hash = match chain_store.get_block_hash_by_height(height) {
                Ok(hash) => hash,
                Err(near_chain_primitives::Error::DBNotFoundErr(_)) => continue,
                Err(other) => return Err(other),
            };
            if self.epoch_manager.is_next_block_epoch_start(&block_hash)? {
                return Ok(Some(block_hash));
            }
        }
        Ok(None)
    }

    /// Archives the block batch if the local block head is behind `batch_range.end()`.
    /// Returns true if the block head was advanced.
    async fn archive_block_batch_if_lagging(
        &self,
        batch_range: &BatchRange,
    ) -> Result<bool, CloudArchivingError> {
        if let Some(head) = self.get_local_block_head()? {
            if head >= batch_range.end() {
                return Ok(false);
            }
        }
        // TODO(cloud_archival): Race condition between this check and the upload below.
        // Will be replaced with ifGenerationMatch:0 atomic uploads + hash metadata verification.
        let ext_head = self.cloud_storage.retrieve_cloud_block_head_if_exists().await?;
        if ext_head.is_some_and(|h| h >= batch_range.end()) {
            return Ok(false);
        }
        self.cloud_storage.archive_block_batch(&self.hot_store, batch_range).await?;
        self.cloud_storage.update_cloud_block_head(batch_range.end()).await?;
        Ok(true)
    }

    /// Archives shard batches for tracked shards whose local head is behind
    /// `batch_range.end()`. Returns the shard IDs that were advanced.
    async fn archive_shard_batches_if_lagging(
        &self,
        batch_range: &BatchRange,
        shard_layout: &ShardLayout,
        tracked_shards: &[ShardUId],
        epoch_ending_block_hash: Option<CryptoHash>,
    ) -> Result<Vec<ShardId>, CloudArchivingError> {
        let shard_batches = self.shard_batches_to_archive(
            batch_range,
            shard_layout,
            tracked_shards,
            epoch_ending_block_hash,
        )?;
        let mut advanced_shards = Vec::new();
        for (shard_uid, shard_layout, batch_range) in shard_batches {
            let shard_id = shard_uid.shard_id();
            let lagging = match self.get_local_shard_head(shard_id)? {
                Some(head) => head < batch_range.end(),
                None => true,
            };
            if !lagging {
                continue;
            }
            // TODO(cloud_archival): Race condition between this check and the upload below.
            // Will be replaced with ifGenerationMatch:0 atomic uploads + hash metadata verification.
            let ext_head = self.cloud_storage.retrieve_cloud_shard_head_if_exists(shard_id).await?;
            if ext_head.is_some_and(|h| h >= batch_range.end()) {
                continue;
            }
            self.cloud_storage
                .archive_shard_batch(&self.hot_store, &shard_layout, &batch_range, shard_uid)
                .await?;
            self.cloud_storage.update_cloud_shard_head(shard_id, batch_range.end()).await?;
            advanced_shards.push(shard_id);
        }
        Ok(advanced_shards)
    }

    /// The shards to archive with the layout and height range each batch covers.
    /// Across a resharding a removed parent ends at the boundary, the new child
    /// shards start after it, and the survivors cover the whole batch.
    fn shard_batches_to_archive(
        &self,
        batch_range: &BatchRange,
        shard_layout: &ShardLayout,
        tracked_shards: &[ShardUId],
        epoch_ending_block_hash: Option<CryptoHash>,
    ) -> Result<Vec<(ShardUId, ShardLayout, BatchRange)>, near_chain_primitives::Error> {
        let resharding_info = match epoch_ending_block_hash {
            Some(block_hash) => self.resharding_info(block_hash)?,
            None => None,
        };
        let Some(resharding_info) = resharding_info else {
            // No resharding: every tracked shard covers the whole batch.
            return Ok(tracked_shards
                .iter()
                .map(|&uid| (uid, shard_layout.clone(), *batch_range))
                .collect());
        };
        let resharding_block = resharding_info.resharding_block_height;
        let new_tracked_shards = self
            .shard_tracker
            .get_tracked_shards_for_non_validator_in_epoch(&resharding_info.new_epoch_id)?;

        let mut shard_batches = Vec::new();
        for &shard_uid in tracked_shards {
            // A removed parent ends at the boundary; a carried-over shard keeps the
            // old layout across it, since the resharding leaves its account mapping unchanged.
            // TODO(cloud_archival): test carried-over reconstruction across the boundary.
            let batch_end = if new_tracked_shards.contains(&shard_uid) {
                batch_range.end()
            } else {
                resharding_block
            };
            let range = BatchRange::new(batch_range.start(), batch_end);
            shard_batches.push((shard_uid, shard_layout.clone(), range));
        }
        let child_range = BatchRange::new(resharding_block + 1, batch_range.end());
        for &child_uid in &new_tracked_shards {
            // New child shards are tracked after the resharding but not before.
            if !tracked_shards.contains(&child_uid) {
                shard_batches.push((child_uid, resharding_info.new_layout.clone(), child_range));
            }
        }
        Ok(shard_batches)
    }

    /// Initializes the cloud archive writer: validates bucket config and
    /// reconciles cloud heads with local state. Missing components start at the
    /// previous epoch's end so the first uploaded `EpochData` reflects a
    /// fully-archived epoch; existing ones are clamped to `hot_final_height - 1`.
    // TODO(cloud_archival) Cover this logic with tests.
    async fn initialize(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
    ) -> Result<(), CloudArchivalInitializationError> {
        self.cloud_storage.ensure_bucket_config().await?;
        self.check_batch_size_below_epoch_length()?;
        let hot_final_height = self.get_hot_final_head_height()?;
        // TODO(cloud_archival): support resharding
        let tracked_shard_ids = self.get_tracked_shard_ids(hot_final_height)?;

        let (block_head_ext, shard_heads_ext) =
            self.read_external_heads(&tracked_shard_ids).await?;

        let init_state =
            self.resolve_init_state(hot_final_height, block_head_ext, &shard_heads_ext)?;

        self.ensure_min_cloud_head_available_for_archiving(runtime_adapter, init_state.min_height)?;
        self.log_initialization_status(block_head_ext, &shard_heads_ext, init_state.prev_epoch_end);
        self.set_local_heads(&init_state)?;

        Ok(())
    }

    /// Reads external head for block (if configured) and each tracked shard.
    async fn read_external_heads(
        &self,
        tracked_shard_ids: &[ShardId],
    ) -> Result<
        (Option<BlockHeight>, Vec<(ShardId, Option<BlockHeight>)>),
        CloudArchivalInitializationError,
    > {
        let block_head_ext = if self.config.archive_block_data {
            self.cloud_storage.retrieve_cloud_block_head_if_exists().await?
        } else {
            None
        };
        let mut shard_heads_ext = Vec::new();
        for &shard_id in tracked_shard_ids {
            let head = self.cloud_storage.retrieve_cloud_shard_head_if_exists(shard_id).await?;
            shard_heads_ext.push((shard_id, head));
        }
        Ok((block_head_ext, shard_heads_ext))
    }

    /// Logs, per component, whether it resumes from an external head or starts
    /// fresh at the previous epoch's end.
    fn log_initialization_status(
        &self,
        block_head_ext: Option<BlockHeight>,
        shard_heads_ext: &[(ShardId, Option<BlockHeight>)],
        prev_epoch_end: BlockHeight,
    ) {
        let log = |component: &str, head: Option<BlockHeight>| match head {
            Some(head) => {
                tracing::info!(target: "cloud_archival", component, head, "resuming from external head")
            }
            None => {
                tracing::info!(target: "cloud_archival", component, start = prev_epoch_end, "no external head, starting from previous epoch end")
            }
        };
        if self.config.archive_block_data {
            log("block", block_head_ext);
        }
        for &(shard_id, head) in shard_heads_ext {
            log(&format!("shard {shard_id}"), head);
        }
    }

    /// Resolves each external head to its final local height and computes the
    /// overall minimum. Missing components default to the previous epoch's end
    /// so the writer archives the current epoch from its start; existing ones
    /// are clamped to `hot_final_height - 1`, the last archivable height.
    /// Callers guarantee `hot_final_height > genesis_height`, so both are valid.
    fn resolve_init_state(
        &self,
        hot_final_height: BlockHeight,
        block_head_ext: Option<BlockHeight>,
        shard_heads_ext: &[(ShardId, Option<BlockHeight>)],
    ) -> Result<ResolvedInitState, CloudArchivalInitializationError> {
        assert!(
            hot_final_height > self.genesis_height,
            "resolve_init_state called before node synced past genesis"
        );
        // The highest archivable height is hot_final_height - 1 (the loop
        // only archives at heights strictly below hot_final_height).
        let max_archivable_height = hot_final_height - 1;
        // Default for missing components: the previous epoch's last block, so
        // the writer archives the current epoch from its first block.
        let prev_epoch_end = self.prev_epoch_end_height(hot_final_height)?;

        let mut min_height_local: Option<BlockHeight> = None;
        let mut update_min = |height: BlockHeight| {
            min_height_local = Some(min_height_local.map_or(height, |cur| cur.min(height)));
        };

        // Clamp to max_archivable_height so the writer never fast-forwards
        // past its own chain state when another writer is ahead.
        let block_head_local = if self.config.archive_block_data {
            let height = block_head_ext.unwrap_or(prev_epoch_end).min(max_archivable_height);
            update_min(height);
            Some(height)
        } else {
            None
        };
        let shard_heads_local: Vec<(ShardId, BlockHeight)> = shard_heads_ext
            .iter()
            .map(|&(shard_id, ext_head)| {
                let height = ext_head.unwrap_or(prev_epoch_end).min(max_archivable_height);
                update_min(height);
                (shard_id, height)
            })
            .collect();

        let min_height_local = min_height_local.expect("writer must track at least one component");

        Ok(ResolvedInitState {
            block_head: block_head_local,
            shard_heads: shard_heads_local,
            min_height: min_height_local,
            prev_epoch_end,
        })
    }

    /// Hash of the last block of the epoch before the one containing
    /// `block_hash`, or the genesis block when there is no earlier epoch.
    fn prev_epoch_end_hash(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<CryptoHash, near_chain_primitives::Error> {
        let chain_store = self.hot_store.chain_store();
        let epoch_start = self.epoch_manager.get_epoch_start_height(block_hash)?;
        // The genesis epoch has no earlier epoch; floor at the genesis block.
        if epoch_start <= self.genesis_height {
            return chain_store.get_block_hash_by_height(self.genesis_height);
        }
        let first_block_hash = chain_store.get_block_hash_by_height(epoch_start)?;
        let first_block_header = chain_store.get_block_header(&first_block_hash)?;
        Ok(*first_block_header.prev_hash())
    }

    /// Height of the last block of the epoch before the one containing `height`,
    /// flooring at genesis when there is no earlier epoch.
    fn prev_epoch_end_height(
        &self,
        height: BlockHeight,
    ) -> Result<BlockHeight, near_chain_primitives::Error> {
        let chain_store = self.hot_store.chain_store();
        let block_hash = chain_store.get_block_hash_by_height(height)?;
        let prev_epoch_end = self.prev_epoch_end_hash(&block_hash)?;
        Ok(chain_store.get_block_header(&prev_epoch_end)?.height())
    }

    /// Returns the tracked shard IDs for the epoch at the given height.
    fn get_tracked_shard_ids(
        &self,
        height: BlockHeight,
    ) -> Result<Vec<ShardId>, CloudArchivalInitializationError> {
        let block_hash = self.hot_store.chain_store().get_block_hash_by_height(height)?;
        let epoch_id = self
            .epoch_manager
            .get_epoch_id(&block_hash)
            .map_err(near_chain_primitives::Error::from)?;
        let tracked_shards = self
            .shard_tracker
            .get_tracked_shards_for_non_validator_in_epoch(&epoch_id)
            .map_err(near_chain_primitives::Error::from)?;
        Ok(tracked_shards.iter().map(|uid| uid.shard_id()).collect())
    }

    /// A batch must never contain an entire epoch as a strict subset, so that
    /// a batch crosses at most one epoch boundary (the writer resolves
    /// `shard_layout` once from the batch's start). `batch_size == epoch_length`
    /// would satisfy this, but we keep a strict `<` as a defensive margin.
    fn check_batch_size_below_epoch_length(&self) -> Result<(), CloudArchivalInitializationError> {
        let height = self.get_hot_final_head_height()?;
        let block_hash = self.hot_store.chain_store().get_block_hash_by_height(height)?;
        let epoch_id = self
            .epoch_manager
            .get_epoch_id(&block_hash)
            .map_err(near_chain_primitives::Error::from)?;
        let epoch_length = self
            .epoch_manager
            .get_epoch_config(&epoch_id)
            .map_err(near_chain_primitives::Error::from)?
            .epoch_length;
        let batch_size = self.cloud_storage.batch_size() as u64;
        if batch_size >= epoch_length {
            return Err(CloudArchivalInitializationError::InvalidBatchSize {
                batch_size,
                epoch_length,
            });
        }
        Ok(())
    }

    /// Ensures the cloud min head has not been garbage collected.
    /// We check against `gc_stop_height` (not just `gc_tail`) because the
    /// writer needs data from the entire epoch containing `min_head` -
    /// `gc_stop_height` is the earliest height whose epoch data is guaranteed
    /// to be retained.
    fn ensure_min_cloud_head_available_for_archiving(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        min_head: BlockHeight,
    ) -> Result<(), CloudArchivalInitializationError> {
        let gc_tail = self.hot_store.chain_store().tail();
        if min_head < gc_tail {
            return Err(CloudArchivalInitializationError::CloudHeadTooOld {
                min_head,
                gc_stop_height: None,
                gc_tail,
            });
        }
        let hot_final_height = self.get_hot_final_head_height()?;
        assert!(min_head < hot_final_height, "guaranteed by resolve_init_state");
        let block_hash = self.hot_store.chain_store().get_block_hash_by_height(min_head)?;
        let gc_stop_height = runtime_adapter.get_gc_stop_height(&block_hash);
        // gc_stop_height at or below genesis means GC hasn't started yet.
        if gc_tail > gc_stop_height && gc_stop_height > self.genesis_height {
            return Err(CloudArchivalInitializationError::CloudHeadTooOld {
                min_head,
                gc_stop_height: Some(gc_stop_height),
                gc_tail,
            });
        }
        Ok(())
    }

    /// Reads the hot final head height; falls back to `genesis_height` if unset.
    fn get_hot_final_head_height(&self) -> io::Result<BlockHeight> {
        let hot_final_head = self.hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY);
        let hot_final_head_height = hot_final_head.map_or(self.genesis_height, |tip| tip.height);
        Ok(hot_final_head_height)
    }

    /// Returns the writer's stored min head: the highest height up to which
    /// all components are known archived (by us or another writer).
    fn get_local_min_head(&self) -> io::Result<BlockHeight> {
        Ok(self
            .hot_store
            .get_ser::<BlockHeight>(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY)
            .expect("CLOUD_MIN_HEAD should exist in hot store after initialize"))
    }

    /// Returns the hash of the last block of the latest fully-archived epoch.
    fn get_local_prev_epoch_end(&self) -> io::Result<CryptoHash> {
        Ok(self
            .hot_store
            .get_ser::<CryptoHash>(DBCol::BlockMisc, CLOUD_PREV_EPOCH_END_KEY)
            .expect("CLOUD_PREV_EPOCH_END should exist after initialize"))
    }

    /// Returns the locally stored cloud block head height, if any.
    fn get_local_block_head(&self) -> io::Result<Option<BlockHeight>> {
        Ok(self.hot_store.get_ser::<BlockHeight>(DBCol::BlockMisc, CLOUD_BLOCK_HEAD_KEY))
    }

    /// Returns the locally stored cloud shard head height, if any.
    fn get_local_shard_head(&self, shard_id: ShardId) -> io::Result<Option<BlockHeight>> {
        let key = cloud_shard_head_key(shard_id);
        Ok(self.hot_store.get_ser::<BlockHeight>(DBCol::BlockMisc, &key))
    }

    /// Sets local heads during initialization, each to its own resolved height.
    /// Block and shard heads are stored as `BlockHeight` (always <=
    /// `hot_final_height - 1`, clamped during `resolve_init_state`).
    /// `CLOUD_PREV_EPOCH_END` is derived from `min_height`.
    fn set_local_heads(
        &self,
        init_state: &ResolvedInitState,
    ) -> Result<(), CloudArchivalInitializationError> {
        let &ResolvedInitState { block_head, ref shard_heads, min_height, .. } = init_state;
        let mut transaction = DBTransaction::new();

        if let Some(block_head) = block_head {
            let height_bytes = borsh::to_vec(&block_head).unwrap();
            transaction.set(DBCol::BlockMisc, CLOUD_BLOCK_HEAD_KEY.to_vec(), height_bytes);
        }

        for &(shard_id, height) in shard_heads {
            let height_bytes = borsh::to_vec(&height).unwrap();
            transaction.set(DBCol::BlockMisc, cloud_shard_head_key(shard_id), height_bytes);
        }

        let min_head_bytes = borsh::to_vec(&min_height).unwrap();
        transaction.set(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY.to_vec(), min_head_bytes);

        let prev_epoch_end = self.compute_initial_prev_epoch_end(min_height)?;
        let prev_epoch_end_bytes = borsh::to_vec(&prev_epoch_end).unwrap();
        transaction.set(DBCol::BlockMisc, CLOUD_PREV_EPOCH_END_KEY.to_vec(), prev_epoch_end_bytes);

        self.hot_store.database().write(transaction);
        Ok(())
    }

    fn compute_initial_prev_epoch_end(
        &self,
        height: BlockHeight,
    ) -> Result<CryptoHash, near_chain_primitives::Error> {
        // `height` may be a skipped slot, so walk down to the nearest present block.
        let block_hash = self.find_present_block_at_or_below(height)?;
        // If the block itself ends an epoch, it is the prev-epoch end.
        if self.epoch_manager.is_next_block_epoch_start(&block_hash)? {
            return Ok(block_hash);
        }
        self.prev_epoch_end_hash(&block_hash)
    }

    fn find_present_block_at_or_below(
        &self,
        height: BlockHeight,
    ) -> Result<CryptoHash, near_chain_primitives::Error> {
        let chain_store = self.hot_store.chain_store();
        for h in (self.genesis_height..=height).rev() {
            match chain_store.get_block_hash_by_height(h) {
                Ok(hash) => return Ok(hash),
                Err(near_chain_primitives::Error::DBNotFoundErr(_)) => continue,
                Err(other) => return Err(other),
            }
        }
        unreachable!("genesis block must be present")
    }

    /// Advances local heads after archiving at `height`. Only updates heads for
    /// components that were actually behind. Always advances CLOUD_MIN_HEAD.
    /// Atomically advances `CLOUD_PREV_EPOCH_END` when an epoch ended in the batch.
    fn advance_local_heads(
        &self,
        height: BlockHeight,
        block_advanced: bool,
        advanced_shard_ids: &[ShardId],
        new_prev_epoch_end: Option<CryptoHash>,
    ) -> Result<(), near_chain_primitives::Error> {
        let height_bytes = borsh::to_vec(&height).unwrap();
        let mut transaction = DBTransaction::new();
        if block_advanced {
            transaction.set(DBCol::BlockMisc, CLOUD_BLOCK_HEAD_KEY.to_vec(), height_bytes.clone());
        }
        for &shard_id in advanced_shard_ids {
            transaction.set(DBCol::BlockMisc, cloud_shard_head_key(shard_id), height_bytes.clone());
        }
        transaction.set(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY.to_vec(), height_bytes);
        if let Some(new_prev_epoch_end) = new_prev_epoch_end {
            transaction.set(
                DBCol::BlockMisc,
                CLOUD_PREV_EPOCH_END_KEY.to_vec(),
                borsh::to_vec(&new_prev_epoch_end).unwrap(),
            );
        }
        self.hot_store.database().write(transaction);
        Ok(())
    }
}
