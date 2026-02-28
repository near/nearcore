//! Cloud archival writer: moves finalized data from the hot store to the cloud storage.
//! Runs in a loop until the cloud head catches up with the hot final head.
use std::io;
use std::sync::Arc;

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
use near_store::db::{
    CLOUD_BLOCK_HEAD_KEY, CLOUD_MIN_HEAD_KEY, DBTransaction, cloud_shard_head_key,
};
use near_store::{DBCol, FINAL_HEAD_KEY, Store};
use time::Duration;

/// Result of a single archiving attempt.
#[derive(Debug)]
enum CloudArchivingResult {
    // Cloud head is at least the previous hot final head; nothing to do. Contains the
    // current cloud head.
    NoHeightArchived(BlockHeight),
    // Archived the previous final head height; nothing to archive until a new block is
    // finalized. Contains the target (final - 1) height that was archived.
    LatestHeightArchived(BlockHeight),
    // Archived a height below the previous final head; more heights are immediately
    // available. Contains (archived_height, target_height).
    OlderHeightArchived(BlockHeight, BlockHeight),
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
                match self.try_initialize_cloud_heads(&runtime_adapter).await {
                    Some(true) => {
                        initialized = true;
                        Duration::ZERO
                    }
                    Some(false) => self.config.polling_interval,
                    None => return,
                }
            } else {
                match self.try_archive_data().await {
                    Ok(CloudArchivingResult::OlderHeightArchived(..)) => Duration::ZERO,
                    _ => self.config.polling_interval,
                }
            };
            self.clock.sleep(sleep_duration).await;
        }
        tracing::debug!(target: "cloud_archival", "stopping the cloud archival loop");
    }

    /// Checks if the node is ready, then initializes cloud heads.
    /// Returns `Some(true)` on success, `Some(false)` if not ready yet,
    /// `None` on fatal error.
    async fn try_initialize_cloud_heads(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
    ) -> Option<bool> {
        let hot_final_height = self.get_hot_final_head_height().ok()?;
        if hot_final_height <= self.genesis_height {
            tracing::debug!(
                target: "cloud_archival",
                hot_final_height,
                genesis_height = self.genesis_height,
                "waiting for node to sync past genesis",
            );
            return Some(false);
        }
        match self.initialize_cloud_heads(runtime_adapter).await {
            Ok(()) => {
                tracing::info!(target: "cloud_archival", "cloud archival initialized");
                Some(true)
            }
            Err(error) => {
                tracing::error!(target: "cloud_archival", ?error, "cloud archival initialization failed");
                None
            }
        }
    }

    /// Tries to archive one height and logs the outcome.
    async fn try_archive_data(&self) -> Result<CloudArchivingResult, CloudArchivingError> {
        // TODO(cloud_archival) Add metrics
        let result = self.try_archive_data_impl().await;

        let Ok(result) = result else {
            tracing::error!(target: "cloud_archival", ?result, "archiving data to cloud failed");
            return result;
        };

        match result {
            CloudArchivingResult::NoHeightArchived(cloud_head) => {
                tracing::trace!(
                    target: "cloud_archival",
                    cloud_head,
                    "no height was archived - cloud archival head is up to date"
                );
            }
            CloudArchivingResult::LatestHeightArchived(target_height) => {
                tracing::trace!(
                    target: "cloud_archival",
                    target_height,
                    "latest height was archived"
                );
            }
            CloudArchivingResult::OlderHeightArchived(archived_height, target_height) => {
                tracing::trace!(
                    target: "cloud_archival",
                    archived_height,
                    target_height,
                    "older height was archived - more archiving needed"
                );
            }
        }
        Ok(result)
    }

    /// If the min cloud head lags the hot final head, archive the next height.
    /// Only archives components whose individual heads are behind.
    async fn try_archive_data_impl(&self) -> Result<CloudArchivingResult, CloudArchivingError> {
        let min_head =
            self.get_cloud_min_head_local()?.expect("CLOUD_MIN_HEAD should exist in hot store");
        let height_to_archive = min_head + 1;
        let hot_final_height = self.get_hot_final_head_height()?;
        tracing::trace!(target: "cloud_archival", height_to_archive, hot_final_height, "try_archive");

        // Archive only while the height to archive is below the finalized height, since
        // the next block should be finalized first (for `DBCol::NextBlockHashes`).
        if height_to_archive >= hot_final_height {
            return Ok(CloudArchivingResult::NoHeightArchived(min_head));
        }

        self.archive_lagging_components(height_to_archive).await?;

        let result = if height_to_archive + 1 == hot_final_height {
            CloudArchivingResult::LatestHeightArchived(height_to_archive)
        } else {
            CloudArchivingResult::OlderHeightArchived(height_to_archive, hot_final_height - 1)
        };
        tracing::trace!(target: "cloud_archival", ?result, "ending");
        Ok(result)
    }

    /// Archives all lagging components at the given height and advances local heads.
    async fn archive_lagging_components(
        &self,
        height: BlockHeight,
    ) -> Result<(), CloudArchivingError> {
        let block_hash = self.hot_store.chain_store().get_block_hash_by_height(height)?;
        let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
        let tracked_shards =
            self.shard_tracker.get_tracked_shards_for_non_validator_in_epoch(&epoch_id)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;

        let block_advanced = if self.config.archive_block_data {
            self.archive_block_and_epoch_if_lagging(height, &block_hash, epoch_id, &shard_layout)
                .await?
        } else {
            false
        };
        let advanced_shards =
            self.archive_shards_if_lagging(height, &tracked_shards, &shard_layout).await?;
        self.advance_local_heads(height, block_advanced, &advanced_shards)?;
        Ok(())
    }

    /// Archives block and epoch data if the local block head is behind `height`.
    /// Epoch data is uploaded at epoch boundaries since it is keyed by epoch ID
    /// and naturally belongs with the last block of the epoch.
    /// Returns true if the block head was advanced.
    async fn archive_block_and_epoch_if_lagging(
        &self,
        height: BlockHeight,
        block_hash: &CryptoHash,
        epoch_id: EpochId,
        shard_layout: &ShardLayout,
    ) -> Result<bool, CloudArchivingError> {
        if let Some(head) = self.get_local_block_head()? {
            if head >= height {
                return Ok(false);
            }
        }
        if self.epoch_manager.is_next_block_epoch_start(block_hash)? {
            self.cloud_storage.archive_epoch_data(&self.hot_store, shard_layout, epoch_id).await?;
        }
        self.cloud_storage.archive_block_data(&self.hot_store, height).await?;
        self.cloud_storage.update_cloud_block_head(height).await?;
        Ok(true)
    }

    /// Archives shard data for tracked shards whose local head is behind `height`.
    /// Returns the shard IDs that were advanced.
    async fn archive_shards_if_lagging(
        &self,
        height: BlockHeight,
        tracked_shards: &[ShardUId],
        shard_layout: &ShardLayout,
    ) -> Result<Vec<ShardId>, CloudArchivingError> {
        let mut advanced_shards = Vec::new();
        for shard_uid in tracked_shards {
            let shard_id = shard_uid.shard_id();
            let lagging = match self.get_local_shard_head(shard_id)? {
                Some(head) => head < height,
                None => true,
            };
            if !lagging {
                continue;
            }
            self.cloud_storage
                .archive_shard_data(
                    &self.hot_store,
                    self.genesis_height,
                    shard_layout,
                    height,
                    *shard_uid,
                )
                .await?;
            self.cloud_storage.update_cloud_shard_head(shard_id, height).await?;
            advanced_shards.push(shard_id);
        }
        Ok(advanced_shards)
    }

    /// Initializes cloud heads by reconciling external and local state.
    /// Missing components start at `hot_final_height`. `CLOUD_MIN_HEAD` is
    /// clamped to `hot_final_height` because it is stored as `Tip` and
    /// needs a block header.
    // TODO(cloud_archival) Cover this logic with tests.
    async fn initialize_cloud_heads(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
    ) -> Result<(), CloudArchivalInitializationError> {
        let hot_final_height = self.get_hot_final_head_height()?;
        // TODO(cloud_archival): support resharding
        let tracked_shard_ids = self.get_tracked_shard_ids(hot_final_height)?;

        let (block_head_ext, shard_heads_ext) =
            self.read_external_heads(&tracked_shard_ids).await?;

        let (block_head_local, shard_heads_local, min_height_local) =
            self.collect_resolved_heads(hot_final_height, block_head_ext, &shard_heads_ext);

        self.ensure_cloud_head_available_for_archiving(runtime_adapter, min_height_local)?;
        self.log_initialization_status(block_head_ext, &shard_heads_ext, hot_final_height);
        self.set_local_heads(block_head_local, &shard_heads_local, min_height_local)?;

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

    /// Logs the initialization status based on external head presence.
    fn log_initialization_status(
        &self,
        block_head_ext: Option<BlockHeight>,
        shard_heads_ext: &[(ShardId, Option<BlockHeight>)],
        hot_final_height: BlockHeight,
    ) {
        // block_head_ext is None both when blocks aren't tracked and when
        // the external head is missing, so we need the config check for has_missing.
        let has_existing =
            block_head_ext.is_some() || shard_heads_ext.iter().any(|&(_, head)| head.is_some());
        let has_missing = (self.config.archive_block_data && block_head_ext.is_none())
            || shard_heads_ext.iter().any(|&(_, head)| head.is_none());

        if has_missing && has_existing {
            tracing::info!(
                target: "cloud_archival",
                start_height = hot_final_height - 1,
                "some external heads missing, new components start at hot_final_height - 1",
            );
        } else if has_missing {
            tracing::info!(
                target: "cloud_archival",
                start_height = hot_final_height - 1,
                "no external heads found, initializing new cloud archive",
            );
        } else {
            tracing::info!(
                target: "cloud_archival",
                "all external heads present, syncing from external",
            );
        }
    }

    /// Resolves each external head to its final local height and computes the
    /// overall minimum.  Missing components default to `hot_final_height - 1`
    /// — the last height that can actually be archived.  Callers guarantee
    /// `hot_final_height > genesis_height` so this is always a valid height.
    /// `min_height_local` is clamped to `hot_final_height` because
    /// `CLOUD_MIN_HEAD` is stored as `Tip` and needs a block header.
    fn collect_resolved_heads(
        &self,
        hot_final_height: BlockHeight,
        block_head_ext: Option<BlockHeight>,
        shard_heads_ext: &[(ShardId, Option<BlockHeight>)],
    ) -> (Option<BlockHeight>, Vec<(ShardId, BlockHeight)>, BlockHeight) {
        assert!(
            hot_final_height > self.genesis_height,
            "collect_resolved_heads called before node synced past genesis"
        );
        // The highest archivable height is hot_final_height - 1 (the loop
        // only archives at heights strictly below hot_final_height).
        let default_height = hot_final_height - 1;

        let mut min_height_local: Option<BlockHeight> = None;
        let mut update_min = |height: BlockHeight| {
            min_height_local = Some(min_height_local.map_or(height, |cur| cur.min(height)));
        };

        let block_head_local = if self.config.archive_block_data {
            let height = block_head_ext.unwrap_or(default_height);
            update_min(height);
            Some(height)
        } else {
            None
        };
        let shard_heads_local: Vec<(ShardId, BlockHeight)> = shard_heads_ext
            .iter()
            .map(|&(shard_id, ext_head)| {
                let height = ext_head.unwrap_or(default_height);
                update_min(height);
                (shard_id, height)
            })
            .collect();

        let min_height_local = min_height_local.expect("writer must track at least one component");
        let min_height_local = min_height_local.min(hot_final_height);

        (block_head_local, shard_heads_local, min_height_local)
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

    /// Ensures the cloud min head has not been garbage collected.
    /// We check against `gc_stop_height` (not just `gc_tail`) because the
    /// writer needs data from the entire epoch containing `min_head` -
    /// `gc_stop_height` is the earliest height whose epoch data is guaranteed
    /// to be retained.
    fn ensure_cloud_head_available_for_archiving(
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
        assert!(min_head < hot_final_height, "guaranteed by collect_resolved_heads");
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

    /// Returns the locally stored cloud min head height, if any.
    fn get_cloud_min_head_local(&self) -> io::Result<Option<BlockHeight>> {
        Ok(self
            .hot_store
            .get_ser::<Tip>(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY)
            .map(|tip| tip.height))
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
    /// Block and shard heads are stored as `BlockHeight` (may be above
    /// `hot_final_height` when catching up to another writer's external heads).
    /// `CLOUD_MIN_HEAD` is stored as `Tip` because external readers (GC, state
    /// sync) need `last_block_hash` and `epoch_id`.
    fn set_local_heads(
        &self,
        block_head: Option<BlockHeight>,
        shard_heads: &[(ShardId, BlockHeight)],
        min_height: BlockHeight,
    ) -> Result<(), near_chain_primitives::Error> {
        let mut transaction = DBTransaction::new();

        if let Some(block_head) = block_head {
            let height_bytes = borsh::to_vec(&block_head).unwrap();
            transaction.set(DBCol::BlockMisc, CLOUD_BLOCK_HEAD_KEY.to_vec(), height_bytes);
        }

        for &(shard_id, height) in shard_heads {
            let height_bytes = borsh::to_vec(&height).unwrap();
            transaction.set(DBCol::BlockMisc, cloud_shard_head_key(shard_id), height_bytes);
        }

        // CLOUD_MIN_HEAD needs a Tip (with block hash and epoch ID) for GC and state sync.
        let header = self.hot_store.chain_store().get_block_header_by_height(min_height)?;
        let tip_bytes = borsh::to_vec(&Tip::from_header(&header)).unwrap();
        transaction.set(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY.to_vec(), tip_bytes);

        self.hot_store.database().write(transaction);
        Ok(())
    }

    /// Advances local heads after archiving at `height`. Only updates heads for
    /// components that were actually behind. Always advances CLOUD_MIN_HEAD.
    fn advance_local_heads(
        &self,
        height: BlockHeight,
        block_advanced: bool,
        advanced_shard_ids: &[ShardId],
    ) -> Result<(), near_chain_primitives::Error> {
        let height_bytes = borsh::to_vec(&height).unwrap();
        let header = self.hot_store.chain_store().get_block_header_by_height(height)?;
        let tip_bytes = borsh::to_vec(&Tip::from_header(&header)).unwrap();

        let mut transaction = DBTransaction::new();
        if block_advanced {
            transaction.set(DBCol::BlockMisc, CLOUD_BLOCK_HEAD_KEY.to_vec(), height_bytes.clone());
        }
        for &shard_id in advanced_shard_ids {
            transaction.set(DBCol::BlockMisc, cloud_shard_head_key(shard_id), height_bytes.clone());
        }
        transaction.set(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY.to_vec(), tip_bytes);
        self.hot_store.database().write(transaction);
        Ok(())
    }
}
