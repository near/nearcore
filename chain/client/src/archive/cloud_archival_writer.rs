//! Cloud archival writer: moves finalized data from the hot store to the cloud storage.
//! Runs in a loop until the cloud head catches up with the hot final head.
use std::io;
use std::sync::Arc;

use futures::FutureExt;
use near_async::futures::FutureSpawner;
use near_async::time::Clock;
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain_configs::{CloudArchivalWriterConfig, InterruptHandle};
use near_primitives::types::BlockHeight;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::archive::cloud_storage::download::CloudRetrievalError;
use near_store::archive::cloud_storage::upload::CloudArchivingError;
use near_store::db::{CLOUD_HEAD_KEY, DBTransaction};
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
        "Cloud head is present locally ({cloud_head_local}) but it is missing externally.\n\
            Please make sure you use the correct cloud archive location, or delete CLOUD_HEAD from the local database"
    )]
    MissingExternalHead { cloud_head_local: BlockHeight },
    #[error(
        "Local cloud head ({cloud_head_local}) is ahead of the external head ({cloud_head_external}).\n\
        This indicates an invalid state. Please ensure the correct cloud archive is used or reset the local CLOUD_HEAD."
    )]
    LocalHeadAboveExternal { cloud_head_local: BlockHeight, cloud_head_external: BlockHeight },
    #[error(
        "GC tail: {gc_tail}, exceeds GC stop height: {gc_stop_height} for the cloud head: {cloud_head}"
    )]
    CloudHeadTooOld { cloud_head: BlockHeight, gc_stop_height: BlockHeight, gc_tail: BlockHeight },
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
) -> anyhow::Result<Option<CloudArchivalWriterHandle>> {
    let Some(config) = writer_config else {
        tracing::debug!(target: "cloud_archival", "not creating the cloud archival writer because it is not configured");
        return Ok(None);
    };
    let cloud_storage = cloud_storage
        .expect("Cloud archival writer is configured but cloud storage was not initialized.");
    let writer =
        CloudArchivalWriter::new(clock, config, genesis_height, hot_store, cloud_storage.clone());
    let handle = writer.handle.clone();
    tracing::info!(target: "cloud_archival", "starting the cloud archival writer");
    future_spawner.spawn_boxed("cloud_archival_writer", writer.start(runtime_adapter).boxed());
    Ok(Some(handle))
}

impl CloudArchivalWriter {
    fn new(
        clock: Clock,
        config: CloudArchivalWriterConfig,
        genesis_height: BlockHeight,
        hot_store: Store,
        cloud_storage: Arc<CloudStorage>,
    ) -> Self {
        let handle = CloudArchivalWriterHandle::new();
        Self { clock, config, genesis_height, hot_store, cloud_storage, handle }
    }

    async fn start(self, runtime_adapter: Arc<dyn RuntimeAdapter>) {
        if let Err(error) = self.initialize_cloud_head(&runtime_adapter).await {
            tracing::error!(target: "cloud_archival", ?error, "cloud archival initialization failed");
            return;
        }
        self.cloud_archival_loop().await;
    }

    /// Main loop: archive as fast as possible until `cloud_head == hot_final_head`, then
    /// sleep for `polling_interval` before trying again.
    async fn cloud_archival_loop(self) {
        while !self.handle.0.is_cancelled() {
            let result = self.try_archive_data().await;

            let duration = if let Ok(CloudArchivingResult::OlderHeightArchived(..)) = result {
                Duration::ZERO
            } else {
                self.config.polling_interval
            };
            self.clock.sleep(duration).await;
        }
        tracing::debug!(target: "cloud_archival", "stopping the cloud archival loop");
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

    /// If the cloud head lags the hot final head, archive the next height. Updates
    /// `cloud_head` on success.
    async fn try_archive_data_impl(&self) -> Result<CloudArchivingResult, CloudArchivingError> {
        let cloud_head =
            self.get_cloud_head_local()?.expect("CLOUD_HEAD should exist in hot store");
        let height_to_archive = cloud_head + 1;
        let hot_final_height = self.get_hot_final_head_height()?;
        tracing::trace!(target: "cloud_archival", height_to_archive, hot_final_height, "try_archive");

        // Archive only while the height to archive is below the finalized height, since
        // the next block should be finalized first (for `DBCol::NextBlockHashes`).
        if height_to_archive >= hot_final_height {
            return Ok(CloudArchivingResult::NoHeightArchived(cloud_head));
        }

        self.archive_data(height_to_archive).await?;
        self.update_cloud_head(height_to_archive).await?;

        let result = if height_to_archive + 1 == hot_final_height {
            Ok(CloudArchivingResult::LatestHeightArchived(height_to_archive))
        } else {
            Ok(CloudArchivingResult::OlderHeightArchived(height_to_archive, hot_final_height - 1))
        };
        tracing::trace!(target: "cloud_archival", ?result, "ending");
        result
    }

    /// Persist finalized data for `height` to cloud storage.
    async fn archive_data(&self, height: BlockHeight) -> Result<(), CloudArchivingError> {
        self.cloud_storage.archive_block_data(&self.hot_store, height).await?;
        // TODO(cloud_archival) Archive chunk data
        Ok(())
    }

    /// Advance the cloud archival head to `new_head` after a successful upload.
    async fn update_cloud_head(&self, new_head: BlockHeight) -> Result<(), CloudArchivingError> {
        self.cloud_storage.update_cloud_head(new_head).await?;
        self.set_cloud_head_local(new_head)?;
        Ok(())
    }

    /// Initializes and reconciles the cloud head between local and external state. If both
    /// are missing – creates a new archive; if local is missing – sets from external; if
    /// external is missing – returns an error; if they differ – uses external and updates
    /// local.
    // TODO(cloud_archival) Cover this logic with tests.
    async fn initialize_cloud_head(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
    ) -> Result<(), CloudArchivalInitializationError> {
        let cloud_head_local = self.get_cloud_head_local()?;
        let cloud_head_external = self.cloud_storage.get_cloud_head_if_exists().await?;
        match (cloud_head_local, cloud_head_external) {
            (None, None) => {
                let hot_final_height = self.get_hot_final_head_height()?;
                tracing::info!(
                    target: "cloud_archival",
                    start_height = hot_final_height,
                    "cloud head is missing both locally and externally, initializing new cloud archive and writer",
                );
                self.initialize_new_cloud_archive_and_writer(hot_final_height).await?;
            }
            (None, Some(cloud_head_external)) => {
                tracing::info!(
                    target: "cloud_archival",
                    cloud_head_external,
                    "cloud head is missing locally, initializing new cloud archival writer",
                );
                self.update_cloud_writer_head(runtime_adapter, cloud_head_external)?;
            }
            (Some(cloud_head_local), None) => {
                return Err(CloudArchivalInitializationError::MissingExternalHead {
                    cloud_head_local,
                });
            }
            (Some(cloud_head_local), Some(cloud_head_external))
                if cloud_head_local < cloud_head_external =>
            {
                tracing::warn!(
                    target: "cloud_archival",
                    cloud_head_local,
                    cloud_head_external,
                    "external cloud head is ahead of the local head, syncing local to external",
                );
                self.update_cloud_writer_head(runtime_adapter, cloud_head_external)?;
            }
            (Some(cloud_head_local), Some(cloud_head_external))
                if cloud_head_local > cloud_head_external =>
            {
                return Err(CloudArchivalInitializationError::LocalHeadAboveExternal {
                    cloud_head_local,
                    cloud_head_external,
                });
            }
            (Some(cloud_head_local), Some(cloud_head_external)) => {
                assert_eq!(cloud_head_local, cloud_head_external);
                tracing::info!(
                    target: "cloud_archival",
                    cloud_head_local,
                    "cloud head is equal locally and externally",
                );
            }
        };
        Ok(())
    }

    /// Sets up a new external cloud archive and local cloud writer starting at
    /// `hot_final_height`. No GC-tail check is needed because we start from the current hot
    /// final head.
    async fn initialize_new_cloud_archive_and_writer(
        &self,
        hot_final_height: BlockHeight,
    ) -> Result<(), CloudArchivalInitializationError> {
        self.cloud_storage.update_cloud_head(hot_final_height).await?;
        self.set_cloud_head_local(hot_final_height)?;
        Ok(())
    }

    /// Updates the local cloud writer head to `cloud_head_external` after validating GC
    /// constraints.
    fn update_cloud_writer_head(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        cloud_head_external: BlockHeight,
    ) -> Result<(), CloudArchivalInitializationError> {
        self.ensure_cloud_head_available_for_archiving(runtime_adapter, cloud_head_external)?;
        self.set_cloud_head_local(cloud_head_external)?;
        Ok(())
    }

    /// Ensures `cloud_head` is not older than GC stop; returns `CloudHeadTooOld` otherwise.
    fn ensure_cloud_head_available_for_archiving(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        cloud_head: BlockHeight,
    ) -> Result<(), CloudArchivalInitializationError> {
        let block_hash = self.hot_store.chain_store().get_block_hash_by_height(cloud_head)?;
        let gc_stop_height = runtime_adapter.get_gc_stop_height(&block_hash);
        let gc_tail = self.hot_store.chain_store().tail()?;
        if gc_tail > gc_stop_height {
            return Err(CloudArchivalInitializationError::CloudHeadTooOld {
                cloud_head,
                gc_stop_height,
                gc_tail,
            });
        }
        Ok(())
    }

    /// Reads the hot final head height; falls back to `genesis_height` if unset.
    fn get_hot_final_head_height(&self) -> io::Result<BlockHeight> {
        let hot_final_head = self.hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
        let hot_final_head_height = hot_final_head.map_or(self.genesis_height, |tip| tip.height);
        Ok(hot_final_head_height)
    }

    /// Returns the locally stored cloud head, if any.
    fn get_cloud_head_local(&self) -> io::Result<Option<BlockHeight>> {
        let cloud_head_tip = self.hot_store.get_ser::<Tip>(DBCol::BlockMisc, CLOUD_HEAD_KEY)?;
        let cloud_head = cloud_head_tip.map(|tip| tip.height);
        Ok(cloud_head)
    }

    /// Writes the local CLOUD_HEAD in the hot DB.
    fn set_cloud_head_local(
        &self,
        new_head: BlockHeight,
    ) -> Result<(), near_chain_primitives::Error> {
        let cloud_head_header =
            self.hot_store.chain_store().get_block_header_by_height(new_head)?;
        let cloud_head_tip = Tip::from_header(&cloud_head_header);
        let mut transaction = DBTransaction::new();
        transaction.set(DBCol::BlockMisc, CLOUD_HEAD_KEY.to_vec(), borsh::to_vec(&cloud_head_tip)?);
        self.hot_store.database().write(transaction)?;
        Ok(())
    }
}
