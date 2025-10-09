//! Cloud archival writer: moves finalized data from the hot store to the cloud storage.
//! Runs in a loop until the cloud head catches up with the hot final head.
use std::io;
use std::sync::Arc;

use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain_configs::{CloudArchivalWriterConfig, CloudArchivalWriterHandle};
use near_primitives::types::BlockHeight;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::db::{CLOUD_HEAD_KEY, DBTransaction};
use near_store::{DBCol, FINAL_HEAD_KEY, Store};
use time::Duration;

/// Result of a single archiving attempt.
#[derive(Debug)]
enum CloudArchivingResult {
    // Cloud head is at least the hot final head; nothing to do. Contains the current
    // cloud head.
    NoHeightArchived(BlockHeight),
    // Archived the current final head height; now up to date until a new block is
    // finalized. Contains the target (final) height that was archived.
    LatestHeightArchived(BlockHeight),
    // Archived a height below the final head; more heights are immediately available.
    // Contains (archived_height, target_height).
    OlderHeightArchived(BlockHeight, BlockHeight),
}

/// Error surfaced while archiving data or performing sanity checks.
#[derive(thiserror::Error, Debug)]
enum CloudArchivingError {
    #[error("Cloud archiving IO error: {message}")]
    IOError { message: String },
    #[error("Cloud archiving chain error: {error}")]
    ChainError { error: near_chain_primitives::Error },
}

impl From<std::io::Error> for CloudArchivingError {
    fn from(error: std::io::Error) -> Self {
        CloudArchivingError::IOError { message: error.to_string() }
    }
}

impl From<near_chain_primitives::Error> for CloudArchivingError {
    fn from(error: near_chain_primitives::Error) -> Self {
        CloudArchivingError::ChainError { error }
    }
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
        "GC tail: {gc_tail}, exceeds GC stop height: {gc_stop_height} for the cloud head: {cloud_head}"
    )]
    CloudHeadTooOld { cloud_head: BlockHeight, gc_stop_height: BlockHeight, gc_tail: BlockHeight },
    #[error("Chain error: {error}")]
    ChainError { error: near_chain_primitives::Error },
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

/// Responsible for copying finalized blocks to cloud storage.
pub struct CloudArchivalWriter {
    config: CloudArchivalWriterConfig,
    genesis_height: BlockHeight,
    hot_store: Store,
    #[allow(unused)]
    cloud_storage: Arc<CloudStorage>,
    handle: CloudArchivalWriterHandle,
}

/// Creates the cloud archival writer if it is configured.
pub fn create_cloud_archival_writer(
    writer_config: Option<CloudArchivalWriterConfig>,
    genesis_height: BlockHeight,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    hot_store: Store,
    cloud_storage: Option<&Arc<CloudStorage>>,
) -> anyhow::Result<Option<CloudArchivalWriterHandle>> {
    let Some(config) = writer_config else {
        tracing::debug!(target: "cloud_archival", "Not creating the cloud archival writer because it is not configured");
        return Ok(None);
    };

    let cloud_storage = cloud_storage
        .expect("Cloud archival writer is configured but cloud storage was not initialized.");
    let writer = CloudArchivalWriter::new(config, genesis_height, hot_store, cloud_storage.clone());
    let handle = writer.handle.clone();
    tracing::info!(target: "cloud_archival", "Starting the cloud archival writer");
    writer.initialize_cloud_head(&runtime_adapter)?;
    Ok(Some(handle))
}

impl Actor for CloudArchivalWriter {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target: "cloud_archival", "Starting the cloud archival loop");
        self.cloud_archival_loop(ctx);
    }
}

impl CloudArchivalWriter {
    fn new(
        config: CloudArchivalWriterConfig,
        genesis_height: BlockHeight,
        hot_store: Store,
        cloud_storage: Arc<CloudStorage>,
    ) -> Self {
        let handle = CloudArchivalWriterHandle::new();
        Self { config, genesis_height, hot_store, cloud_storage, handle }
    }

    /// Main loop: archive as fast as possible until `cloud_head == hot_final_head`, then
    /// sleep for `polling_interval` before trying again.
    fn cloud_archival_loop(&self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if self.handle.is_cancelled() {
            tracing::debug!(target: "cloud_archival", "Stopping the cloud archival loop");
            return;
        }

        let result = self.try_archive_data();

        let duration = if let Ok(CloudArchivingResult::OlderHeightArchived(..)) = result {
            Duration::ZERO
        } else {
            self.config.polling_interval
        };

        ctx.run_later("cloud_archival_loop", duration, move |actor, ctx| {
            actor.cloud_archival_loop(ctx);
        });
    }

    /// Tries to archive one height and logs the outcome.
    fn try_archive_data(&self) -> Result<CloudArchivingResult, CloudArchivingError> {
        // TODO(cloud_archival) Add metrics
        let result = self.try_archive_data_impl();

        let Ok(result) = result else {
            tracing::error!(target: "cloud_archival", ?result, "Archiving data to cloud failed");
            return result;
        };

        match result {
            CloudArchivingResult::NoHeightArchived(cloud_head) => {
                tracing::trace!(
                    target: "cloud_archival",
                    cloud_head,
                    "No height was archived - cloud archival head is up to date"
                );
            }
            CloudArchivingResult::LatestHeightArchived(target_height) => {
                tracing::trace!(
                    target: "cloud_archival",
                    target_height,
                    "Latest height was archived"
                );
            }
            CloudArchivingResult::OlderHeightArchived(archived_height, target_height) => {
                tracing::trace!(
                    target: "cloud_archival",
                    archived_height,
                    target_height,
                    "Older height was archived - more archiving needed"
                );
            }
        }
        Ok(result)
    }

    /// If the cloud head lags the hot final head, archive the next height. Updates
    /// `cloud_head` on success.
    fn try_archive_data_impl(&self) -> Result<CloudArchivingResult, CloudArchivingError> {
        let _span = tracing::debug_span!(target: "cloud_archival", "cloud_archive").entered();

        let cloud_head =
            self.get_cloud_head_local()?.expect("CLOUD_HEAD should exist in hot store");
        let target_height = self.get_hot_final_head_height()?;
        tracing::trace!(target: "cloud_archival", target_height, "try_archive");
        if cloud_head >= target_height {
            return Ok(CloudArchivingResult::NoHeightArchived(cloud_head));
        }
        let height_to_archive = cloud_head + 1;
        self.archive_data(height_to_archive)?;
        self.update_cloud_head(height_to_archive)?;

        let result = if height_to_archive == target_height {
            Ok(CloudArchivingResult::LatestHeightArchived(target_height))
        } else {
            Ok(CloudArchivingResult::OlderHeightArchived(height_to_archive, target_height))
        };
        tracing::trace!(target: "cloud_archival", ?result, "ending");
        result
    }

    /// Persist finalized data for `height` to cloud storage.
    // TODO(cloud_archival): Implement
    fn archive_data(&self, _height: BlockHeight) -> Result<(), CloudArchivingError> {
        Ok(())
    }

    /// Advance the cloud archival head to `new_head` after a successful upload.
    fn update_cloud_head(&self, new_head: BlockHeight) -> Result<(), CloudArchivingError> {
        self.set_cloud_head_external(new_head)?;
        self.set_cloud_head_local(new_head)?;
        Ok(())
    }

    /// Initializes and reconciles the cloud head between local and external state. If both
    /// are missing – creates a new archive; if local is missing – sets from external; if
    /// external is missing – returns an error; if they differ – uses external and updates
    /// local.
    // TODO(cloud_archival) Cover this logic with tests.
    fn initialize_cloud_head(
        &self,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
    ) -> Result<(), CloudArchivalInitializationError> {
        let cloud_head_local = self.get_cloud_head_local()?;
        let cloud_head_external = self.get_cloud_head_external()?;
        match (cloud_head_local, cloud_head_external) {
            (None, None) => {
                let hot_final_height = self.get_hot_final_head_height()?;
                tracing::info!(
                    target: "cloud_archival",
                    start_height = hot_final_height,
                    "Cloud head is missing both locally and externally. Initializing new cloud archive and writer.",
                );
                self.initialize_new_cloud_archive_and_writer(hot_final_height)?;
            }
            (None, Some(cloud_head_external)) => {
                tracing::info!(
                    target: "cloud_archival",
                    cloud_head_external,
                    "Cloud head is missing locally. Initializing new cloud archival writer.",
                );
                self.update_cloud_writer_head(runtime_adapter, cloud_head_external)?;
            }
            (Some(cloud_head_local), None) => {
                return Err(CloudArchivalInitializationError::MissingExternalHead {
                    cloud_head_local,
                });
            }
            (Some(cloud_head_local), Some(cloud_head_external)) => {
                if cloud_head_local != cloud_head_external {
                    tracing::warn!(
                        target: "cloud_archival",
                        cloud_head_local,
                        cloud_head_external,
                        "Cloud head is different between the local and external version. Using the external version.",
                    );
                    self.update_cloud_writer_head(runtime_adapter, cloud_head_external)?;
                }
            }
        };
        Ok(())
    }

    /// Sets up a new external cloud archive and local cloud writer starting at
    /// `hot_final_height`. No GC-tail check is needed because we start from the current hot
    /// final head.
    fn initialize_new_cloud_archive_and_writer(
        &self,
        hot_final_height: BlockHeight,
    ) -> Result<(), CloudArchivalInitializationError> {
        self.set_cloud_head_external(hot_final_height)?;
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

    /// Returns the cloud head from external storage, if any.
    #[allow(unused)]
    fn get_cloud_head_external(&self) -> io::Result<Option<BlockHeight>> {
        // TODO(cloud_archival) Retrieve the `cloud_head` from the external storage
        Ok(None)
    }

    /// Persists the cloud head to external storage.
    #[allow(unused)]
    fn set_cloud_head_external(&self, new_head: BlockHeight) -> io::Result<()> {
        // TODO(cloud_archival) Set the `cloud_head` to the external storage
        Ok(())
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
