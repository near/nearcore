//! Cloud archival actor: moves finalized data from the hot store to the cloud storage.
//! Runs in a loop until the cloud head catches up with the hot final head.
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
use near_chain::types::Tip;
use near_chain_configs::CloudArchivalWriterConfig;
use near_primitives::types::BlockHeight;
use near_store::{DBCol, FINAL_HEAD_KEY, NodeStorage, Store};
use time::Duration;

/// Result of a single archiving attempt.
#[derive(Debug)]
enum CloudArchivingResult {
    // Cloud head is at least the hot final head; nothing to do.
    // Contains the current cloud head.
    NoHeightArchived(BlockHeight),
    // Archived the current final head height; now up to date until a new block is finalized.
    // Contains the target (final) height that was archived.
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
}

impl From<std::io::Error> for CloudArchivingError {
    fn from(error: std::io::Error) -> Self {
        CloudArchivingError::IOError { message: error.to_string() }
    }
}

/// Actor responsible for copying finalized blocks to cloud storage and tracking the cloud head.
pub struct CloudArchivalActor {
    config: CloudArchivalWriterConfig,
    genesis_height: BlockHeight,
    hot_store: Store,
    cloud_head: BlockHeight,
    keep_going: Arc<AtomicBool>,
}

/// Creates the cloud archival actor and `keep_going` handle if cloud archival writer is configured.
pub fn create_cloud_archival_actor(
    cloud_archival_config: Option<CloudArchivalWriterConfig>,
    genesis_height: BlockHeight,
    storage: &NodeStorage,
) -> anyhow::Result<Option<(CloudArchivalActor, Arc<AtomicBool>)>> {
    let Some(config) = cloud_archival_config else {
        tracing::debug!(target : "cloud_archival", "Not creating the cloud archival actor because cloud archival writer is not configured");
        return Ok(None);
    };
    let hot_store = storage.get_hot_store();

    // TODO(cloud_archival) Retrieve the `cloud_head` from the external storage
    let cloud_head = hot_final_head_height(&hot_store, genesis_height)?;

    let keep_going = Arc::new(AtomicBool::new(true));
    let keep_going_clone = keep_going.clone();

    tracing::info!(target: "cloud_archival", "Creating the cloud archival actor");
    let actor = CloudArchivalActor { config, genesis_height, hot_store, cloud_head, keep_going };
    Ok(Some((actor, keep_going_clone)))
}

impl Actor for CloudArchivalActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target: "cloud_archival", "Starting the cloud archival actor");
        self.cloud_archival_loop(ctx);
    }
}

impl CloudArchivalActor {
    /// Main loop: archive as fast as possible until `cloud_head == hot_final_head`, then sleep for
    /// `polling_interval` before trying again.
    fn cloud_archival_loop(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if !self.keep_going.load(std::sync::atomic::Ordering::Relaxed) {
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
    fn try_archive_data(&mut self) -> anyhow::Result<CloudArchivingResult, CloudArchivingError> {
        // TODO(cloud_archival) Add metrics
        let result = self.try_archive_data_impl();

        let Ok(result) = result else {
            tracing::error!(target : "cloud_archival", ?result, "Archiving data to cloud failed");
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

    /// If the cloud head lags the hot final head, archive the next height. Updates `cloud_head` on success.
    fn try_archive_data_impl(
        &mut self,
    ) -> anyhow::Result<CloudArchivingResult, CloudArchivingError> {
        let _span = tracing::debug_span!(target: "cloud_archival", "cloud_archive").entered();

        let target_height = hot_final_head_height(&self.hot_store, self.genesis_height)?;
        if self.cloud_head >= target_height {
            return Ok(CloudArchivingResult::NoHeightArchived(self.cloud_head));
        }
        let height_to_archive = self.cloud_head + 1;
        self.archive_data(height_to_archive)?;
        self.update_cloud_archival_head(height_to_archive)?;

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
    fn archive_data(&self, _height: BlockHeight) -> anyhow::Result<(), CloudArchivingError> {
        Ok(())
    }

    /// Advance the cloud archival head to `new_head` after a successful upload.
    // TODO(cloud_archival) Update cloud head in the external storage
    fn update_cloud_archival_head(
        &mut self,
        new_head: BlockHeight,
    ) -> anyhow::Result<(), CloudArchivingError> {
        debug_assert_eq!(new_head, self.cloud_head + 1);
        self.cloud_head = new_head;
        Ok(())
    }
}

/// Read the hot store's final head height, defaulting to `genesis_height` if unset.
fn hot_final_head_height(
    hot_store: &Store,
    genesis_height: BlockHeight,
) -> io::Result<BlockHeight> {
    let hot_final_head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
    let hot_final_head_height = hot_final_head.map_or(genesis_height, |tip| tip.height);
    Ok(hot_final_head_height)
}
