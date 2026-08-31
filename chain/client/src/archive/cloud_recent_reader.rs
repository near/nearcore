use crate::archive::cloud_archival_utils::{
    CloudArchivalReaderError, pull_block_batch, save_reader_head,
};
use near_async::time::{Clock, Duration};
use near_chain_configs::InterruptHandle;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::BlockHeight;
use near_store::Store;
use near_store::adapter::cloud_archival_store::CloudReaderHead;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::archive::cloud_storage::CloudStorage;
use std::sync::Arc;

/// Result of one recent-reader iteration.
#[derive(Debug)]
enum PullOutcome {
    /// Took a batch and moved the reader head to this.
    Pulled { head: CloudReaderHead },
    /// The bucket holds no block past the reader's head.
    WaitingForBlocks,
}

/// Reads recent chain data out of cloud storage into a local store.
#[derive(Clone)]
pub struct CloudArchivalRecentReader {
    clock: Clock,
    store: Store,
    cloud_storage: Arc<CloudStorage>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    polling_interval: Duration,
    interrupt: InterruptHandle,
}

impl CloudArchivalRecentReader {
    pub fn new(
        clock: Clock,
        store: Store,
        cloud_storage: Arc<CloudStorage>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        polling_interval: Duration,
    ) -> Self {
        Self {
            clock,
            store,
            cloud_storage,
            epoch_manager,
            polling_interval,
            interrupt: InterruptHandle::new(),
        }
    }

    /// Takes the store over on the first run, otherwise returns where the reader resumes.
    ///
    /// A store a stopped node handed over carries no reader head yet and takes that
    /// node's final head, since its own head can sit on a block that later reorged.
    /// Taking it over also drops the height index above that head.
    fn ensure_reader_head(&self) -> Result<CloudReaderHead, CloudArchivalReaderError> {
        if let Some(reader_head) = self.store.cloud_archival_store().reader_head() {
            return Ok(reader_head);
        }
        let final_head = self.store.chain_store().final_head()?;
        let header_head = self.store.chain_store().header_head()?;
        self.clear_height_index_range(final_head.height, header_head.height);
        Ok(save_reader_head(&self.store, final_head.height, final_head.last_block_hash))
    }

    /// Deletes the height index rows in `(final_height, header_height]`. Above the final
    /// head, a handed-over store can hold a fork the finalized chain dropped.
    fn clear_height_index_range(&self, final_height: BlockHeight, header_height: BlockHeight) {
        let mut update = self.store.store_update();
        for height in final_height + 1..=header_height {
            update.chain_store_update().delete_block_height(height);
        }
        update.commit();
    }

    /// Stops the loop after the iteration in flight.
    pub fn stop(&self) {
        self.interrupt.stop();
    }

    /// Follows the bucket, copying what it holds into the local store, until interrupted.
    pub async fn cloud_archival_loop(self) -> Result<(), CloudArchivalReaderError> {
        let mut reader_head = self.ensure_reader_head()?;
        tracing::info!(target: "cloud_archival", ?reader_head, "following the cloud archive");

        while !self.interrupt.is_cancelled() {
            let sleep_duration = match self.try_pull_next_batch(&reader_head) {
                Ok(outcome) => {
                    tracing::trace!(target: "cloud_archival", ?outcome, "pull");
                    match outcome {
                        PullOutcome::Pulled { head } => {
                            reader_head = head;
                            Duration::ZERO
                        }
                        PullOutcome::WaitingForBlocks => self.polling_interval,
                    }
                }
                Err(error) => {
                    tracing::error!(target: "cloud_archival", ?error, "pulling a batch failed");
                    self.polling_interval
                }
            };
            self.clock.sleep(sleep_duration).await;
        }
        tracing::debug!(target: "cloud_archival", "stopping the recent reader");
        Ok(())
    }

    /// Takes the batches after `reader_head`, once the bucket has them.
    fn try_pull_next_batch(
        &self,
        reader_head: &CloudReaderHead,
    ) -> Result<PullOutcome, CloudArchivalReaderError> {
        let cloud_block_head = self.cloud_storage.get_cloud_block_head()?;
        if !cloud_block_head.is_some_and(|cloud_head| cloud_head > reader_head.height) {
            return Ok(PullOutcome::WaitingForBlocks);
        }
        let batch_pull = pull_block_batch(
            &self.store,
            &self.cloud_storage,
            self.epoch_manager.as_ref(),
            reader_head.height + 1,
        )?;
        // TODO(cloud_archival): write the batch's shard rows here once every shard it
        // covers has published its cloud head.
        let last_present_block_hash =
            batch_pull.last_present_block_hash.unwrap_or(reader_head.last_present_block_hash);
        let head = save_reader_head(&self.store, batch_pull.end_height, last_present_block_hash);
        Ok(PullOutcome::Pulled { head })
    }
}
