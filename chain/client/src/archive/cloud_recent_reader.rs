use crate::archive::cloud_archival_utils::{
    CloudArchivalReaderError, pull_block_batch, save_reader_head,
};
use near_async::time::{Clock, Duration};
use near_chain_configs::InterruptHandle;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::BlockHeight;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::CloudStorage;
use std::sync::Arc;

/// Result of one recent-reader iteration.
#[derive(Debug)]
enum PullOutcome {
    /// Took the batches ending at this height.
    Pulled { batch_end: BlockHeight },
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

    /// The height the reader resumes at, taking the store over on the first run.
    ///
    /// A store a stopped node handed over carries no reader head yet and takes that
    /// node's final head, since its own head can sit on a block that later reorged.
    fn ensure_reader_head(&self) -> Result<BlockHeight, CloudArchivalReaderError> {
        if let Some(reader_head) = self.store.cloud_archival_store().reader_head() {
            return Ok(reader_head);
        }
        let final_head = self.store.chain_store().final_head()?;
        save_reader_head(&self.store, final_head.height);
        Ok(final_head.height)
    }

    /// Stops the loop after the iteration in flight.
    pub fn stop(&self) {
        self.interrupt.stop();
    }

    /// Follows the bucket, copying what it holds into the local store, until interrupted.
    pub async fn cloud_archival_loop(self) -> Result<(), CloudArchivalReaderError> {
        let mut reader_head = self.ensure_reader_head()?;
        tracing::info!(target: "cloud_archival", reader_head, "following the cloud archive");

        while !self.interrupt.is_cancelled() {
            let sleep_duration = match self.try_pull_next_batch(reader_head) {
                Ok(outcome) => {
                    tracing::trace!(target: "cloud_archival", ?outcome, "pull");
                    match outcome {
                        PullOutcome::Pulled { batch_end } => {
                            reader_head = batch_end;
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
        reader_head: BlockHeight,
    ) -> Result<PullOutcome, CloudArchivalReaderError> {
        let cloud_block_head = self.cloud_storage.get_cloud_block_head()?;
        if !cloud_block_head.is_some_and(|cloud_head| cloud_head > reader_head) {
            return Ok(PullOutcome::WaitingForBlocks);
        }
        let batch_end = pull_block_batch(
            &self.store,
            &self.cloud_storage,
            self.epoch_manager.as_ref(),
            reader_head + 1,
        )?;
        // TODO(cloud_archival): write the batch's shard rows here once they become
        // available.
        save_reader_head(&self.store, batch_end);
        Ok(PullOutcome::Pulled { batch_end })
    }
}
