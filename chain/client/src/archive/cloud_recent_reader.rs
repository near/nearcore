use crate::archive::cloud_archival_utils::CloudArchivalReaderError;
use near_async::time::{Clock, Duration};
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::InterruptHandle;
use near_primitives::types::BlockHeight;
use near_store::adapter::StoreAdapter;

/// Result of one recent-reader iteration.
#[derive(Debug)]
enum PullOutcome {
    /// Took the batch ending at this height.
    // TODO(cloud_archival): drop the allow once the pull constructs this.
    #[allow(dead_code)]
    Pulled { batch_end: BlockHeight },
    /// The bucket holds nothing past the reader's head.
    WaitingForBucket,
}

/// Reads recent chain data out of cloud storage into a local store.
#[derive(Clone)]
pub struct CloudArchivalRecentReader {
    clock: Clock,
    chain_store: ChainStore,
    polling_interval: Duration,
    interrupt: InterruptHandle,
}

impl CloudArchivalRecentReader {
    pub fn new(clock: Clock, chain_store: ChainStore, polling_interval: Duration) -> Self {
        Self { clock, chain_store, polling_interval, interrupt: InterruptHandle::new() }
    }

    /// The height the reader resumes at. A store handed over by a stopped node carries
    /// none yet and takes its final head, since that node's head can still reorg.
    fn ensure_reader_head(&self) -> Result<BlockHeight, CloudArchivalReaderError> {
        let store = self.chain_store.store();
        if let Some(reader_head) = store.cloud_archival_store().reader_head() {
            return Ok(reader_head);
        }
        let final_head = self.chain_store.final_head()?;
        let mut update = store.cloud_archival_store().store_update();
        update.set_reader_head(final_head.height);
        update.commit();
        Ok(final_head.height)
    }

    /// Stops the loop after the iteration in flight.
    pub fn stop(&self) {
        self.interrupt.stop();
    }

    /// Follows the bucket, copying what it holds into the local store, until interrupted.
    pub async fn cloud_archival_loop(mut self) -> Result<(), CloudArchivalReaderError> {
        let reader_head = self.ensure_reader_head()?;
        tracing::info!(target: "cloud_archival", reader_head, "following the cloud archive");

        while !self.interrupt.is_cancelled() {
            let sleep_duration = match self.try_pull_next_batch() {
                Ok(outcome) => {
                    tracing::trace!(target: "cloud_archival", ?outcome, "pull");
                    match outcome {
                        PullOutcome::Pulled { .. } => Duration::ZERO,
                        PullOutcome::WaitingForBucket => self.polling_interval,
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

    /// Takes the batch after the reader head, when the bucket holds all of it.
    // TODO(cloud_archival): drop the allow once the pull writes through the store.
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn try_pull_next_batch(&mut self) -> Result<PullOutcome, CloudArchivalReaderError> {
        // TODO(cloud_archival): compute the batch at the reader head, take it when the
        // bucket covers it, and move the reader head onto its end.
        Ok(PullOutcome::WaitingForBucket)
    }
}
