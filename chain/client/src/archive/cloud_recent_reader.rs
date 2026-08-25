use crate::archive::cloud_archival_utils::{
    CloudArchivalReaderError, compute_initial_prev_epoch_end,
};
use near_async::time::{Clock, Duration};
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::InterruptHandle;
use near_epoch_manager::EpochManagerAdapter;
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

/// Takes the store a stopped node handed over, before the reader's loop starts.
///
/// The reader head goes onto that node's final head, since the node's own head can sit on
/// a block that later reorged.
pub fn take_over_store(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<(), CloudArchivalReaderError> {
    let store = chain_store.store();
    if store.cloud_archival_store().reader_head().is_some() {
        return Ok(());
    }
    let final_head = chain_store.final_head()?;
    let prev_epoch_end = compute_initial_prev_epoch_end(&store, epoch_manager, final_head.height)?;
    let mut update = store.cloud_archival_store().store_update();
    update.set_prev_epoch_end(prev_epoch_end);
    update.set_reader_head(final_head.height);
    update.commit();
    Ok(())
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

    /// The height every component is written through.
    fn reader_head(&self) -> Result<BlockHeight, CloudArchivalReaderError> {
        self.chain_store
            .store()
            .cloud_archival_store()
            .reader_head()
            .ok_or(CloudArchivalReaderError::NoReaderHead)
    }

    /// Stops the loop after the iteration in flight.
    pub fn stop(&self) {
        self.interrupt.stop();
    }

    /// Follows the bucket, copying what it holds into the local store, until interrupted.
    pub async fn cloud_archival_loop(mut self) -> Result<(), CloudArchivalReaderError> {
        let reader_head = self.reader_head()?;
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
