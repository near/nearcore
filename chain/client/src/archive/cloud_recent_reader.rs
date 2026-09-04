use crate::archive::cloud_archival_utils::{
    CloudArchivalReaderError, apply_batch_state_changes, pull_block_batch, pull_shard_batch,
    save_reader_position, shard_state_anchor, shards_tracked_in_batch,
};
use crate::archive::cloud_reader_trie_utils::build_shard_tries;
use near_async::time::{Clock, Duration};
use near_chain_configs::InterruptHandle;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_store::adapter::cloud_archival_store::CloudReaderHead;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::archive::cloud_storage::CloudStorage;
use near_store::{ShardTries, ShardUId, Store};
use std::mem;
use std::sync::Arc;

/// Result of one recent-reader iteration.
#[derive(Debug)]
enum PullOutcome {
    /// Took the batches for the next window.
    Pulled { head: CloudReaderHead },
    /// The bucket holds no block past the reader's head.
    WaitingForBlocks,
    /// This shard's data for the next window is not in the bucket.
    WaitingForShard {
        #[allow(dead_code)] // Read through the derived Debug in the pull trace.
        shard_uid: ShardUId,
    },
}

/// The height range the reader is taking, held until the bucket carries every component
/// batch over it.
#[derive(Clone)]
struct PendingWindow {
    /// The window's last height.
    end_height: BlockHeight,
    /// The last block the reader holds at or below `end_height`.
    last_present_block_hash: CryptoHash,
    /// The window's shards the reader has not seen in the bucket.
    waiting_shards: Vec<ShardUId>,
    /// The window's shards that are ready to pull.
    ready_shards: Vec<ShardUId>,
}

impl PendingWindow {
    /// The shard the reader checks next, and the one it waits on when the bucket does not
    /// carry that shard over the window.
    fn waiting_shard(&self) -> Option<ShardUId> {
        self.waiting_shards.last().copied()
    }
}

/// Reads recent chain data out of cloud storage into a local store.
#[derive(Clone)]
pub struct CloudArchivalRecentReader {
    clock: Clock,
    store: Store,
    cloud_storage: Arc<CloudStorage>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    polling_interval: Duration,
    interrupt: InterruptHandle,
    pending_window: Option<PendingWindow>,
    tries: ShardTries,
}

impl CloudArchivalRecentReader {
    pub fn new(
        clock: Clock,
        store: Store,
        cloud_storage: Arc<CloudStorage>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        polling_interval: Duration,
    ) -> Self {
        let tries = build_shard_tries(&store);
        Self {
            clock,
            store,
            cloud_storage,
            epoch_manager,
            shard_tracker,
            polling_interval,
            interrupt: InterruptHandle::new(),
            pending_window: None,
            tries,
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
        save_reader_position(&self.store, final_head.height, final_head.last_block_hash)
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

    /// Reads the root every shard the reader tracks at its head stands at, so a store
    /// holding no state for one of them stops the reader here instead of on a poll it
    /// would go on repeating.
    fn check_state_anchors(
        &self,
        reader_head: &CloudReaderHead,
    ) -> Result<(), CloudArchivalReaderError> {
        let shard_uids = shards_tracked_in_batch(
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            &reader_head.last_present_block_hash,
            None,
        )?;
        for shard_uid in shard_uids {
            let state_root =
                shard_state_anchor(&self.tries, &reader_head.last_present_block_hash, shard_uid)?;
            // The row naming the root is written even where the state behind it is not, so
            // the root has to be read to tell the two apart.
            self.tries.get_trie_for_shard(shard_uid, state_root).retrieve_root_node()?;
        }
        Ok(())
    }

    /// Follows the bucket, copying what it holds into the local store, until interrupted.
    pub async fn cloud_archival_loop(mut self) -> Result<(), CloudArchivalReaderError> {
        let mut reader_head = self.ensure_reader_head()?;
        self.check_state_anchors(&reader_head)?;
        tracing::info!(target: "cloud_archival", ?reader_head, "following the cloud archive");

        while !self.interrupt.is_cancelled() {
            let sleep_duration = match self.try_pull_next_batch(&reader_head).await {
                Ok(outcome) => {
                    tracing::trace!(target: "cloud_archival", ?outcome, "pull");
                    match outcome {
                        PullOutcome::Pulled { head } => {
                            reader_head = head;
                            Duration::ZERO
                        }
                        PullOutcome::WaitingForBlocks => self.polling_interval,
                        PullOutcome::WaitingForShard { .. } => self.polling_interval,
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

    /// Takes the window after `reader_head`, once the bucket has its blocks and every
    /// shard the reader tracks over it.
    ///
    /// The reader head moves only once the whole window is in, so a run that stops or
    /// errors part way through one takes it again from that head.
    async fn try_pull_next_batch(
        &mut self,
        reader_head: &CloudReaderHead,
    ) -> Result<PullOutcome, CloudArchivalReaderError> {
        let Some(mut window) = self.get_pending_window(reader_head).await? else {
            return Ok(PullOutcome::WaitingForBlocks);
        };
        let Some(shard_uids) = self.check_shards_ready(reader_head, &mut window).await? else {
            // `check_shards_ready` returns `None` only with a shard still waiting.
            let shard_uid = window.waiting_shard().expect("a shard held the window back");
            self.pending_window = Some(window);
            return Ok(PullOutcome::WaitingForShard { shard_uid });
        };
        for shard_uid in shard_uids {
            let shard_batch = pull_shard_batch(
                &self.store,
                &self.cloud_storage,
                shard_uid,
                reader_head.height + 1,
            )
            .await?;
            // TODO(cloud_archival): anchor a shard a resharding added above the head.
            let state_root =
                shard_state_anchor(&self.tries, &reader_head.last_present_block_hash, shard_uid)?;
            // A reader that joined mid-batch has its head inside the batch, and the
            // heights below that head are applied already.
            apply_batch_state_changes(
                &self.tries,
                shard_uid,
                &shard_batch,
                reader_head.height + 1,
                state_root,
            )?;
        }
        let head =
            save_reader_position(&self.store, window.end_height, window.last_present_block_hash)?;
        Ok(PullOutcome::Pulled { head })
    }

    /// Hands back the window the reader holds, opening the one above `reader_head` and
    /// writing its block rows when it holds none. `None` when the bucket holds no block
    /// past that head.
    async fn get_pending_window(
        &mut self,
        reader_head: &CloudReaderHead,
    ) -> Result<Option<PendingWindow>, CloudArchivalReaderError> {
        if let Some(window) = self.pending_window.take() {
            return Ok(Some(window));
        }
        let cloud_block_head = self.cloud_storage.get_cloud_block_head().await?;
        if !cloud_block_head.is_some_and(|cloud_head| cloud_head > reader_head.height) {
            return Ok(None);
        }
        // A restart pulls this batch again and writes the same rows over themselves.
        let batch_pull = pull_block_batch(
            &self.store,
            &self.cloud_storage,
            self.epoch_manager.as_ref(),
            reader_head.height + 1,
        )
        .await?;
        let window_tracked_shards = shards_tracked_in_batch(
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            &reader_head.last_present_block_hash,
            batch_pull.opening_epoch_id,
        )?;
        let last_present_block_hash =
            batch_pull.last_present_block_hash.unwrap_or(reader_head.last_present_block_hash);
        let window = PendingWindow {
            end_height: batch_pull.end_height,
            last_present_block_hash,
            waiting_shards: window_tracked_shards.into_iter().collect(),
            ready_shards: Vec::new(),
        };
        Ok(Some(window))
    }

    /// Returns the window's shards once the bucket carries every one of them past
    /// `reader_head`, which is where a shard's batch over the window is complete.
    async fn check_shards_ready(
        &self,
        reader_head: &CloudReaderHead,
        window: &mut PendingWindow,
    ) -> Result<Option<Vec<ShardUId>>, CloudArchivalReaderError> {
        while let Some(shard_uid) = window.waiting_shard() {
            // TODO(cloud_archival): each head read lists the shard heads, a couple of
            // files. Solve it properly with a get that tells a missing blob from a
            // failed one.
            let cloud_shard_head =
                self.cloud_storage.get_cloud_shard_head(shard_uid.shard_id()).await?;
            if !cloud_shard_head.is_some_and(|cloud_head| cloud_head > reader_head.height) {
                return Ok(None);
            }
            window.waiting_shards.pop();
            window.ready_shards.push(shard_uid);
        }
        let ready_shards = mem::take(&mut window.ready_shards);
        Ok(Some(ready_shards))
    }
}
