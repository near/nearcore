use crate::archive::cloud_storage::{
    BlockData, CloudRetrievalError, CloudStorage, ShardBatch, ShardData,
};
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};
use std::future::Future;

/// Single-item fetches, and the state header, in the blocking form tests call them in.
/// Production callers go through the batch fetches on `CloudStorage`, so consecutive-height
/// loops reuse a batch, and await them rather than blocking a thread on the retrieval.
impl CloudStorage {
    pub fn get_state_header(
        &self,
        epoch_height: EpochHeight,
        epoch_id: EpochId,
        shard_id: ShardId,
    ) -> Result<ShardStateSyncResponseHeader, CloudRetrievalError> {
        block_on_future(self.retrieve_state_header(epoch_height, epoch_id, shard_id))
    }

    /// One block's data. `Ok(None)` when the height has no block, a skipped slot.
    pub fn get_block_data(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<BlockData>, CloudRetrievalError> {
        let batch = block_on_future(self.get_block_batch_for_height(block_height))?;
        Ok(batch.get_block_at_height(block_height).cloned())
    }

    /// The batch of `shard_id` data covering `block_height`, rejecting one that opens
    /// above or ends below it.
    pub fn get_shard_batch_for_height(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<ShardBatch, CloudRetrievalError> {
        let batch = block_on_future(self.get_shard_batch(block_height, shard_id))?;
        if block_height < batch.start_height() || block_height > batch.end_height() {
            return Err(CloudRetrievalError::NoShardData { height: block_height, shard_id });
        }
        Ok(batch)
    }

    /// One shard's data at one height. `Ok(None)` when the height has no block.
    pub fn get_shard_data(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<Option<ShardData>, CloudRetrievalError> {
        let batch = self.get_shard_batch_for_height(block_height, shard_id)?;
        Ok(batch.get_data_at_height(block_height).cloned())
    }
}

fn block_on_future<F: Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}
