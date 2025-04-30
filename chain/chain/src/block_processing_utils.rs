use crate::Provenance;
use crate::chain::BlockMissingChunks;
use crate::near_chain_primitives::error::BlockKnownError::KnownInProcessing;
use crate::orphan::OrphanMissingChunks;
use near_async::time::Instant;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::optimistic_block::{BlockToApply, CachedShardUpdateKey, OptimisticBlock};
use near_primitives::sharding::{ReceiptProof, ShardChunkHeader, StateSyncInfo};
use near_primitives::types::{BlockHeight, ShardId};
use std::collections::HashMap;
use std::sync::Arc;

/// Max number of blocks that can be in the pool at once.
/// This number will likely never be hit unless there are many forks in the chain.
pub(crate) const MAX_PROCESSING_BLOCKS: usize = 5;

/// Contains information from preprocessing a block
pub(crate) struct BlockPreprocessInfo {
    /// This field has two related but actually different meanings. For the first block of an
    /// epoch, this will be set to false if we need to download state for shards we'll track in
    /// the future but don't track currently. This implies the first meaning, which is that if
    /// this is true, then we are ready to apply all chunks and update flat state for shards
    /// we'll track in this and the next epoch. This comes into play when we decide what ApplyChunksMode
    /// to pass to Chain::apply_chunks_preprocessing().
    /// The other meaning is that the catchup code should process this block. When the state sync sync_hash
    /// is the first block of the epoch, these two meanings are the same. But if the sync_hash is moved forward
    /// in order to sync the current epoch's state instead of last epoch's, this field being false no longer implies
    /// that we want to apply this block during catchup, so some care is needed to ensure we start catchup at the right
    /// point in Client::run_catchup()
    pub(crate) is_caught_up: bool,
    pub(crate) state_sync_info: Option<StateSyncInfo>,
    pub(crate) incoming_receipts: HashMap<ShardId, Vec<ReceiptProof>>,
    pub(crate) provenance: Provenance,
    /// Used to get notified when the applying chunks of a block finishes.
    pub(crate) apply_chunks_done_waiter: ApplyChunksDoneWaiter,
    /// Used to calculate block processing time metric.
    pub(crate) block_start_processing_time: Instant,
}

pub(crate) struct OptimisticBlockInfo {
    /// Used to get notified when the applying chunks of a block finishes.
    pub(crate) apply_chunks_done_waiter: ApplyChunksDoneWaiter,
    /// Used to calculate processing time metric.
    pub(crate) block_start_processing_time: Instant,
    /// Shard update keys for the processed chunks.
    pub(crate) shard_update_keys: Vec<CachedShardUpdateKey>,
}

/// Blocks which finished pre-processing and are now being applied asynchronously
pub(crate) struct BlocksInProcessing {
    // A map that stores all blocks in processing
    preprocessed_blocks: HashMap<CryptoHash, (Block, BlockPreprocessInfo)>,
    optimistic_blocks: HashMap<BlockHeight, (OptimisticBlock, OptimisticBlockInfo)>,
}

#[derive(Debug)]
pub(crate) enum AddError {
    ExceedingPoolSize,
    BlockAlreadyInPool,
}

impl From<AddError> for near_chain_primitives::Error {
    fn from(err: AddError) -> Self {
        match err {
            AddError::ExceedingPoolSize => near_chain_primitives::Error::TooManyProcessingBlocks,
            AddError::BlockAlreadyInPool => {
                near_chain_primitives::Error::BlockKnown(KnownInProcessing)
            }
        }
    }
}

/// Results from processing a block that are useful for client and client actor to use
/// for steps after a block is processed that can't be finished inside Chain after a block is processed
/// (for example, sending requests for missing chunks or challenges).
/// This struct is passed to Chain::process_block as an argument instead of returned as Result,
/// because the information stored here need to returned whether process_block succeeds or returns an error.
#[derive(Default)]
pub struct BlockProcessingArtifact {
    pub orphans_missing_chunks: Vec<OrphanMissingChunks>,
    pub blocks_missing_chunks: Vec<BlockMissingChunks>,
    pub invalid_chunks: Vec<ShardChunkHeader>,
}

#[derive(Debug)]
pub struct BlockNotInPoolError;

impl BlocksInProcessing {
    pub(crate) fn new() -> Self {
        BlocksInProcessing {
            preprocessed_blocks: HashMap::new(),
            optimistic_blocks: HashMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.preprocessed_blocks.len() + self.optimistic_blocks.len()
    }

    /// Add a preprocessed block to the pool. Return Error::ExceedingPoolSize if the pool already
    /// reaches its max size.
    pub(crate) fn add(
        &mut self,
        block: Block,
        preprocess_info: BlockPreprocessInfo,
    ) -> Result<(), AddError> {
        self.add_dry_run(&BlockToApply::Normal(*block.hash()))?;

        self.preprocessed_blocks.insert(*block.hash(), (block, preprocess_info));
        Ok(())
    }

    pub(crate) fn add_optimistic(
        &mut self,
        block: OptimisticBlock,
        preprocess_info: OptimisticBlockInfo,
    ) -> Result<(), AddError> {
        self.add_dry_run(&BlockToApply::Optimistic(block.height()))?;

        self.optimistic_blocks.insert(block.height(), (block, preprocess_info));
        Ok(())
    }

    pub(crate) fn contains(&self, block_to_apply: &BlockToApply) -> bool {
        match block_to_apply {
            BlockToApply::Normal(block_hash) => self.preprocessed_blocks.contains_key(block_hash),
            BlockToApply::Optimistic(block_height) => {
                self.optimistic_blocks.contains_key(block_height)
            }
        }
    }

    pub(crate) fn remove(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Option<(Block, BlockPreprocessInfo)> {
        self.preprocessed_blocks.remove(block_hash)
    }

    pub(crate) fn remove_optimistic(
        &mut self,
        block_height: &BlockHeight,
    ) -> Option<(OptimisticBlock, OptimisticBlockInfo)> {
        self.optimistic_blocks.remove(block_height)
    }

    /// This function does NOT add the block, it simply checks if the block can be added
    pub(crate) fn add_dry_run(&self, block_to_apply: &BlockToApply) -> Result<(), AddError> {
        // We set a limit to the max number of blocks that we will be processing at the same time.
        // Since processing a block requires that the its previous block is processed, this limit
        // is likely never hit, unless there are many forks in the chain.
        // In this case, we will simply drop the block.
        if self.len() >= MAX_PROCESSING_BLOCKS {
            Err(AddError::ExceedingPoolSize)
        } else if self.contains(block_to_apply) {
            Err(AddError::BlockAlreadyInPool)
        } else {
            Ok(())
        }
    }

    /// Check if there is an optimistic block in processing for the given
    /// height and shard update keys.
    pub fn has_optimistic_block_with(
        &self,
        block_height: BlockHeight,
        shard_update_keys: &[&CachedShardUpdateKey],
    ) -> bool {
        let Some((_, optimistic_block_info)) = self.optimistic_blocks.get(&block_height) else {
            return false;
        };
        let info_keys: Vec<&CachedShardUpdateKey> =
            optimistic_block_info.shard_update_keys.iter().collect();
        shard_update_keys == info_keys.as_slice()
    }

    pub(crate) fn has_blocks_to_catch_up(&self, prev_hash: &CryptoHash) -> bool {
        self.preprocessed_blocks
            .iter()
            .any(|(_, (block, _))| block.header().prev_hash() == prev_hash)
    }

    /// This function waits until apply_chunks_done is marked as true for all blocks in the pool
    /// Returns true if new blocks are done applying chunks
    pub(crate) fn wait_for_all_blocks(&self) -> bool {
        for (_, (_, block_preprocess_info)) in &self.preprocessed_blocks {
            let _ = block_preprocess_info.apply_chunks_done_waiter.wait();
        }
        for (_, (_, optimistic_block_info)) in &self.optimistic_blocks {
            let _ = optimistic_block_info.apply_chunks_done_waiter.wait();
        }
        !self.preprocessed_blocks.is_empty() || !self.optimistic_blocks.is_empty()
    }

    /// This function waits until apply_chunks_done is marked as true for block `block_hash`
    pub(crate) fn wait_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<(), BlockNotInPoolError> {
        let _ = self
            .preprocessed_blocks
            .get(block_hash)
            .ok_or(BlockNotInPoolError)?
            .1
            .apply_chunks_done_waiter
            .wait();
        Ok(())
    }
}

/// The waiter's wait() will block until the corresponding ApplyChunksStillApplying is dropped.
#[derive(Clone)]
pub struct ApplyChunksDoneWaiter(Arc<tokio::sync::Mutex<()>>);
pub struct ApplyChunksStillApplying {
    // We're using tokio's mutex guard, because the std one is not Send.
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl ApplyChunksDoneWaiter {
    pub fn new() -> (Self, ApplyChunksStillApplying) {
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        // Use try_lock_owned() rather than blocking_lock_owned(), because otherwise
        // this causes a panic if we do this on a tokio runtime.
        let guard = lock.clone().try_lock_owned().expect("should succeed on a fresh mutex");
        (ApplyChunksDoneWaiter(lock), ApplyChunksStillApplying { _guard: guard })
    }

    pub fn wait(&self) {
        // This would only go through if the guard has been dropped.
        drop(self.0.blocking_lock());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::ApplyChunksDoneWaiter;

    #[test]
    fn test_apply_chunks_with_multiple_waiters() {
        let shared_value: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        let (waiter, still_applying) = ApplyChunksDoneWaiter::new();
        let waiter1 = waiter.clone();
        let waiter2 = waiter.clone();
        let waiter3 = waiter;

        let (results_sender, results_receiver) = std::sync::mpsc::channel();

        // Spawn waiter tasks
        for waiter in [waiter1, waiter2, waiter3] {
            let current_sender = results_sender.clone();
            let current_shared_value = shared_value.clone();
            std::thread::spawn(move || {
                waiter.wait();
                let read_value = current_shared_value.load(Ordering::Relaxed);
                current_sender.send(read_value).unwrap();
            });
        }

        // Wait 300ms then set the shared_value to true, and notify the waiters.
        std::thread::sleep(Duration::from_millis(300));
        shared_value.store(true, Ordering::Relaxed);
        drop(still_applying);

        // Check values that waiters read
        for _ in 0..3 {
            let waiter_value = results_receiver.recv().unwrap();
            assert_eq!(waiter_value, true);
        }
    }
}
