use crate::chain::{BlockMissingChunks, OrphanMissingChunks};
use crate::near_chain_primitives::error::BlockKnownError::KnownInProcessing;
use crate::Provenance;
use near_primitives::block::Block;
use near_primitives::challenge::{ChallengeBody, ChallengesResult};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ReceiptProof, StateSyncInfo};
use near_primitives::types::ShardId;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Max number of blocks that can be in the pool at once.
/// This number will likely never be hit unless there are many forks in the chain.
pub(crate) const MAX_PROCESSING_BLOCKS: usize = 5;

/// Contains information from preprocessing a block
pub(crate) struct BlockPreprocessInfo {
    pub(crate) is_caught_up: bool,
    pub(crate) state_dl_info: Option<StateSyncInfo>,
    pub(crate) incoming_receipts: HashMap<ShardId, Vec<ReceiptProof>>,
    pub(crate) challenges_result: ChallengesResult,
    pub(crate) challenged_blocks: Vec<CryptoHash>,
    pub(crate) provenance: Provenance,
    /// This field will be set to true when the apply_chunks has finished.
    pub(crate) apply_chunks_done: Arc<OnceCell<bool>>,
}

/// Blocks which finished pre-processing and are now being applied asynchronously
pub(crate) struct BlocksInProcessing {
    preprocessed_blocks: HashMap<CryptoHash, (Block, BlockPreprocessInfo)>,
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
    pub challenges: Vec<ChallengeBody>,
}

pub type ApplyChunkCallback = Arc<dyn Fn(CryptoHash) -> () + Send + Sync + 'static>;

#[derive(Debug)]
pub struct BlockNotInPoolError;

impl BlocksInProcessing {
    pub(crate) fn new() -> Self {
        BlocksInProcessing { preprocessed_blocks: HashMap::new() }
    }

    /// Add a preprocessed block to the pool. Return Error::ExceedingPoolSize if the pool already
    /// reaches its max size.
    pub(crate) fn add(
        &mut self,
        block: Block,
        preprocess_info: BlockPreprocessInfo,
    ) -> Result<(), AddError> {
        self.add_dry_run(block.hash())?;

        self.preprocessed_blocks.insert(*block.hash(), (block, preprocess_info));
        Ok(())
    }

    pub(crate) fn remove(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Option<(Block, BlockPreprocessInfo)> {
        self.preprocessed_blocks.remove(block_hash)
    }

    /// This function does NOT add the block, it simply checks if the block can be added
    pub(crate) fn add_dry_run(&self, block_hash: &CryptoHash) -> Result<(), AddError> {
        // We set a limit to the max number of blocks that we will be processing at the same time.
        // Since processing a block requires that the its previous block is processed, this limit
        // is likely never hit, unless there are many forks in the chain.
        // In this case, we will simply drop the block.
        if self.preprocessed_blocks.len() >= MAX_PROCESSING_BLOCKS {
            Err(AddError::ExceedingPoolSize)
        } else if self.preprocessed_blocks.contains_key(block_hash) {
            Err(AddError::BlockAlreadyInPool)
        } else {
            Ok(())
        }
    }

    /// This function waits until apply_chunks_done is marked as true for all blocks in the pool
    /// Returns true if the pool is empty
    pub(crate) fn wait_for_all_block(&self) -> bool {
        for (_, (_, block_preprocess_info)) in self.preprocessed_blocks.iter() {
            let _ = block_preprocess_info.apply_chunks_done.wait();
        }
        self.preprocessed_blocks.is_empty()
    }

    /// This function wait until apply_chunks_done is marked as true for block `block_hash`
    pub(crate) fn wait_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<(), BlockNotInPoolError> {
        let _ = self
            .preprocessed_blocks
            .get(block_hash)
            .ok_or(BlockNotInPoolError)?
            .1
            .apply_chunks_done
            .wait();
        Ok(())
    }
}
