use crate::near_chain_primitives::error::BlockKnownError::KnownInProcessing;
use crate::Provenance;
use near_primitives::block::Block;
use near_primitives::challenge::ChallengesResult;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ReceiptProof, StateSyncInfo};
use near_primitives::types::ShardId;
use std::collections::HashMap;

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
}

/// Blocks which finished pre-processing and are now being applied asynchronously
pub(crate) struct BlocksInProcessing {
    preprocessed_blocks: HashMap<CryptoHash, (Block, BlockPreprocessInfo)>,
}

#[derive(Debug)]
pub enum AddError {
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
}
