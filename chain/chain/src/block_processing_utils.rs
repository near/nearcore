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

/// BlockProcessingPool is used to keep track of blocks have been preprocessed but
/// haven't been processed fully yet. It is needed because the applying of blocks
/// (processing transactions and receipts) are done in a separate thread from the
/// ClientActor thread, where blocks are preprocessed and changes from applying blocks
/// are stored. This struct is thread-safe.
/// The reason to use the BlockLike trait is to make testing easier.
pub(crate) struct BlockProcessingPool {
    preprocessed_blocks: HashMap<CryptoHash, (Block, BlockPreprocessInfo)>,
}

#[derive(Debug)]
pub(crate) enum Error {
    ExceedingPoolSize,
    BlockNotPreprocessed,
}

impl BlockProcessingPool {
    pub(crate) fn new() -> Self {
        BlockProcessingPool { preprocessed_blocks: HashMap::new() }
    }

    /// Add a preprocessed block to the pool. Return Error::ExceedingPoolSize if the pool already
    /// reaches its max size.
    pub(crate) fn add_preprocessed_block(
        &mut self,
        block: Block,
        preprocess_info: BlockPreprocessInfo,
    ) -> Result<(), Error> {
        if self.preprocessed_blocks.len() >= MAX_PROCESSING_BLOCKS {
            return Err(Error::ExceedingPoolSize);
        }

        self.preprocessed_blocks.insert(*block.hash(), (block, preprocess_info));
        Ok(())
    }

    pub(crate) fn take_preprocess_block(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<(Block, BlockPreprocessInfo), Error> {
        self.preprocessed_blocks.remove(block_hash).ok_or(Error::BlockNotPreprocessed)
    }

    /// Returns whether the block has been added as preprocessed
    pub(crate) fn is_block_preprocessed(&self, block_hash: &CryptoHash) -> bool {
        self.preprocessed_blocks.contains_key(block_hash)
    }

    /// Returns whether the pool is full
    pub(crate) fn is_full(&self) -> bool {
        self.preprocessed_blocks.len() >= MAX_PROCESSING_BLOCKS
    }
}
