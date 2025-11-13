use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::block::Block;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::ExecutionOutcomeWithProof;
use near_primitives::types::BlockHeight;
use near_schema_checker_lib::ProtocolSchema;

use crate::Store;
use crate::adapter::StoreAdapter;

/// Versioned container for block-related data stored in the cloud archival.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum BlockData {
    V1(BlockDataV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct BlockDataV1 {
    /// Read from `DBCol::Block`.
    block: Block,
    /// Read from `DBCol::BlockInfo`.
    block_info: BlockInfo,
    /// Read from `DBCol::NextBlockHashes`.
    next_block_hash: CryptoHash,
    /// Read from `DBCol::TransactionResultForBlock`.
    transaction_result_for_block: HashMap<CryptoHash, ExecutionOutcomeWithProof>,
}

/// Builds a `BlockData` object for the given block height by reading data from the store.
pub fn build_block_data(store: &Store, block_height: BlockHeight) -> Result<BlockData, Error> {
    let store = store.chain_store();
    let block_hash = store.get_block_hash_by_height(block_height)?;
    let block = (*store.get_block(&block_hash)?).clone();
    let block_info = store.epoch_store().get_block_info(&block_hash)?;
    let next_block_hash = store.get_next_block_hash(&block_hash)?;
    // TODO(cloud_archival) Read from `DBCol::TransactionResultForBlock`
    let transaction_result_for_block = HashMap::new();
    let block_data =
        BlockDataV1 { block, block_info, next_block_hash, transaction_result_for_block };
    Ok(BlockData::V1(block_data))
}

impl BlockData {
    pub fn get_block(&self) -> &Block {
        match self {
            BlockData::V1(data) => &data.block,
        }
    }
}
