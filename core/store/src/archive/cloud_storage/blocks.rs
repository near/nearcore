use crate::Store;
use crate::adapter::StoreAdapter;
use crate::archive::cloud_storage::batch::BatchRange;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::block::Block;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::ExecutionOutcomeWithProof;
use near_primitives::types::BlockHeight;
use near_schema_checker_lib::ProtocolSchema;
use std::collections::HashMap;

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
    pub fn block(&self) -> &Block {
        match self {
            BlockData::V1(data) => &data.block,
        }
    }

    pub fn block_info(&self) -> &BlockInfo {
        match self {
            BlockData::V1(data) => &data.block_info,
        }
    }
}

/// Versioned container for a batch of block data spanning consecutive heights.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum BlockBatch {
    V1(BlockBatchV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct BlockBatchV1 {
    start_height: BlockHeight,
    end_height: BlockHeight,
    data: Vec<BlockData>,
}

/// Builds a `BlockBatch` by reading block data for each height in `range`.
pub fn build_block_batch(store: &Store, range: &BatchRange) -> Result<BlockBatch, Error> {
    let count = (range.end() - range.start() + 1) as usize;
    let mut data = Vec::with_capacity(count);
    for height in range.start()..=range.end() {
        data.push(build_block_data(store, height)?);
    }
    Ok(BlockBatch::new(range.start(), range.end(), data))
}

impl BlockBatch {
    /// Constructs a `BlockBatch`, asserting the length invariant.
    /// Use `validate_blob` for batches deserialized from cloud storage.
    pub fn new(start_height: BlockHeight, end_height: BlockHeight, data: Vec<BlockData>) -> Self {
        let batch = Self::V1(BlockBatchV1 { start_height, end_height, data });
        batch.validate_blob().expect("BlockBatch::new called with inconsistent data");
        batch
    }

    /// Validates the length invariant: `data.len() == end_height - start_height + 1`.
    /// Returns a human-readable reason on mismatch. Call after deserializing
    /// a blob from cloud storage.
    pub fn validate_blob(&self) -> Result<(), String> {
        let Self::V1(batch) = self;
        let expected = (batch.end_height - batch.start_height + 1) as usize;
        if batch.data.len() != expected {
            return Err(format!(
                "BlockBatch data.len() {} does not match range [{}, {}]",
                batch.data.len(),
                batch.start_height,
                batch.end_height,
            ));
        }
        Ok(())
    }

    pub fn start_height(&self) -> BlockHeight {
        let BlockBatch::V1(batch) = self;
        batch.start_height
    }

    pub fn end_height(&self) -> BlockHeight {
        let BlockBatch::V1(batch) = self;
        batch.end_height
    }

    /// Returns the block data at `height` within this batch. `height` must
    /// be within the batch range — passing an out-of-range height is a
    /// programmer error and panics.
    pub fn get_block_at_height(&self, height: BlockHeight) -> &BlockData {
        let BlockBatch::V1(batch) = self;
        assert!(
            height >= batch.start_height && height <= batch.end_height,
            "height {height} out of batch range [{}, {}]",
            batch.start_height,
            batch.end_height,
        );
        let index = (height - batch.start_height) as usize;
        &batch.data[index]
    }
}
