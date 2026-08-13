use crate::Store;
use crate::adapter::StoreAdapter;
use crate::archive::cloud_storage::batch::BatchRange;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::block::Block;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;

/// Versioned container for block-related data stored in the cloud archival.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum BlockData {
    V1(BlockDataV1),
}

// TODO(cloud_archival): remove this note once the cloud blob format is stabilized.
// Pre-stabilization there is no committed blob-format contract, so appending a field
// to `V1` is fine: no stable blobs exist to break. Once the format freezes, add a
// `BlockData::V2` variant instead of appending here.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct BlockDataV1 {
    /// Read from `DBCol::Block`.
    block: Block,
    /// Read from `DBCol::BlockInfo`.
    block_info: BlockInfo,
    /// Read from `DBCol::NextBlockHashes`.
    next_block_hash: CryptoHash,
    /// Rows of `DBCol::ChunkProducers` for this block, keyed by shard_id; empty
    /// when EarlyKickout/nightly is off.
    chunk_producers: Vec<(ShardId, ValidatorStake)>,
}

/// Builds a `BlockData` object for the given block height by reading data
/// from the store. Returns `Ok(None)` if no block was produced at this
/// height (skipped slot).
pub fn build_block_data(
    store: &Store,
    block_height: BlockHeight,
) -> Result<Option<BlockData>, Error> {
    let store = store.chain_store();
    let block_hash = match store.get_block_hash_by_height(block_height) {
        Ok(hash) => hash,
        Err(Error::DBNotFoundErr(_)) => {
            tracing::debug!(target: "cloud_archival", block_height, "skipped slot");
            return Ok(None);
        }
        Err(other) => return Err(other),
    };
    let block = (*store.get_block(&block_hash)?).clone();
    let block_info = store.epoch_store().get_block_info(&block_hash)?;
    let next_block_hash = store.get_next_block_hash(&block_hash)?;
    // `read_chunk_producers` needs the base `Store`, not the chain-store adapter
    // that shadows `store` above.
    let chunk_producers = read_chunk_producers(store.store_ref(), &block_hash)?;
    let block_data = BlockDataV1 { block, block_info, next_block_hash, chunk_producers };
    Ok(Some(BlockData::V1(block_data)))
}

#[cfg(feature = "nightly")]
fn read_chunk_producers(
    store: &Store,
    block_hash: &CryptoHash,
) -> Result<Vec<(ShardId, ValidatorStake)>, Error> {
    use crate::DBCol;
    use near_primitives::utils::get_block_shard_id_rev;
    store
        .iter_prefix(DBCol::ChunkProducers, block_hash.as_ref())
        .map(|(key, value)| {
            let shard_id = get_block_shard_id_rev(&key)
                .map_err(|err| Error::Other(format!("malformed chunk producers key: {err}")))?
                .1;
            let stake = ValidatorStake::try_from_slice(&value)
                .map_err(|err| Error::Other(format!("malformed chunk producers value: {err}")))?;
            Ok((shard_id, stake))
        })
        .collect()
}

#[cfg(not(feature = "nightly"))]
fn read_chunk_producers(
    _store: &Store,
    _block_hash: &CryptoHash,
) -> Result<Vec<(ShardId, ValidatorStake)>, Error> {
    Ok(Vec::new())
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

    pub fn next_block_hash(&self) -> &CryptoHash {
        match self {
            BlockData::V1(data) => &data.next_block_hash,
        }
    }

    pub fn chunk_producers(&self) -> &[(ShardId, ValidatorStake)] {
        match self {
            BlockData::V1(data) => &data.chunk_producers,
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
    /// One entry per height; `None` at skipped slots.
    data: Vec<Option<BlockData>>,
}

/// Builds a `BlockBatch` by reading block data for each height in `range`.
/// Heights with no produced block (skipped slots) become `None` entries.
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
    pub fn new(
        start_height: BlockHeight,
        end_height: BlockHeight,
        data: Vec<Option<BlockData>>,
    ) -> Self {
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

    /// Returns the block data at `height` within this batch, or `None` if
    /// the height is a skipped slot. `height` must be within the batch
    /// range - passing an out-of-range height is a programmer error and panics.
    pub fn get_block_at_height(&self, height: BlockHeight) -> Option<&BlockData> {
        let BlockBatch::V1(batch) = self;
        assert!(
            height >= batch.start_height && height <= batch.end_height,
            "height {height} out of batch range [{}, {}]",
            batch.start_height,
            batch.end_height,
        );
        let index = (height - batch.start_height) as usize;
        batch.data[index].as_ref()
    }
}
