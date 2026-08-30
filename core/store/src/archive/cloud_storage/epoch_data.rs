use crate::Store;
use crate::adapter::StoreAdapter;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{BlockHeight, EpochId};
use near_schema_checker_lib::ProtocolSchema;

/// Versioned container for epoch-related data stored in the cloud archival.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum EpochData {
    V1(EpochDataV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EpochDataV1 {
    /// The epoch ID this data belongs to.
    epoch_id: EpochId,
    /// Read from `DBCol::EpochInfo`.
    epoch_info: EpochInfo,
    /// Provided by the caller of `build_epoch_data`.
    /// From `EpochInfoV5`, this data is already part of `EpochInfo`.
    shard_layout: ShardLayout,
    /// Read from `DBCol::BlockInfo`.
    epoch_first_block_info: BlockInfo,
}

/// Builds the `EpochData` of the epoch that starts after `prev_epoch_end`, the last
/// block of the epoch below it.
pub fn build_epoch_data(
    store: &Store,
    shard_layout: ShardLayout,
    prev_epoch_end: &CryptoHash,
) -> Result<EpochData, Error> {
    let first_block_hash = store.chain_store().get_next_block_hash(prev_epoch_end)?;
    let epoch_store = store.epoch_store();
    let epoch_first_block_info = epoch_store.get_block_info(&first_block_hash)?;
    let epoch_id = *epoch_first_block_info.epoch_id();
    let epoch_info = epoch_store.get_epoch_info(&epoch_id)?;

    let epoch_data = EpochDataV1 { epoch_id, epoch_info, shard_layout, epoch_first_block_info };
    Ok(EpochData::V1(epoch_data))
}

impl EpochData {
    pub fn epoch_id(&self) -> &EpochId {
        match self {
            EpochData::V1(data) => &data.epoch_id,
        }
    }

    pub fn epoch_info(&self) -> &EpochInfo {
        match self {
            EpochData::V1(data) => &data.epoch_info,
        }
    }

    pub fn epoch_start_height(&self) -> BlockHeight {
        self.epoch_first_block_info().height()
    }

    pub fn epoch_first_block_info(&self) -> &BlockInfo {
        match self {
            EpochData::V1(data) => &data.epoch_first_block_info,
        }
    }

    pub fn shard_layout(&self) -> &ShardLayout {
        match self {
            EpochData::V1(data) => &data.shard_layout,
        }
    }
}
