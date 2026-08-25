use crate::Store;
use crate::adapter::StoreAdapter;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::epoch_info::EpochInfo;
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
    /// Read from `DBCol::EpochStart`.
    epoch_start_height: BlockHeight,
}

/// Builds an `EpochData` object for the given epoch ID by reading data from the store.
pub fn build_epoch_data(
    store: &Store,
    shard_layout: ShardLayout,
    epoch_id: EpochId,
) -> Result<EpochData, Error> {
    let store = store.epoch_store();
    let epoch_info = store.get_epoch_info(&epoch_id)?;
    let epoch_start_height = store.get_epoch_start(&epoch_id)?;

    let epoch_data = EpochDataV1 { epoch_id, epoch_info, shard_layout, epoch_start_height };
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
        match self {
            EpochData::V1(data) => data.epoch_start_height,
        }
    }

    pub fn shard_layout(&self) -> &ShardLayout {
        match self {
            EpochData::V1(data) => &data.shard_layout,
        }
    }
}
