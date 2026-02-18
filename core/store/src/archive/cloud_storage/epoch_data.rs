use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{BlockHeight, EpochId};
use near_schema_checker_lib::ProtocolSchema;

use crate::Store;
use crate::adapter::StoreAdapter;

/// Versioned container for epoch-related data stored in the cloud archival.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum EpochData {
    V1(EpochDataV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EpochDataV1 {
    /// Read from `DBCol::EpochInfo`.
    epoch_info: EpochInfo,
    /// Provided by the caller of `build_epoch_data`.
    /// From `EpochInfoV5`, this data is already part of `EpochInfo`.
    shard_layout: ShardLayout,
    /// Read from `DBCol::EpochStart` and `DBCol::BlockHeight`.
    epoch_start_height: BlockHeight,
    /// Read from `DBCol::BlockMerkleTree`.
    epoch_start_prev_block_merkle_tree: PartialMerkleTree,
    /// Read from `DBCol::StateSyncHashes` and `DBCol::BlockHeight`.
    sync_block_height: BlockHeight,
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

    let store = store.chain_store();
    let epoch_start_block_hash = store.get_block_hash_by_height(epoch_start_height)?;
    let epoch_start_block = store.get_block(&epoch_start_block_hash)?;
    let sync_block_hash = store
        .get_current_epoch_sync_hash(&epoch_id)
        .ok_or_else(|| Error::DBNotFoundErr(format!("StateSyncHashes, epoch ID: {epoch_id:?}")))?;
    let sync_block_height = store.get_block_height(&sync_block_hash)?;
    let epoch_start_prev_block_merkle_tree =
        store.get_block_merkle_tree(epoch_start_block.header().prev_hash())?;
    let epoch_data = EpochDataV1 {
        epoch_info,
        shard_layout,
        epoch_start_height,
        epoch_start_prev_block_merkle_tree,
        sync_block_height,
    };
    Ok(EpochData::V1(epoch_data))
}

impl EpochData {
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

    pub fn sync_block_height(&self) -> BlockHeight {
        match self {
            EpochData::V1(data) => data.sync_block_height,
        }
    }

    pub fn epoch_start_prev_block_merkle_tree(&self) -> &PartialMerkleTree {
        match self {
            EpochData::V1(data) => &data.epoch_start_prev_block_merkle_tree,
        }
    }
}
