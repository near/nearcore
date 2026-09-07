use crate::Store;
use crate::adapter::StoreAdapter;
use crate::light_client_block::StoredLightClientBlock;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::EpochSummary;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::views::LightClientBlockView;
use near_schema_checker_lib::ProtocolSchema;

/// Versioned container for epoch-related data stored in the cloud archival.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum EpochData {
    V1(EpochDataV1) = 0,
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
    /// The epoch below this one, whose last block this blob is built from.
    prev_epoch_id: EpochId,
    /// Read from `DBCol::EpochValidatorInfo`, under `prev_epoch_id`.
    prev_epoch_summary: Option<EpochSummary>,
    /// Read from `DBCol::EpochLightClientBlocks`, under `prev_epoch_id`.
    prev_epoch_light_client_block: Option<StoredLightClientBlock>,
    /// Read from `DBCol::EpochInfo`, under `next_epoch_id()`.
    next_epoch_info: EpochInfo,
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

    // This blob is built at epoch start, so the aggregate data it carries is the epoch
    // below's. The genesis epoch has neither: it has no summary, and its last block has
    // no final block behind it to build a view from.
    let prev_epoch_id = *epoch_store.get_block_info(prev_epoch_end)?.epoch_id();
    let prev_epoch_summary = match epoch_store.get_epoch_validator_info(&prev_epoch_id) {
        Ok(summary) => Some(summary),
        Err(EpochError::EpochOutOfBounds(_)) => None,
        Err(err) => return Err(err.into()),
    };
    let prev_epoch_light_client_block =
        match store.chain_store().get_epoch_light_client_block(&prev_epoch_id.0) {
            Ok(view) => Some(LightClientBlockView::clone(&view).into()),
            Err(Error::DBNotFoundErr(_)) => None,
            Err(err) => return Err(err),
        };

    let next_epoch_id = EpochId(*epoch_first_block_info.prev_hash());
    let next_epoch_info = epoch_store.get_epoch_info(&next_epoch_id)?;

    let epoch_data = EpochDataV1 {
        epoch_id,
        epoch_info,
        shard_layout,
        epoch_first_block_info,
        prev_epoch_id,
        prev_epoch_summary,
        prev_epoch_light_client_block,
        next_epoch_info,
    };
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

    pub fn prev_epoch_id(&self) -> &EpochId {
        match self {
            EpochData::V1(data) => &data.prev_epoch_id,
        }
    }

    pub fn prev_epoch_summary(&self) -> Option<&EpochSummary> {
        match self {
            EpochData::V1(data) => data.prev_epoch_summary.as_ref(),
        }
    }

    pub fn prev_epoch_light_client_block(&self) -> Option<&StoredLightClientBlock> {
        match self {
            EpochData::V1(data) => data.prev_epoch_light_client_block.as_ref(),
        }
    }

    /// The epoch above this one, derived the way `get_next_epoch_id_from_info` does.
    pub fn next_epoch_id(&self) -> EpochId {
        EpochId(*self.epoch_first_block_info().prev_hash())
    }

    pub fn next_epoch_info(&self) -> &EpochInfo {
        match self {
            EpochData::V1(data) => &data.next_epoch_info,
        }
    }

    pub fn shard_layout(&self) -> &ShardLayout {
        match self {
            EpochData::V1(data) => &data.shard_layout,
        }
    }
}
