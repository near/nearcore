use crate::spice::data_manager::DataId;
use near_chain::spice::all_stake_fallback::fallback_eligible;
use near_chain::spice::chunk_application::shards_applied_itself;
use near_chain::spice::core::{get_last_certified_block_header, get_uncertified_chunks};
use near_chain::{Block, Error};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::errors::EpochError;
use near_primitives::types::{AccountId, BlockHeight, SpiceChunkId};
use near_primitives::utils::get_endorsements_key;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, Store};
use std::sync::Arc;

/// The witnesses of `block` that `me` validates without applying the chunk itself: as a
/// designated validator, or as any other validator of the epoch through the all-stake fallback.
pub(crate) fn needed_items(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    me: &AccountId,
    block: &BlockHeader,
) -> Result<Vec<DataId>, Error> {
    match epoch_manager.get_validator_by_account_id(block.epoch_id(), me) {
        Ok(_) => {}
        Err(EpochError::NotAValidator(..)) => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    }
    let applied_itself = shards_applied_itself(shard_tracker, epoch_manager, block)?;
    let shard_layout = epoch_manager.get_shard_layout(block.epoch_id())?;
    Ok(shard_layout
        .shard_ids()
        .filter(|shard_id| !applied_itself.contains(shard_id))
        .map(|shard_id| DataId::Witness(SpiceChunkId { block_hash: *block.hash(), shard_id }))
        .collect())
}

/// The lowest height whose witnesses may still be needed: the one above the last block certified
/// as of the final head, since every chunk at or below that block is certified.
pub(crate) fn lowest_needed_height(chain_store: &ChainStoreAdapter) -> Result<BlockHeight, Error> {
    Ok(last_certified_as_of_final_head(chain_store)?.height() + 1)
}

/// The last block certified as of the final head.
pub(crate) fn last_certified_as_of_final_head(
    chain_store: &ChainStoreAdapter,
) -> Result<Arc<BlockHeader>, Error> {
    let final_head = chain_store.final_head()?;
    get_last_certified_block_header(chain_store, &final_head.last_block_hash)
}

/// Whether `me`'s endorsement of the chunk is stored.
pub(crate) fn is_done(store: &Store, me: &AccountId, chunk_id: &SpiceChunkId) -> bool {
    let key = get_endorsements_key(&chunk_id.block_hash, chunk_id.shard_id, me);
    store.exists(DBCol::endorsements(), &key)
}

/// The witnesses `block` makes pullable for `me`: each chunk certifiable but still uncertified as
/// of `block`'s parent whose producers were due to push its witness to `me` as of the parent. Producers push
/// to the designated validators once the chunk is certifiable, and to every other validator of
/// the epoch once the all-stake fallback opens.
pub(crate) fn made_pullable_by(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    me: &AccountId,
    block: &Block,
) -> Result<Vec<DataId>, Error> {
    let parent = chain_store.get_block_header(block.header().prev_hash())?;
    let mut ids = Vec::new();
    for chunk_info in get_uncertified_chunks(chain_store, parent.hash())? {
        if chunk_info.certifiable_since_height.is_none() {
            continue;
        }
        let chunk_id = &chunk_info.chunk_id;
        // A missing header means a store fault; skip the chunk and log it.
        let Some(header) = chain_store.get_block_header_if_exists(&chunk_id.block_hash) else {
            tracing::error!(target: "spice_data_distribution", ?chunk_id, "uncertified chunk's block header is missing");
            continue;
        };
        let designated = epoch_manager
            .get_chunk_validator_assignments(header.epoch_id(), chunk_id.shard_id, header.height())?
            .contains(me);
        if designated
            || fallback_eligible(epoch_manager, &header, &chunk_info, parent.height() + 1)?
        {
            ids.push(DataId::Witness(chunk_info.chunk_id));
        }
    }
    Ok(ids)
}

/// The witnesses whose chunks `block`'s parent certifies. A witness stays pullable at the
/// trigger of the block that certifies its chunk, and is not needed after it.
pub(crate) fn certified_by_parent(
    chain_store: &ChainStoreAdapter,
    block: &Block,
) -> Result<Vec<DataId>, Error> {
    let parent = chain_store.get_block(block.header().prev_hash())?;
    Ok(parent
        .spice_core_statements()
        .iter_execution_results()
        .map(|(chunk_id, _)| DataId::Witness(chunk_id.clone()))
        .collect())
}
