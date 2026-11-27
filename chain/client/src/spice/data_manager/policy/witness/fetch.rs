use crate::spice::data_manager::DataId;
use near_chain::spice::all_stake_fallback::{fallback_eligible, is_fallback_only_chunk};
use near_chain::spice::chunk_application::shards_applied_itself;
use near_chain::spice::core::{get_last_certified_block_header, get_uncertified_chunks};
use near_chain::{Block, Error};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::errors::EpochError;
use near_primitives::types::{AccountId, BlockHeight, SpiceChunkId};
use near_primitives::utils::{get_endorsements_key, get_execution_results_key};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, Store};
use std::sync::Arc;

/// Blocks before a chunk becomes fallback-eligible from which the epoch's other validators track
/// its witness, so a fallback push that arrives before this node sees the fallback open is kept.
pub(crate) const FALLBACK_TRACKING_LOOKAHEAD: BlockHeight = 2;

/// The witnesses of `block` that `me` validates without applying the chunk itself: as a
/// designated validator, or as any validator of the epoch for a chunk only the all-stake fallback
/// certifies. None without an account.
pub(crate) fn needed_items(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    me: Option<&AccountId>,
    block: &BlockHeader,
) -> Result<Vec<DataId>, Error> {
    let Some(me) = me else {
        return Ok(Vec::new());
    };
    if !is_epoch_validator(epoch_manager, me, block)? {
        return Ok(Vec::new());
    }
    let applied_itself = shards_applied_itself(shard_tracker, epoch_manager, block)?;
    let shard_layout = epoch_manager.get_shard_layout(block.epoch_id())?;
    let mut ids = Vec::new();
    for shard_id in shard_layout.shard_ids() {
        if applied_itself.contains(&shard_id) {
            continue;
        }
        let designated = epoch_manager
            .get_chunk_validator_assignments(block.epoch_id(), shard_id, block.height())?
            .contains(me);
        if designated || is_fallback_only_chunk(epoch_manager, block, shard_id)? {
            ids.push(DataId::Witness(SpiceChunkId { block_hash: *block.hash(), shard_id }));
        }
    }
    Ok(ids)
}

/// The witnesses `me` starts tracking at `block` as a validator of their epoch: each chunk
/// uncertified as of `block`'s parent that is fallback-eligible within
/// `FALLBACK_TRACKING_LOOKAHEAD` blocks of the block after `block`, with its block's header. Skips
/// the chunks `me` applies itself and those with a stored execution result. None without an
/// account.
pub(crate) fn nearing_fallback(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    me: Option<&AccountId>,
    block: &Block,
) -> Result<Vec<(Arc<BlockHeader>, DataId)>, Error> {
    let Some(me) = me else {
        return Ok(Vec::new());
    };
    let carrying_height = block.header().height() + 1 + FALLBACK_TRACKING_LOOKAHEAD;
    let mut items = Vec::new();
    for chunk_info in get_uncertified_chunks(chain_store, block.header().prev_hash())? {
        let chunk_id = &chunk_info.chunk_id;
        // A missing header means a store fault; skip the chunk and log it.
        let Some(header) = chain_store.get_block_header_if_exists(&chunk_id.block_hash) else {
            tracing::error!(target: "spice_data_distribution", ?chunk_id, "uncertified chunk's block header is missing");
            continue;
        };
        let certified = chain_store.store().exists(
            DBCol::execution_results(),
            &get_execution_results_key(&chunk_id.block_hash, chunk_id.shard_id),
        );
        if certified
            || !fallback_eligible(epoch_manager, &header, &chunk_info, carrying_height)?
            || !is_epoch_validator(epoch_manager, me, &header)?
            || shards_applied_itself(shard_tracker, epoch_manager, &header)?
                .contains(&chunk_id.shard_id)
        {
            continue;
        }
        items.push((header, DataId::Witness(chunk_info.chunk_id)));
    }
    Ok(items)
}

/// Whether `me` is a validator of `block`'s epoch.
fn is_epoch_validator(
    epoch_manager: &dyn EpochManagerAdapter,
    me: &AccountId,
    block: &BlockHeader,
) -> Result<bool, Error> {
    match epoch_manager.get_validator_by_account_id(block.epoch_id(), me) {
        Ok(_) => Ok(true),
        Err(EpochError::NotAValidator(..)) => Ok(false),
        Err(err) => Err(err.into()),
    }
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

/// Whether witnesses are pushed to `me`: to any node with an account.
pub(crate) fn is_pushed_to(me: Option<&AccountId>) -> bool {
    me.is_some()
}

/// Whether `me`'s endorsement of the chunk is stored; never without an account.
pub(crate) fn is_done(store: &Store, me: Option<&AccountId>, chunk_id: &SpiceChunkId) -> bool {
    me.is_some_and(|me| {
        let key = get_endorsements_key(&chunk_id.block_hash, chunk_id.shard_id, me);
        store.exists(DBCol::endorsements(), &key)
    })
}

/// The witnesses `block` makes pullable for `me`: each chunk certifiable but still uncertified as
/// of `block`'s parent whose producers were due to push its witness to `me` as of the parent. Producers push
/// to the designated validators once the chunk is certifiable, and to every other validator of
/// the epoch once the all-stake fallback opens.
// TODO(spice-data-distribution): when fewer producers push than decoding needs, each height
// takes two blocks: a pull here and the endorsement in the next block. Add standing requests,
// so the producers still online push the missing parts with every new item of the shard.
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

/// The witnesses whose chunks `block` certifies.
pub(crate) fn certified_by(block: &Block) -> Vec<DataId> {
    block
        .spice_core_statements()
        .iter_execution_results()
        .map(|(chunk_id, _)| DataId::Witness(chunk_id.clone()))
        .collect()
}
