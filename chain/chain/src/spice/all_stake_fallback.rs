//! The all-stake fallback: if a chunk's designated validators do not certify it in time, it may instead
//! be certified by 2/3 of total epoch stake.

use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::BlockHeader;
use near_primitives::errors::EpochError;
use near_primitives::stateless_validation::validator_assignment::ChunkValidatorAssignments;
use near_primitives::types::{
    AccountId, BlockHeight, BlockHeightDelta, EpochHeight, EpochId, ShardId, ShardIndex,
    SpiceUncertifiedChunkInfo,
};
use std::collections::HashSet;

/// Blocks a chunk must stay certifiable-but-uncertified before the all-stake fallback opens for it.
/// Well below epoch length (to rescue liveness before the one-epoch lag guard stalls consensus).
pub const SPICE_FALLBACK_CERTIFICATION_DELAY: BlockHeight = 20;

/// Whether the chunk may certify via the all-stake fallback in a block at `carrying_height`: true
/// once its designated validators have had `SPICE_FALLBACK_CERTIFICATION_DELAY` blocks to act. A
/// fallback-only chunk is eligible from the start: it has no delay to wait out, and
/// `certifiable_since_height` is only set in a later block than the chunk's own.
pub fn fallback_eligible(
    carrying_height: BlockHeight,
    chunk_info: &SpiceUncertifiedChunkInfo,
) -> bool {
    if chunk_info.is_fallback_only {
        return true;
    }
    let Some(certifiable_since) = chunk_info.certifiable_since_height else {
        return false;
    };
    carrying_height.saturating_sub(certifiable_since) >= SPICE_FALLBACK_CERTIFICATION_DELAY
}

/// Whether `endorsers`, all attesting one execution result, certify the chunk in a block at
/// `carrying_height`.
///
/// The producer and block validation both decide inclusion with this, so a producer never builds a
/// certification its own validation rejects.
pub fn endorsers_certify_chunk(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_block_header: &BlockHeader,
    chunk_info: &SpiceUncertifiedChunkInfo,
    carrying_height: BlockHeight,
    endorsers: &HashSet<AccountId>,
) -> Result<bool, Error> {
    let epoch_id = chunk_block_header.epoch_id();
    if !chunk_info.is_fallback_only {
        let designated = epoch_manager.get_chunk_validator_assignments(
            epoch_id,
            chunk_info.chunk_id.shard_id,
            chunk_block_header.height(),
        )?;
        if designated.is_endorsed(endorsers) {
            return Ok(true);
        }
    }
    if !fallback_eligible(carrying_height, chunk_info) {
        return Ok(false);
    }
    Ok(all_stake_fallback_assignment(epoch_manager, epoch_id)?.is_endorsed(endorsers))
}

/// The epoch's full validator set as a shard-independent assignment weighted by real stake. The
/// all-stake fallback certifies via 2/3 of this total when the designated assignment didn't in time.
// TODO(spice-perf): the result is epoch-invariant but rebuilt (with per-validator AccountId clones)
// on every call, and this is called per fallback-eligible chunk from validation, the producer, and
// the writer. Cache it per EpochId, like get_chunk_validator_assignments.
pub fn all_stake_fallback_assignment(
    epoch_manager: &dyn EpochManagerAdapter,
    epoch_id: &EpochId,
) -> Result<ChunkValidatorAssignments, EpochError> {
    let epoch_info = epoch_manager.get_epoch_info(epoch_id)?;
    let assignments = epoch_info.validators_iter().map(|validator| validator.account_and_stake());
    Ok(ChunkValidatorAssignments::new(assignments.collect()))
}

/// Epoch validators that may endorse the chunk only through the all-stake fallback: every validator
/// of the epoch that is not designated for it. Keeps the epoch's validator order, the same order
/// [`all_stake_fallback_assignment`] has, so callers building block content stay deterministic.
pub fn fallback_endorsers(
    epoch_manager: &dyn EpochManagerAdapter,
    epoch_id: &EpochId,
    shard_id: ShardId,
    chunk_height_created: BlockHeight,
) -> Result<Vec<AccountId>, EpochError> {
    let designated =
        epoch_manager.get_chunk_validator_assignments(epoch_id, shard_id, chunk_height_created)?;
    let epoch_info = epoch_manager.get_epoch_info(epoch_id)?;
    Ok(epoch_info
        .validators_iter()
        .map(|validator| validator.take_account_id())
        .filter(|account_id| !designated.contains(account_id))
        .collect())
}

/// A slot opens every `epoch_length / num_shards` heights and picks one shard, so the witness
/// traffic does not land on every shard at once. The first block past a slot takes it, so skipped
/// heights delay a slot rather than dropping it, until a gap runs past the next one. `epoch_height`
/// offsets which shard a slot picks, so slots do not line up with epoch boundaries.
pub(super) fn is_fallback_only_height_for_shard_index(
    epoch_length: BlockHeightDelta,
    epoch_height: EpochHeight,
    num_shards: usize,
    shard_index: ShardIndex,
    height: BlockHeight,
    prev_height: BlockHeight,
) -> bool {
    // Zero when epoch_length < num_shards, which leaves the epoch with no slots at all. Only
    // tests reach that: production epoch lengths far exceed shard counts.
    let blocks_between = epoch_length / num_shards as u64;
    if blocks_between == 0 {
        return false;
    }
    let slot = height / blocks_between;
    if slot == prev_height / blocks_between {
        return false;
    }
    let num_shards = num_shards as u64;
    let shard_for_slot = slot % num_shards;
    let epoch_offset = epoch_height % num_shards;
    shard_index as u64 == (shard_for_slot + epoch_offset) % num_shards
}

/// The fallback-only schedule, resolved from the previous block: the epoch manager does not know a
/// block while that block's chunks are recorded.
pub fn is_fallback_only_chunk(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_block_header: &BlockHeader,
    shard_id: ShardId,
) -> Result<bool, Error> {
    let prev_hash = chunk_block_header.prev_hash();
    // Absent only on header versions that predate spice, which never carry a slot.
    let Some(prev_height) = chunk_block_header.prev_height() else {
        return Ok(false);
    };
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash)?;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    Ok(is_fallback_only_height_for_shard_index(
        epoch_manager.get_epoch_config(&epoch_id)?.epoch_length,
        epoch_manager.get_epoch_height_from_prev_block(prev_hash)?,
        shard_layout.num_shards() as usize,
        shard_layout.get_shard_index(shard_id)?,
        chunk_block_header.height(),
        prev_height,
    ))
}
