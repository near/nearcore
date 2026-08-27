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

/// Blocks a chunk must stay certifiable-but-uncertified before the all-stake fallback opens for it.
/// Well below epoch length (to rescue liveness before the one-epoch lag guard stalls consensus).
pub const SPICE_FALLBACK_CERTIFICATION_DELAY: BlockHeight = 20;

/// Whether the chunk may certify via the all-stake fallback in a block at `carrying_height`: true
/// once its designated validators have had `SPICE_FALLBACK_CERTIFICATION_DELAY` blocks to act.
pub fn fallback_eligible(
    carrying_height: BlockHeight,
    chunk_info: &SpiceUncertifiedChunkInfo,
) -> bool {
    let Some(certifiable_since) = chunk_info.certifiable_since_height else {
        return false;
    };
    carrying_height.saturating_sub(certifiable_since) >= SPICE_FALLBACK_CERTIFICATION_DELAY
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
/// traffic does not land on every shard at once. Only the block at the slot height takes it. An
/// honest producer makes one block per height, so two forks cannot both take the same slot. If the
/// slot height is skipped, the shard waits for its next slot. `epoch_height` offsets which shard a
/// slot picks, so slots do not line up with epoch boundaries.
pub(super) fn fallback_only_shard_index(
    epoch_length: BlockHeightDelta,
    epoch_height: EpochHeight,
    num_shards: usize,
    height: BlockHeight,
) -> Option<ShardIndex> {
    // Zero when epoch_length < num_shards, which leaves the epoch with no slots at all. Only
    // tests reach that: production epoch lengths far exceed shard counts.
    let blocks_between = epoch_length / num_shards as u64;
    if blocks_between == 0 {
        return None;
    }
    if !height.is_multiple_of(blocks_between) {
        return None;
    }
    let slot = height / blocks_between;
    let num_shards = num_shards as u64;
    let shard_for_slot = slot % num_shards;
    let epoch_offset = epoch_height % num_shards;
    Some(((shard_for_slot + epoch_offset) % num_shards) as ShardIndex)
}

/// The shard whose chunk in `chunk_block_header`'s block certifies only via the all-stake
/// fallback, if the block takes a slot.
pub fn fallback_only_shard(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_block_header: &BlockHeader,
) -> Result<Option<ShardId>, Error> {
    let epoch_id = chunk_block_header.epoch_id();
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    let Some(shard_index) = fallback_only_shard_index(
        epoch_manager.get_epoch_config(epoch_id)?.epoch_length,
        epoch_manager.get_epoch_info(epoch_id)?.epoch_height(),
        shard_layout.num_shards() as usize,
        chunk_block_header.height(),
    ) else {
        return Ok(None);
    };
    Ok(Some(shard_layout.get_shard_id(shard_index)?))
}

pub fn is_fallback_only_chunk(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_block_header: &BlockHeader,
    shard_id: ShardId,
) -> Result<bool, Error> {
    Ok(fallback_only_shard(epoch_manager, chunk_block_header)? == Some(shard_id))
}
