//! The all-stake fallback: if a chunk's designated validators do not certify it in time, it may instead
//! be certified by 2/3 of total epoch stake.

use near_epoch_manager::EpochManagerAdapter;
use near_primitives::errors::EpochError;
use near_primitives::stateless_validation::validator_assignment::ChunkValidatorAssignments;
use near_primitives::types::{BlockHeight, EpochId, SpiceUncertifiedChunkInfo};

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
