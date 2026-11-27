mod fetch;
mod serve;

#[cfg(test)]
pub(crate) use fetch::FALLBACK_TRACKING_LOOKAHEAD;
pub(in crate::spice::data_manager) use fetch::{
    certified_by, is_done, is_pushed_to, last_certified_as_of_final_head, lowest_needed_height,
    made_pullable_by, nearing_fallback, needed_items,
};
use near_primitives::stateless_validation::state_witness::MAX_UNCOMPRESSED_STATE_WITNESS_SIZE;
pub(super) use serve::WitnessServePolicy;

/// The largest encoded witness: the witness size limit plus the `SpiceData` variant tag.
// TODO(spice-data-distribution): bound `proof_of_invalid_chunk` and the boundary witness's
// receipt proofs and implicit transitions; nothing yet keeps them under this cap.
pub(in crate::spice::data_manager) const MAX_ENCODED_LENGTH: u64 =
    MAX_UNCOMPRESSED_STATE_WITNESS_SIZE + 1;
