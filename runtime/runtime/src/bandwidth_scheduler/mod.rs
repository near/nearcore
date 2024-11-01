use std::num::NonZeroU64;

use near_primitives::bandwidth_scheduler::{BandwidthSchedulerParams, BandwidthSchedulerState};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{ShardId, StateChangeCause};
use near_primitives::version::ProtocolFeature;
use near_store::{
    get_bandwidth_scheduler_state, set_bandwidth_scheduler_state, StorageError, TrieUpdate,
};

use crate::ApplyState;

/// In future the output will contain the granted bandwidth.
pub struct BandwidthSchedulerOutput {
    /// Parameters used by the bandwidth scheduler algorithm.
    /// Will be used for generating bandwidth requests.
    pub params: BandwidthSchedulerParams,
    /// Used only for a sanity check.
    pub scheduler_state_hash: CryptoHash,
}

pub fn run_bandwidth_scheduler(
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
) -> Result<Option<BandwidthSchedulerOutput>, StorageError> {
    if !ProtocolFeature::BandwidthScheduler.enabled(apply_state.current_protocol_version) {
        return Ok(None);
    }

    let _span = tracing::debug_span!(
        target: "runtime",
        "run_bandwidth_scheduler",
        height = apply_state.block_height,
        shard_id = ?apply_state.shard_id);

    // Read the current scheduler state from the Trie
    let mut scheduler_state = match get_bandwidth_scheduler_state(state_update)? {
        Some(prev_state) => prev_state,
        None => {
            tracing::debug!(target: "runtime", "Bandwidth scheduler state not found - initializing");
            BandwidthSchedulerState { mock_data: [0; 32] }
        }
    };

    let scheduler_state_hash: CryptoHash = hash(&borsh::to_vec(&scheduler_state).unwrap());

    let mut all_shards = apply_state.congestion_info.all_shards();
    all_shards.sort();
    if all_shards.is_empty() {
        all_shards = vec![ShardId::new(0)];
    }
    let num_shards =
        NonZeroU64::new(all_shards.len().try_into().expect("Can't convert usize to u64"))
            .expect("Number of shards can't be zero");

    let params = BandwidthSchedulerParams::new(num_shards, &apply_state.config);

    let prev_block_hash = apply_state.prev_block_hash;
    let bandwidth_requests = &apply_state.bandwidth_requests;
    tracing::debug!(
        target: "runtime",
        ?prev_block_hash,
        ?scheduler_state,
        ?all_shards,
        ?bandwidth_requests,
        "Running bandwidth scheduler with inputs",
    );

    // Simulate bandwidth scheduler doing something here
    // The scheduler algorithm has the following inputs:
    // * previous scheduler state
    // * list of all shards - used to generate bandwidth grants
    // * bandwidth requests from the previous height
    // * prev_block_hash which is used as a seed for the random generator which resolves draws between requests.
    //
    // Bandwidth scheduler takes these inputs and produces bandwidth grants and new scheduler state.
    // The inputs and outputs are the same on all shards.
    let mut data = Vec::new();
    data.extend_from_slice(scheduler_state.mock_data.as_slice());
    data.extend_from_slice(borsh::to_vec(&all_shards).unwrap().as_slice());
    data.extend_from_slice(
        borsh::to_vec(&bandwidth_requests.shards_bandwidth_requests).unwrap().as_slice(),
    );
    data.extend_from_slice(prev_block_hash.as_bytes().as_slice());
    scheduler_state.mock_data = hash(data.as_slice()).into();

    // Save the updated scheduler state to the trie.
    set_bandwidth_scheduler_state(state_update, &scheduler_state);
    state_update.commit(StateChangeCause::BandwidthSchedulerStateUpdate);

    Ok(Some(BandwidthSchedulerOutput { params, scheduler_state_hash }))
}
