use near_primitives::bandwidth_scheduler::{
    BandwidthRequest, BandwidthRequests, BandwidthRequestsV1, BandwidthSchedulerState,
};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{ShardId, StateChangeCause};
use near_primitives::version::ProtocolFeature;
use near_store::{
    get_bandwidth_scheduler_state, set_bandwidth_scheduler_state, StorageError, TrieUpdate,
};

use crate::ApplyState;

pub struct BandwidthSchedulerOutput {
    /// In future the output will contain the granted bandwidth.
    pub mock_data: [u8; 32],
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

    Ok(Some(BandwidthSchedulerOutput {
        mock_data: scheduler_state.mock_data,
        scheduler_state_hash,
    }))
}

/// Generate mock bandwidth requests based on scheduler output and shard id.
pub fn generate_mock_bandwidth_requests(
    apply_state: &ApplyState,
    scheduler_output_opt: &Option<BandwidthSchedulerOutput>,
) -> Option<BandwidthRequests> {
    if !ProtocolFeature::BandwidthScheduler.enabled(apply_state.current_protocol_version) {
        return None;
    }

    let _span = tracing::debug_span!(
        target: "runtime",
        "generate_mock_bandwidth_requests",
        height = apply_state.block_height,
        shard_id = ?apply_state.shard_id);

    let Some(scheduler_output) = scheduler_output_opt else {
        tracing::debug!(
            target: "runtime",
            "scheduler output is None, not generating any requests",
        );
        return None;
    };

    let mut data = Vec::new();
    data.extend_from_slice(scheduler_output.mock_data.as_slice());
    data.extend_from_slice(apply_state.shard_id.to_be_bytes().as_slice());
    let hash_data: [u8; 32] = hash(data.as_slice()).into();

    let mut requests = Vec::new();
    let mut debug_ids = Vec::new();
    for hash_byte in hash_data.iter().take(4) {
        requests.push(BandwidthRequest { to_shard: *hash_byte });
        debug_ids.push(*hash_byte);
    }
    tracing::debug!(
        target: "runtime",
        "generate_mock_bandwidth_requests - generated requests to shards: {:?}",
        debug_ids
    );
    Some(BandwidthRequests::V1(BandwidthRequestsV1 { requests }))
}
