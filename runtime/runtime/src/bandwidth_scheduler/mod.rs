use std::collections::BTreeMap;
use std::num::NonZeroU64;

use near_primitives::bandwidth_scheduler::{BandwidthSchedulerParams, BandwidthSchedulerState};
use near_primitives::congestion_info::CongestionControl;
use near_primitives::errors::RuntimeError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{EpochInfoProvider, ShardId, ShardIndex, StateChangeCause};
use near_primitives::version::ProtocolFeature;
use near_store::{get_bandwidth_scheduler_state, set_bandwidth_scheduler_state, TrieUpdate};
use scheduler::{BandwidthScheduler, GrantedBandwidth, ShardStatus};

use crate::ApplyState;

mod scheduler;
#[cfg(test)]
mod simulator;

pub struct BandwidthSchedulerOutput {
    /// How many bytes of outgoing receipts can be sent from one shard to another at the current height.
    pub granted_bandwidth: GrantedBandwidth,
    /// Parameters used by the bandwidth scheduler algorithm.
    /// Will be used for generating bandwidth requests.
    pub params: BandwidthSchedulerParams,
    /// Used only for a sanity check.
    pub scheduler_state_hash: CryptoHash,
}

/// Run the bandwidth scheduler algorithm to figure out how many bytes
/// of outgoing receipts can be sent between shards at the current height.
pub fn run_bandwidth_scheduler(
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
    epoch_info_provider: &dyn EpochInfoProvider,
) -> Result<Option<BandwidthSchedulerOutput>, RuntimeError> {
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
            BandwidthSchedulerState {
                link_allowances: Vec::new(),
                sanity_check_hash: CryptoHash::default(),
            }
        }
    };

    let shard_layout = epoch_info_provider.shard_layout(&apply_state.epoch_id)?;

    // Prepare the status info for each shard based on congestion info.
    let mut shards_status: BTreeMap<ShardId, ShardStatus> = BTreeMap::new();
    for (shard_id, extended_congestion_info) in apply_state.congestion_info.iter() {
        let last_chunk_missing = extended_congestion_info.missed_chunks_count > 0;
        let allowed_sender_shard_id: ShardId =
            extended_congestion_info.congestion_info.allowed_shard().into();
        let allowed_sender_shard_index: Option<ShardIndex> =
            shard_layout.get_shard_index(allowed_sender_shard_id).ok();

        let congestion_control = CongestionControl::new(
            apply_state.config.congestion_control_config,
            extended_congestion_info.congestion_info,
            extended_congestion_info.missed_chunks_count,
        );
        let congestion_level = congestion_control.congestion_level();
        let is_fully_congested = CongestionControl::is_fully_congested(congestion_level);

        shards_status.insert(
            *shard_id,
            ShardStatus { last_chunk_missing, allowed_sender_shard_index, is_fully_congested },
        );
    }

    // Prepare lists of sender and receiver shards.
    let all_shards: Vec<ShardId> = shard_layout.shard_ids().collect();

    // Calculate the current scheduler parameters.
    let params = BandwidthSchedulerParams::new(
        NonZeroU64::new(shard_layout.num_shards()).expect("ShardLayout has zero shards!"),
        &apply_state.config,
    );

    // Run the bandwidth scheduler algorithm.
    let granted_bandwidth = BandwidthScheduler::run(
        shard_layout,
        &mut scheduler_state,
        &params,
        &apply_state.bandwidth_requests,
        &shards_status,
        apply_state.prev_block_hash.0,
    );

    // Hash (some of) the inputs to the scheduler algorithm and save the checksum in the state.
    // This is a sanity check to make sure that all shards run the scheduler with the same inputs.
    // It would be a bit nicer to hash all inputs, but that could be slow and the serialization
    // format of the hashed structs would become part of the protocol.
    let mut sanity_check_bytes = Vec::new();
    sanity_check_bytes.extend_from_slice(scheduler_state.sanity_check_hash.as_ref());
    sanity_check_bytes.extend_from_slice(CryptoHash::hash_borsh(&all_shards).as_ref());
    scheduler_state.sanity_check_hash = CryptoHash::hash_bytes(&sanity_check_bytes);

    // Save the updated scheduler state to the trie.
    set_bandwidth_scheduler_state(state_update, &scheduler_state);
    state_update.commit(StateChangeCause::BandwidthSchedulerStateUpdate);

    let scheduler_state_hash: CryptoHash = hash(&borsh::to_vec(&scheduler_state).unwrap());
    Ok(Some(BandwidthSchedulerOutput { granted_bandwidth, params, scheduler_state_hash }))
}
