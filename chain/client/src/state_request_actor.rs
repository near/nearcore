use std::collections::VecDeque;
use std::sync::Arc;

use near_async::messaging::{Actor, Handler};
use near_async::time::{Clock, Duration, Instant};
use near_chain::state_sync::ChainStateSyncAdapter;
use near_chain::types::RuntimeAdapter;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::{StateRequestHeader, StateRequestPart, StateResponse};
use near_network::types::{StateResponseInfo, StateResponseInfoV2};
use near_performance_metrics_macros::perf;
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::{
    ShardStateSyncResponse, ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV2,
};
use near_primitives::types::ShardId;
use near_store::adapter::chain_store::ChainStoreAdapter;
use parking_lot::Mutex;

use crate::metrics;

/// Actor that handles state sync requests.
pub struct StateRequestActor {
    clock: Clock,
    state_sync_adapter: ChainStateSyncAdapter,
    chain_store: ChainStoreAdapter,
    genesis_hash: CryptoHash,
    throttle_period: Duration,
    num_state_requests_per_throttle_period: usize,
    state_request_timestamps: Arc<Mutex<VecDeque<Instant>>>,
}

impl Actor for StateRequestActor {}

/// Result of sync hash validation for request processing
enum SyncHashValidationResult {
    Valid,      // Proceed with operation
    Invalid,    // Return None response, continue processing
    BadRequest, // Don't respond to the node, because the request is malformed.
}

impl StateRequestActor {
    pub fn new(
        clock: Clock,
        runtime: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        genesis_hash: CryptoHash,
        throttle_period: Duration,
        num_state_requests_per_throttle_period: usize,
    ) -> Self {
        let chain_store = ChainStoreAdapter::new(runtime.store().clone());
        let state_sync_adapter = ChainStateSyncAdapter::new(
            clock.clone(),
            chain_store.clone(),
            epoch_manager,
            runtime.clone(),
        );
        Self {
            clock,
            state_sync_adapter,
            chain_store,
            genesis_hash,
            throttle_period,
            num_state_requests_per_throttle_period,
            state_request_timestamps: Arc::new(Mutex::new(VecDeque::with_capacity(
                num_state_requests_per_throttle_period,
            ))),
        }
    }

    /// Returns true if this request needs to be **dropped** due to exceeding a
    /// rate limit of state sync requests.
    fn throttle_state_sync_request(&self) -> bool {
        let mut timestamps = self.state_request_timestamps.lock();
        let now = self.clock.now();
        while let Some(&instant) = timestamps.front() {
            // Assume that time is linear. While in different threads there might be some small differences,
            // it should not matter in practice.
            if now - instant > self.throttle_period {
                timestamps.pop_front();
            } else {
                break;
            }
        }
        if timestamps.len() >= self.num_state_requests_per_throttle_period {
            return true;
        }
        timestamps.push_back(now);
        false
    }

    fn get_sync_hash(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Option<CryptoHash>, near_chain::Error> {
        if block_hash == &self.genesis_hash {
            // We shouldn't be trying to sync state from before the genesis block
            return Ok(None);
        }
        let header = self.chain_store.get_block_header(block_hash)?;
        self.chain_store.get_current_epoch_sync_hash(header.epoch_id())
    }

    // TODO(darioush): Remove the code duplication with Chain.
    fn check_sync_hash_validity(
        &self,
        sync_hash: &near_primitives::hash::CryptoHash,
    ) -> Result<bool, near_chain::Error> {
        // It's important to check that Block exists because we will sync with it.
        // Do not replace with `get_block_header()`.
        let _sync_block = self.chain_store.get_block(sync_hash)?;

        let good_sync_hash = self.get_sync_hash(sync_hash)?;
        Ok(good_sync_hash.as_ref() == Some(sync_hash))
    }

    /// Validates sync hash and returns appropriate action to take.
    fn validate_sync_hash(&self, sync_hash: &CryptoHash) -> SyncHashValidationResult {
        match self.check_sync_hash_validity(sync_hash) {
            Ok(true) => SyncHashValidationResult::Valid,
            Ok(false) => {
                tracing::warn!(target: "sync", "sync_hash didn't pass validation, possible malicious behavior");
                SyncHashValidationResult::BadRequest
            }
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                // This case may appear in case of latency in epoch switching.
                // Request sender is ready to sync but we still didn't get the block.
                tracing::info!(target: "sync", "Can't get sync_hash block for state request");
                SyncHashValidationResult::Invalid
            }
            Err(err) => {
                tracing::error!(target: "sync", ?err, "Failed to verify sync_hash validity");
                SyncHashValidationResult::Invalid
            }
        }
    }
}

fn new_header_response(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    header: ShardStateSyncResponseHeaderV2,
) -> StateResponse {
    let state_response = ShardStateSyncResponse::new_from_header(Some(header));
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StateResponse(Box::new(info))
}

fn new_header_response_empty(shard_id: ShardId, sync_hash: CryptoHash) -> StateResponse {
    let state_response = ShardStateSyncResponse::new_from_header(None);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StateResponse(Box::new(info))
}

fn new_part_response(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    part_id: u64,
    part: Option<Vec<u8>>,
) -> StateResponse {
    let part = part.map(|part| (part_id, part));
    let state_response = ShardStateSyncResponse::new_from_part(part);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StateResponse(Box::new(info))
}

fn new_part_response_empty(shard_id: ShardId, sync_hash: CryptoHash) -> StateResponse {
    let state_response = ShardStateSyncResponse::new_from_part(None);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StateResponse(Box::new(info))
}

impl Handler<StateRequestHeader> for StateRequestActor {
    #[perf]
    fn handle(&mut self, msg: StateRequestHeader) -> Option<StateResponse> {
        let StateRequestHeader { shard_id, sync_hash } = msg;
        let _timer = metrics::STATE_SYNC_REQUEST_TIME
            .with_label_values(&["StateRequestHeader"])
            .start_timer();
        let _span =
            tracing::debug_span!(target: "sync", "StateRequestHeader", ?shard_id, ?sync_hash)
                .entered();

        if self.throttle_state_sync_request() {
            tracing::debug!(target: "sync", "Throttling state sync request for shard");
            metrics::STATE_SYNC_REQUESTS_THROTTLED_TOTAL.inc();
            return None;
        }

        tracing::debug!(target: "sync", "Handle state request header");

        match self.validate_sync_hash(&sync_hash) {
            SyncHashValidationResult::Valid => {
                // The request is valid - proceed.
            }
            SyncHashValidationResult::Invalid => {
                // The request is invalid - could not be validated - return empty response.
                return Some(new_header_response_empty(shard_id, sync_hash));
            }
            SyncHashValidationResult::BadRequest => {
                // The request is malformed - do not respond.
                return None;
            }
        };

        let header = self.state_sync_adapter.get_state_response_header(shard_id, sync_hash);
        let Ok(header) = header else {
            tracing::error!(target: "sync", "Cannot build state sync header");
            return Some(new_header_response_empty(shard_id, sync_hash));
        };
        let ShardStateSyncResponseHeader::V2(header) = header else {
            tracing::error!(target: "sync", "Invalid state sync header format");
            return None;
        };

        let response = new_header_response(shard_id, sync_hash, header);
        Some(response)
    }
}

impl Handler<StateRequestPart> for StateRequestActor {
    #[perf]
    fn handle(&mut self, msg: StateRequestPart) -> Option<StateResponse> {
        let StateRequestPart { shard_id, sync_hash, part_id } = msg;
        let _timer =
            metrics::STATE_SYNC_REQUEST_TIME.with_label_values(&["StateRequestPart"]).start_timer();
        let _span =
            tracing::debug_span!(target: "sync", "StateRequestPart", ?shard_id, ?sync_hash, part_id)
                .entered();

        tracing::debug!(target: "sync", "Handle state request part");

        if self.throttle_state_sync_request() {
            metrics::STATE_SYNC_REQUESTS_THROTTLED_TOTAL.inc();
            return None;
        }

        tracing::debug!(target: "sync", "Computing state request part");
        match self.validate_sync_hash(&sync_hash) {
            SyncHashValidationResult::Valid => {
                // The request is valid - proceed.
            }
            SyncHashValidationResult::BadRequest => {
                // Do not respond, possible malicious behavior.
                return None;
            }
            SyncHashValidationResult::Invalid => {
                // The request is invalid - could not be validated - return empty response.
                return Some(new_part_response_empty(shard_id, sync_hash));
            }
        };

        let part = self.state_sync_adapter.get_state_response_part(shard_id, part_id, sync_hash);
        let Ok(part) = part else {
            tracing::error!(target: "sync", ?part, "Cannot build state part");
            return Some(new_part_response_empty(shard_id, sync_hash));
        };
        tracing::trace!(target: "sync", "Finished computation for state request part");

        let response = new_part_response(shard_id, sync_hash, part_id, Some(part));
        Some(response)
    }
}
