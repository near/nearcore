use crate::metrics;
use near_async::messaging::{Actor, Handler};
use near_async::time::{Clock, Duration, Instant};
use near_chain::Error;
use near_chain::state_sync::ChainStateSyncAdapter;
use near_chain::types::RuntimeAdapter;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::{StatePartOrHeader, StateRequestHeader, StateRequestPart};
use near_network::types::{StateResponseInfo, StateResponseInfoV2};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::StatePart;
use near_primitives::state_sync::{
    ShardStateSyncResponse, ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV2,
};
use near_primitives::types::ShardId;
use near_primitives::version::ProtocolVersion;
use near_store::adapter::chain_store::ChainStoreAdapter;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

/// Actor that handles state sync requests.
pub struct StateRequestActor {
    clock: Clock,
    state_sync_adapter: ChainStateSyncAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    chain_store: ChainStoreAdapter,
    genesis_hash: CryptoHash,
    throttle_period: Duration,
    num_state_requests_per_throttle_period: usize,
    state_request_timestamps: Arc<Mutex<VecDeque<Instant>>>,
}

impl Actor for StateRequestActor {}

/// Result of sync hash validation for request processing
#[derive(PartialEq)]
enum SyncHashValidationResult {
    Valid,    // Proceed with operation
    Rejected, // Don't respond; the sync hash is invalid, from an old epoch, or can't be verified.
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
            epoch_manager.clone(),
            runtime.clone(),
        );
        Self {
            clock,
            state_sync_adapter,
            epoch_manager,
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
        Ok(self.chain_store.get_current_epoch_sync_hash(header.epoch_id()))
    }

    /// Checks if the sync_hash belongs to an epoch that we know.
    /// We allow sync_hash from the current epoch and the immediately previous epoch.
    fn is_sync_hash_from_known_recent_epoch(&self, sync_hash: &CryptoHash) -> Result<bool, Error> {
        let head = self.chain_store.head()?;
        let sync_block = match self.chain_store.get_block(sync_hash) {
            Ok(block) => block,
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                // The block may be missing because it was garbage-collected or because
                // this node hasn't switched to the new epoch yet. Either way, we can't
                // determine the epoch, so treat it as unknown and drop the request.
                tracing::debug!(target: "sync", ?sync_hash, "can't get sync_hash block for state request");
                return Ok(false);
            }
            Err(err) => {
                return Err(err);
            }
        };
        let sync_epoch_id = sync_block.header().epoch_id();

        if sync_epoch_id == &head.epoch_id {
            return Ok(true);
        }

        if let Ok(prev_epoch_id) =
            self.epoch_manager.get_prev_epoch_id_from_prev_block(&head.prev_block_hash)
        {
            if sync_epoch_id == &prev_epoch_id {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Validates sync hash and returns appropriate action to take.
    fn validate_sync_hash(&self, sync_hash: &CryptoHash) -> SyncHashValidationResult {
        match self.is_sync_hash_from_known_recent_epoch(sync_hash) {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!(
                    target: "sync",
                    ?sync_hash,
                    "sync_hash didn't pass validation; belongs to an unknown epoch"
                );
                return SyncHashValidationResult::Rejected;
            }
            Err(err) => {
                tracing::warn!(target: "sync", ?err, "failed to check sync_hash epoch");
                return SyncHashValidationResult::Rejected;
            }
        }

        let good_sync_hash = match self.get_sync_hash(sync_hash) {
            Ok(sync_hash) => sync_hash,
            Err(err) => {
                tracing::debug!(target: "sync", ?err, "failed to get sync_hash for state request");
                return SyncHashValidationResult::Rejected;
            }
        };

        if good_sync_hash.as_ref() == Some(sync_hash) {
            SyncHashValidationResult::Valid
        } else {
            tracing::warn!(
                target: "sync",
                "sync_hash didn't pass validation; possible divergence in sync hash computation"
            );
            SyncHashValidationResult::Rejected
        }
    }

    /// Returns the protocol version used in the epoch containing the sync block.
    /// This is used for synchronization related to the state parts format (see #14013 for details).
    fn get_protocol_version_from_sync_hash(
        &self,
        sync_hash: &CryptoHash,
    ) -> Result<ProtocolVersion, EpochError> {
        let epoch_id = self.epoch_manager.get_epoch_id(sync_hash)?;
        self.epoch_manager.get_epoch_protocol_version(&epoch_id)
    }
}

fn new_header_response(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    header: ShardStateSyncResponseHeaderV2,
    protocol_version: ProtocolVersion,
) -> StatePartOrHeader {
    let state_response = ShardStateSyncResponse::new_from_header(Some(header), protocol_version);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StatePartOrHeader(Box::new(info))
}

fn new_header_response_empty(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    protocol_version: ProtocolVersion,
) -> StatePartOrHeader {
    let state_response = ShardStateSyncResponse::new_from_header(None, protocol_version);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StatePartOrHeader(Box::new(info))
}

fn new_part_response(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    part_id: u64,
    part: Option<StatePart>,
    protocol_version: ProtocolVersion,
) -> StatePartOrHeader {
    let part = part.map(|part| (part_id, part));
    let state_response = ShardStateSyncResponse::new_from_part(part, protocol_version);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StatePartOrHeader(Box::new(info))
}

fn new_part_response_empty(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    protocol_version: ProtocolVersion,
) -> StatePartOrHeader {
    let state_response = ShardStateSyncResponse::new_from_part(None, protocol_version);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StatePartOrHeader(Box::new(info))
}

impl Handler<StateRequestHeader, Option<StatePartOrHeader>> for StateRequestActor {
    fn handle(&mut self, msg: StateRequestHeader) -> Option<StatePartOrHeader> {
        let StateRequestHeader { shard_id, sync_hash } = msg;
        let _timer = metrics::STATE_SYNC_REQUEST_TIME
            .with_label_values(&["StateRequestHeader"])
            .start_timer();
        let _span =
            tracing::debug_span!(target: "sync", "StateRequestHeader", ?shard_id, ?sync_hash)
                .entered();

        if self.throttle_state_sync_request() {
            tracing::debug!(target: "sync", "throttling state sync request for shard");
            metrics::STATE_SYNC_REQUESTS_THROTTLED_TOTAL.inc();
            return None;
        }

        if self.validate_sync_hash(&sync_hash) == SyncHashValidationResult::Rejected {
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL
                .with_label_values(&["header", "failed"])
                .inc();
            return None;
        }

        let protocol_version = self
            .get_protocol_version_from_sync_hash(&sync_hash)
            .inspect_err(|err| {
                tracing::debug!(target: "sync", ?err, "failed to get sync_hash protocol version");
                metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL
                    .with_label_values(&["header", "failed"])
                    .inc();
            })
            .ok()?;

        let header = self.state_sync_adapter.get_state_response_header(shard_id, sync_hash);
        let Ok(header) = header else {
            tracing::warn!(target: "sync", "cannot build state sync header");
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL
                .with_label_values(&["header", "failed"])
                .inc();
            return Some(new_header_response_empty(shard_id, sync_hash, protocol_version));
        };
        let ShardStateSyncResponseHeader::V2(header) = header else {
            tracing::warn!(target: "sync", "invalid state sync header format");
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL
                .with_label_values(&["header", "failed"])
                .inc();
            return None;
        };

        metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL.with_label_values(&["header", "success"]).inc();
        let response = new_header_response(shard_id, sync_hash, header, protocol_version);
        Some(response)
    }
}

impl Handler<StateRequestPart, Option<StatePartOrHeader>> for StateRequestActor {
    fn handle(&mut self, msg: StateRequestPart) -> Option<StatePartOrHeader> {
        let StateRequestPart { shard_id, sync_hash, part_id } = msg;
        let _timer =
            metrics::STATE_SYNC_REQUEST_TIME.with_label_values(&["StateRequestPart"]).start_timer();
        let _span =
            tracing::debug_span!(target: "sync", "StateRequestPart", ?shard_id, ?sync_hash, part_id)
                .entered();

        tracing::debug!(target: "sync", "handle state request part");

        if self.throttle_state_sync_request() {
            metrics::STATE_SYNC_REQUESTS_THROTTLED_TOTAL.inc();
            return None;
        }

        if self.validate_sync_hash(&sync_hash) == SyncHashValidationResult::Rejected {
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL.with_label_values(&["part", "failed"]).inc();
            return None;
        }

        let protocol_version = self
            .get_protocol_version_from_sync_hash(&sync_hash)
            .inspect_err(|err| {
                tracing::debug!(target: "sync", ?err, "failed to get sync_hash protocol version");
                metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL
                    .with_label_values(&["part", "failed"])
                    .inc();
            })
            .ok()?;

        tracing::debug!(target: "sync", "computing state request part");
        let part = self.state_sync_adapter.get_state_response_part(shard_id, part_id, sync_hash);
        let Ok(part) = part else {
            tracing::warn!(target: "sync", ?part, "cannot build state part");
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL.with_label_values(&["part", "failed"]).inc();
            return Some(new_part_response_empty(shard_id, sync_hash, protocol_version));
        };
        tracing::trace!(target: "sync", "finished computation for state request part");

        metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL.with_label_values(&["part", "success"]).inc();
        let response =
            new_part_response(shard_id, sync_hash, part_id, Some(part), protocol_version);
        Some(response)
    }
}
