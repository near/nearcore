use std::collections::VecDeque;
use std::sync::Arc;

use near_async::actix_wrapper::{ActixWrapper, spawn_actix_actor};
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
    ShardStateSyncResponse, ShardStateSyncResponseHeader, ShardStateSyncResponseV3,
};
use near_store::adapter::chain_store::ChainStoreAdapter;
use parking_lot::Mutex;
use tracing::{error, info, warn};

use crate::metrics;

pub type StateRequestActor = ActixWrapper<StateRequestActorInner>;

pub struct StateRequestActorInner {
    clock: Clock,
    genesis_hash: CryptoHash,
    state_request_cache: Arc<Mutex<VecDeque<Instant>>>,
    config: StateRequestActorConfig,
    state_sync_adapter: ChainStateSyncAdapter,
    chain_store: ChainStoreAdapter,
}

pub struct StateRequestActorConfig {
    pub view_client_throttle_period: Duration,
    pub view_client_num_state_requests_per_throttle_period: usize,
}

impl StateRequestActorInner {
    pub fn spawn_actix_actor(
        clock: Clock,
        genesis_hash: CryptoHash,
        config: StateRequestActorConfig,
        runtime: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> actix::Addr<StateRequestActor> {
        let actor =
            StateRequestActorInner::new(clock, genesis_hash, config, runtime, epoch_manager);
        let (addr, _arbiter) = spawn_actix_actor(actor);
        addr
    }

    pub fn new(
        clock: Clock,
        genesis_hash: CryptoHash,
        config: StateRequestActorConfig,
        runtime: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
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
            genesis_hash,
            state_request_cache: Arc::new(Mutex::new(VecDeque::default())),
            config,
            state_sync_adapter,
            chain_store,
        }
    }

    /// Returns true if this request needs to be **dropped** due to exceeding a
    /// rate limit of state sync requests.
    fn throttle_state_sync_request(&self) -> bool {
        let mut cache = self.state_request_cache.lock();
        let now = self.clock.now();
        while let Some(&instant) = cache.front() {
            if now - instant > self.config.view_client_throttle_period {
                cache.pop_front();
            } else {
                // Assume that time is linear. While in different threads there might be some small differences,
                // it should not matter in practice.
                break;
            }
        }
        if cache.len() >= self.config.view_client_num_state_requests_per_throttle_period {
            return true;
        }
        cache.push_back(now);
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

    // XXX: TODO: Remove the code duplication with Chain.
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
}

impl Actor for StateRequestActorInner {}

impl Handler<StateRequestHeader> for StateRequestActorInner {
    #[perf]
    fn handle(&mut self, msg: StateRequestHeader) -> Option<StateResponse> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["StateRequestHeader"])
            .start_timer();
        let StateRequestHeader { shard_id, sync_hash } = msg;
        if self.throttle_state_sync_request() {
            metrics::STATE_SYNC_REQUESTS_THROTTLED_TOTAL.inc();
            return None;
        }
        let header = match self.check_sync_hash_validity(&sync_hash) {
            Ok(true) => {
                match self.state_sync_adapter.get_state_response_header(shard_id, sync_hash) {
                    Ok(header) => Some(header),
                    Err(err) => {
                        error!(target: "sync", ?err, "Cannot build state sync header");
                        None
                    }
                }
            }
            Ok(false) => {
                warn!(target: "sync", ?sync_hash, "sync_hash didn't pass validation, possible malicious behavior");
                // Don't respond to the node, because the request is malformed.
                return None;
            }
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                // This case may appear in case of latency in epoch switching.
                // Request sender is ready to sync but we still didn't get the block.
                info!(target: "sync", ?sync_hash, "Can't get sync_hash block for state request header");
                None
            }
            Err(err) => {
                error!(target: "sync", ?err, ?sync_hash, "Failed to verify sync_hash validity");
                None
            }
        };
        let state_response = match header {
            Some(header) => {
                let header = match header {
                    ShardStateSyncResponseHeader::V2(inner) => inner,
                    _ => {
                        tracing::error!(target: "sync", ?sync_hash, %shard_id, "Invalid state sync header format");
                        return None;
                    }
                };

                ShardStateSyncResponse::V3(ShardStateSyncResponseV3 {
                    header: Some(header),
                    part: None,
                    cached_parts: None,  // Unused
                    can_generate: false, // Unused
                })
            }
            None => ShardStateSyncResponse::V3(ShardStateSyncResponseV3 {
                header: None,
                part: None,
                cached_parts: None,  // Unused
                can_generate: false, // Unused
            }),
        };
        let info = StateResponseInfo::V2(Box::new(StateResponseInfoV2 {
            shard_id,
            sync_hash,
            state_response,
        }));
        Some(StateResponse(Box::new(info)))
    }
}

impl Handler<StateRequestPart> for StateRequestActorInner {
    #[perf]
    fn handle(&mut self, msg: StateRequestPart) -> Option<StateResponse> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["StateRequestPart"])
            .start_timer();
        let StateRequestPart { shard_id, sync_hash, part_id } = msg;
        if self.throttle_state_sync_request() {
            metrics::STATE_SYNC_REQUESTS_THROTTLED_TOTAL.inc();
            return None;
        }
        tracing::debug!(target: "sync", %shard_id, ?sync_hash, ?part_id, "Computing state request part");
        let part = match self.check_sync_hash_validity(&sync_hash) {
            Ok(true) => {
                let part = match self
                    .state_sync_adapter
                    .get_state_response_part(shard_id, part_id, sync_hash)
                {
                    Ok(part) => Some((part_id, part)),
                    Err(err) => {
                        error!(target: "sync", ?err, ?sync_hash, %shard_id, part_id, "Cannot build state part");
                        None
                    }
                };

                tracing::trace!(target: "sync", ?sync_hash, %shard_id, part_id, "Finished computation for state request part");
                part
            }
            Ok(false) => {
                warn!(target: "sync", ?sync_hash, %shard_id, "sync_hash didn't pass validation, possible malicious behavior");
                // Do not respond, possible malicious behavior.
                return None;
            }
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                // This case may appear in case of latency in epoch switching.
                // Request sender is ready to sync but we still didn't get the block.
                info!(target: "sync", ?sync_hash, "Can't get sync_hash block for state request part");
                None
            }
            Err(err) => {
                error!(target: "sync", ?err, ?sync_hash, "Failed to verify sync_hash validity");
                None
            }
        };
        let state_response = ShardStateSyncResponse::V3(ShardStateSyncResponseV3 {
            header: None,
            part,
            cached_parts: None,  // Unused
            can_generate: false, // Unused
        });
        let info = StateResponseInfo::V2(Box::new(StateResponseInfoV2 {
            shard_id,
            sync_hash,
            state_response,
        }));
        Some(StateResponse(Box::new(info)))
    }
}
