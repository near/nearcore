use crate::metrics;
use near_async::ActorSystem;
use near_async::messaging::{Actor, Handler};
use near_async::multithread::MultithreadRuntimeHandle;
use near_async::time::{Clock, Duration, Instant};
use near_chain::Error;
use near_chain::state_sync::ChainStateSyncAdapter;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::ClientConfig;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::{
    StatePartOrHeader, StateRequestHeader, StateRequestPart, StateRequestResponse,
};
use near_network::types::{StateResponseInfo, StateResponseInfoV2};
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::{StatePart, StatePartIndex};
use near_primitives::state_sync::{ShardStateSyncResponse, ShardStateSyncResponseHeader};
use near_primitives::types::ShardId;
use near_store::adapter::chain_store::ChainStoreAdapter;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

/// Admits at most `limit` state requests in any window of `period`. Clones share the window, so that
/// one throttle limits all the actors which serve the requests.
#[derive(Clone)]
pub struct StateRequestThrottle {
    clock: Clock,
    period: Duration,
    limit: usize,
    /// When the requests in the window were admitted, oldest first.
    admitted: Arc<Mutex<VecDeque<Instant>>>,
}

impl StateRequestThrottle {
    fn new(clock: Clock, period: Duration, limit: usize) -> Self {
        Self { clock, period, limit, admitted: Arc::new(Mutex::new(VecDeque::new())) }
    }

    /// The throttle of a node with `config`. `state_requests_per_throttle_period` is the limit per
    /// state request server thread, and one window holds the requests of all the threads.
    pub fn from_config(clock: Clock, config: &ClientConfig) -> Self {
        let limit = config
            .state_requests_per_throttle_period
            .saturating_mul(config.state_request_server_threads);
        Self::new(clock, config.state_request_throttle_period, limit)
    }

    /// Admits a request unless `limit` requests were admitted in the last `period`. Requests which
    /// aren't admitted don't count in the window, only in the throttled requests metric.
    fn try_admit(&self) -> bool {
        let mut admitted = self.admitted.lock();
        // The clock is read under the lock, so that the timestamps stay ordered.
        let now = self.clock.now();
        while let Some(&instant) = admitted.front() {
            if now - instant > self.period {
                admitted.pop_front();
            } else {
                break;
            }
        }
        if admitted.len() < self.limit {
            admitted.push_back(now);
            return true;
        }
        drop(admitted);
        tracing::debug!(target: "sync", "throttling state sync request");
        metrics::STATE_SYNC_REQUESTS_THROTTLED_TOTAL.inc();
        false
    }
}

/// Actor that handles state sync requests.
pub struct StateRequestActor {
    state_sync_adapter: ChainStateSyncAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    chain_store: ChainStoreAdapter,
    genesis_hash: CryptoHash,
    throttle: StateRequestThrottle,
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
        throttle: StateRequestThrottle,
    ) -> Self {
        let chain_store = ChainStoreAdapter::new(runtime.store().clone());
        let state_sync_adapter =
            ChainStateSyncAdapter::new(clock, chain_store.clone(), epoch_manager.clone(), runtime);
        Self { state_sync_adapter, epoch_manager, chain_store, genesis_hash, throttle }
    }

    /// Spawns one actor per state request server thread. The actors share one throttle, so that it
    /// limits the node as a whole.
    pub fn spawn_multithread_actor(
        clock: Clock,
        actor_system: ActorSystem,
        runtime: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        genesis_hash: CryptoHash,
        config: &ClientConfig,
    ) -> MultithreadRuntimeHandle<Self> {
        actor_system.spawn_multithread_actor(
            config.state_request_server_threads,
            Self::factory(clock, runtime, epoch_manager, genesis_hash, config),
        )
    }

    /// Makes the actors of the state request server threads, all with clones of one throttle.
    fn factory(
        clock: Clock,
        runtime: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        genesis_hash: CryptoHash,
        config: &ClientConfig,
    ) -> impl Fn() -> Self + Send + Sync + 'static {
        let throttle = StateRequestThrottle::from_config(clock.clone(), config);
        move || {
            Self::new(
                clock.clone(),
                runtime.clone(),
                epoch_manager.clone(),
                genesis_hash,
                throttle.clone(),
            )
        }
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
}

fn new_header_response(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    header: ShardStateSyncResponseHeader,
) -> StatePartOrHeader {
    let state_response = ShardStateSyncResponse::new_from_header(header);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StatePartOrHeader(Box::new(info))
}

fn new_part_response(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    part_idx: StatePartIndex,
    part: StatePart,
) -> StatePartOrHeader {
    let state_response = ShardStateSyncResponse::new_from_part(part_idx, part);
    let state_response_info = StateResponseInfoV2 { shard_id, sync_hash, state_response };
    let info = StateResponseInfo::V2(Box::new(state_response_info));
    StatePartOrHeader(Box::new(info))
}

impl Handler<StateRequestHeader, StateRequestResponse> for StateRequestActor {
    fn handle(&mut self, msg: StateRequestHeader) -> StateRequestResponse {
        let StateRequestHeader { shard_id, sync_hash } = msg;
        let _timer = metrics::STATE_SYNC_REQUEST_TIME
            .with_label_values(&["StateRequestHeader"])
            .start_timer();
        let _span =
            tracing::debug_span!(target: "sync", "StateRequestHeader", ?shard_id, ?sync_hash)
                .entered();

        if !self.throttle.try_admit() {
            return StateRequestResponse::Busy;
        }

        if self.validate_sync_hash(&sync_hash) == SyncHashValidationResult::Rejected {
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL
                .with_label_values(&["header", "failed"])
                .inc();
            return StateRequestResponse::Error;
        }

        let header = self.state_sync_adapter.get_state_response_header(shard_id, sync_hash);
        let Ok(header) = header else {
            tracing::warn!(target: "sync", "cannot build state sync header");
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL
                .with_label_values(&["header", "failed"])
                .inc();
            return StateRequestResponse::Error;
        };
        metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL.with_label_values(&["header", "success"]).inc();
        let response = new_header_response(shard_id, sync_hash, header);
        StateRequestResponse::Respond(response)
    }
}

impl Handler<StateRequestPart, StateRequestResponse> for StateRequestActor {
    fn handle(&mut self, msg: StateRequestPart) -> StateRequestResponse {
        let StateRequestPart { shard_id, sync_hash, part_idx } = msg;
        let _timer =
            metrics::STATE_SYNC_REQUEST_TIME.with_label_values(&["StateRequestPart"]).start_timer();
        let _span =
            tracing::debug_span!(target: "sync", "StateRequestPart", ?shard_id, ?sync_hash, part_idx)
                .entered();

        tracing::debug!(target: "sync", "handle state request part");

        if !self.throttle.try_admit() {
            return StateRequestResponse::Busy;
        }

        if self.validate_sync_hash(&sync_hash) == SyncHashValidationResult::Rejected {
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL.with_label_values(&["part", "failed"]).inc();
            return StateRequestResponse::Error;
        }

        tracing::debug!(target: "sync", "computing state request part");
        let part = self.state_sync_adapter.get_state_response_part(shard_id, part_idx, sync_hash);
        let Ok(part) = part else {
            tracing::warn!(target: "sync", ?part, "cannot build state part");
            metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL.with_label_values(&["part", "failed"]).inc();
            return StateRequestResponse::Error;
        };
        tracing::trace!(target: "sync", "finished computation for state request part");

        metrics::STATE_SYNC_REQUESTS_SERVED_TOTAL.with_label_values(&["part", "success"]).inc();
        let response = new_part_response(shard_id, sync_hash, part_idx, part);
        StateRequestResponse::Respond(response)
    }
}

#[cfg(test)]
mod tests {
    use super::{StateRequestActor, StateRequestThrottle};
    use near_async::messaging::Handler;
    use near_async::time::{Duration, FakeClock};
    use near_chain::test_utils::setup;
    use near_chain_configs::ClientConfig;
    use near_chain_configs::test_utils::TestClientConfigParams;
    use near_network::client::{StateRequestHeader, StateRequestPart, StateRequestResponse};
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::ShardId;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    /// Differs from the default period, so that a throttle which ignores the config is caught.
    const PERIOD: Duration = Duration::seconds(20);

    /// A config whose throttle admits `per_thread` requests per `PERIOD` for each of `threads`
    /// state request server threads.
    fn config(per_thread: usize, threads: usize) -> ClientConfig {
        let mut config = ClientConfig::test(TestClientConfigParams {
            skip_sync_wait: false,
            min_block_prod_time: 100,
            max_block_prod_time: 200,
            num_block_producer_seats: 1,
            archive: false,
            transaction_pool_size_limit: None,
        });
        config.state_request_throttle_period = PERIOD;
        config.state_requests_per_throttle_period = per_thread;
        config.state_request_server_threads = threads;
        config
    }

    #[test]
    fn admits_up_to_limit() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 3);
        for _ in 0..3 {
            assert!(throttle.try_admit());
        }
        assert!(!throttle.try_admit());
    }

    #[test]
    fn window_admits_again_after_period() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 1);
        assert!(throttle.try_admit());
        // A request admitted exactly `PERIOD` ago still counts.
        clock.advance(PERIOD);
        assert!(!throttle.try_admit());
        clock.advance(Duration::nanoseconds(1));
        assert!(throttle.try_admit());
    }

    #[test]
    fn zero_limit_admits_nothing() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 0);
        assert!(!throttle.try_admit());
        clock.advance(PERIOD + Duration::nanoseconds(1));
        assert!(!throttle.try_admit());
    }

    #[test]
    fn zero_period_admits_one_per_instant() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), Duration::ZERO, 1);
        assert!(throttle.try_admit());
        assert!(!throttle.try_admit());
        clock.advance(Duration::nanoseconds(1));
        assert!(throttle.try_admit());
    }

    #[test]
    fn rejected_requests_not_counted() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 1);
        assert!(throttle.try_admit());
        clock.advance(PERIOD / 2);
        assert!(!throttle.try_admit());
        // The admitted request expires, and the rejected one doesn't take its place.
        clock.advance(PERIOD / 2 + Duration::nanoseconds(1));
        assert!(throttle.try_admit());
    }

    #[test]
    fn clones_share_window() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 2);
        let clone = throttle.clone();
        assert!(throttle.try_admit());
        assert!(clone.try_admit());
        assert!(!throttle.try_admit());
        assert!(!clone.try_admit());
    }

    /// The actors which the node spawns for its state request server threads share one throttle,
    /// so that it limits the node as a whole.
    #[test]
    fn actors_share_throttle() {
        let clock = FakeClock::default();
        let (chain, epoch_manager, runtime, _) = setup(clock.clock());
        // The node admits 2 requests per period.
        let make_actor = StateRequestActor::factory(
            clock.clock(),
            runtime,
            epoch_manager,
            *chain.genesis().hash(),
            &config(1, 2),
        );
        let mut first = make_actor();
        let mut second = make_actor();

        // Admitted requests fail for their unknown sync hash.
        let (shard_id, sync_hash) = (ShardId::new(0), CryptoHash::hash_bytes(&[1]));
        let header = StateRequestHeader { shard_id, sync_hash };
        let part = StateRequestPart { shard_id, sync_hash, part_idx: 0 };
        assert_eq!(first.handle(header.clone()), StateRequestResponse::Error);
        assert_eq!(second.handle(header.clone()), StateRequestResponse::Error);
        assert_eq!(first.handle(part.clone()), StateRequestResponse::Busy);
        assert_eq!(second.handle(part.clone()), StateRequestResponse::Busy);

        clock.advance(PERIOD + Duration::nanoseconds(1));
        assert_eq!(second.handle(part.clone()), StateRequestResponse::Error);
        assert_eq!(second.handle(part.clone()), StateRequestResponse::Error);
        assert_eq!(first.handle(part), StateRequestResponse::Busy);
        assert_eq!(first.handle(header), StateRequestResponse::Busy);
    }

    #[test]
    fn config_limit_scales_with_threads() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::from_config(clock.clock(), &config(3, 4));
        assert_eq!(throttle.period, PERIOD);
        for _ in 0..12 {
            assert!(throttle.try_admit());
        }
        assert!(!throttle.try_admit());

        let throttle = StateRequestThrottle::from_config(clock.clock(), &config(usize::MAX, 4));
        assert_eq!(throttle.limit, usize::MAX);

        let throttle = StateRequestThrottle::from_config(clock.clock(), &config(usize::MAX, 0));
        assert_eq!(throttle.limit, 0);
    }

    #[test]
    fn concurrent_threads_admit_up_to_limit() {
        const THREADS: usize = 8;
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 100);
        let admitted = AtomicUsize::new(0);
        // All the threads start admitting at once.
        let barrier = Barrier::new(THREADS);
        thread::scope(|scope| {
            for _ in 0..THREADS {
                let (throttle, admitted, barrier) = (throttle.clone(), &admitted, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..50 {
                        if throttle.try_admit() {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(admitted.into_inner(), 100);
    }
}
