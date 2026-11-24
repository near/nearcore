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
    StatePartOrHeader, StateRequestHeader, StateRequestPart, StateRequestResponse, StateRequester,
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

/// Clones share one sliding window.
#[derive(Clone)]
pub struct StateRequestThrottle {
    clock: Clock,
    period: Duration,
    limit: usize,
    /// Admissions which only validators may take. Validators may take the others too.
    reserved_for_validators: usize,
    admitted: Arc<Mutex<Admitted>>,
}

/// Admission times, oldest first.
#[derive(Default)]
struct Admitted {
    validators: VecDeque<Instant>,
    others: VecDeque<Instant>,
}

impl StateRequestThrottle {
    fn new(clock: Clock, period: Duration, limit: usize, reserved_for_validators: usize) -> Self {
        Self { clock, period, limit, reserved_for_validators, admitted: Default::default() }
    }

    pub fn from_config(clock: Clock, config: &ClientConfig) -> Self {
        let threads = config.state_request_server_threads;
        let limit = config.state_requests_per_throttle_period.saturating_mul(threads);
        let reserved_for_validators = match config.state_requests_reserved_for_validators {
            Some(per_thread) => per_thread.saturating_mul(threads),
            None => limit / 4,
        };
        Self::new(clock, config.state_request_throttle_period, limit, reserved_for_validators)
    }

    fn try_admit(&self, requester: StateRequester) -> bool {
        let mut admitted = self.admitted.lock();
        let Admitted { validators, others } = &mut *admitted;
        // Read under the lock to keep the timestamps ordered.
        let now = self.clock.now();
        for times in [&mut *validators, &mut *others] {
            while times.front().is_some_and(|&instant| now - instant > self.period) {
                times.pop_front();
            }
        }
        let full = validators.len() + others.len() >= self.limit;
        let admit = match requester {
            StateRequester::Validator => !full,
            StateRequester::Other => {
                !full && others.len() < self.limit.saturating_sub(self.reserved_for_validators)
            }
        };
        if !admit {
            tracing::debug!(target: "sync", ?requester, "throttling state sync request");
            metrics::STATE_SYNC_REQUESTS_THROTTLED_TOTAL
                .with_label_values(&[<&str>::from(requester)])
                .inc();
            return false;
        }
        match requester {
            StateRequester::Validator => validators.push_back(now),
            StateRequester::Other => others.push_back(now),
        }
        true
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

    /// The actors share one throttle, so that it limits the node as a whole.
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
            Self::make_actor_fn(clock, runtime, epoch_manager, genesis_hash, config),
        )
    }

    fn make_actor_fn(
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
        let StateRequestHeader { shard_id, sync_hash, requester } = msg;
        let _timer = metrics::STATE_SYNC_REQUEST_TIME
            .with_label_values(&["StateRequestHeader"])
            .start_timer();
        let _span = tracing::debug_span!(
            target: "sync",
            "StateRequestHeader",
            ?shard_id,
            ?sync_hash,
            ?requester
        )
        .entered();

        if !self.throttle.try_admit(requester) {
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
        let StateRequestPart { shard_id, sync_hash, part_idx, requester } = msg;
        let _timer =
            metrics::STATE_SYNC_REQUEST_TIME.with_label_values(&["StateRequestPart"]).start_timer();
        let _span = tracing::debug_span!(
            target: "sync",
            "StateRequestPart",
            ?shard_id,
            ?sync_hash,
            part_idx,
            ?requester
        )
        .entered();

        tracing::debug!(target: "sync", "handle state request part");

        if !self.throttle.try_admit(requester) {
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
    use near_network::client::{
        StateRequestHeader, StateRequestPart, StateRequestResponse, StateRequester,
    };
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::ShardId;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    /// Not the default period, so that a throttle which ignores the config fails.
    const PERIOD: Duration = Duration::seconds(20);

    const REQUESTERS: [StateRequester; 2] = [StateRequester::Validator, StateRequester::Other];

    fn config(per_thread: usize, threads: usize, reserved: Option<usize>) -> ClientConfig {
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
        config.state_requests_reserved_for_validators = reserved;
        config
    }

    #[test]
    fn window_admits_again_after_period() {
        for requester in REQUESTERS {
            let clock = FakeClock::default();
            let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 1, 0);
            assert!(throttle.try_admit(requester));
            // A request admitted exactly `PERIOD` ago still counts.
            clock.advance(PERIOD);
            assert!(!throttle.try_admit(requester));
            clock.advance(Duration::nanoseconds(1));
            assert!(throttle.try_admit(requester));
        }
    }

    #[test]
    fn expiry_frees_only_expired_requests() {
        for requester in REQUESTERS {
            let clock = FakeClock::default();
            let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 2, 0);
            assert!(throttle.try_admit(requester));
            clock.advance(PERIOD / 2);
            assert!(throttle.try_admit(requester));
            assert!(!throttle.try_admit(requester));
            clock.advance(PERIOD / 2 + Duration::nanoseconds(1));
            assert!(throttle.try_admit(requester));
            assert!(!throttle.try_admit(requester));
        }
    }

    #[test]
    fn expiry_frees_both_classes() {
        for (old, new) in [
            (StateRequester::Other, StateRequester::Validator),
            (StateRequester::Validator, StateRequester::Other),
        ] {
            let clock = FakeClock::default();
            let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 3, 1);
            assert!(throttle.try_admit(old));
            assert!(throttle.try_admit(old));
            clock.advance(PERIOD / 2);
            assert!(throttle.try_admit(new));
            assert!(!throttle.try_admit(new));
            // The `old` requests have expired, so they no longer count.
            clock.advance(PERIOD / 2 + Duration::nanoseconds(1));
            assert!(throttle.try_admit(new));
        }
    }

    #[test]
    fn reserve_kept_for_validators() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 4, 1);
        for _ in 0..3 {
            assert!(throttle.try_admit(StateRequester::Other));
        }
        assert!(!throttle.try_admit(StateRequester::Other));
        assert!(throttle.try_admit(StateRequester::Validator));
        assert!(!throttle.try_admit(StateRequester::Validator));
        assert!(!throttle.try_admit(StateRequester::Other));
    }

    #[test]
    fn validators_use_whole_limit() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 4, 1);
        for _ in 0..4 {
            assert!(throttle.try_admit(StateRequester::Validator));
        }
        assert!(!throttle.try_admit(StateRequester::Validator));
        assert!(!throttle.try_admit(StateRequester::Other));
    }

    #[test]
    fn others_fill_what_validators_leave() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 4, 1);
        assert!(throttle.try_admit(StateRequester::Validator));
        assert!(throttle.try_admit(StateRequester::Validator));
        assert!(throttle.try_admit(StateRequester::Other));
        assert!(throttle.try_admit(StateRequester::Other));
        assert!(!throttle.try_admit(StateRequester::Other));
    }

    #[test]
    fn reserve_edges() {
        for reserved in [0, 1] {
            let clock = FakeClock::default();
            let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 0, reserved);
            for requester in REQUESTERS {
                assert!(!throttle.try_admit(requester));
            }
            clock.advance(PERIOD + Duration::nanoseconds(1));
            for requester in REQUESTERS {
                assert!(!throttle.try_admit(requester));
            }
        }

        // A reserve of the whole limit or more leaves nothing to other requesters.
        for reserved in [2, 3] {
            let clock = FakeClock::default();
            let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 2, reserved);
            assert!(!throttle.try_admit(StateRequester::Other));
            assert!(throttle.try_admit(StateRequester::Validator));
            assert!(throttle.try_admit(StateRequester::Validator));
            assert!(!throttle.try_admit(StateRequester::Validator));
            assert!(!throttle.try_admit(StateRequester::Other));
        }
    }

    #[test]
    fn actors_share_throttle() {
        let clock = FakeClock::default();
        let (chain, epoch_manager, runtime, _) = setup(clock.clock());
        let make_actor = StateRequestActor::make_actor_fn(
            clock.clock(),
            runtime,
            epoch_manager,
            *chain.genesis().hash(),
            &config(1, 2, Some(0)),
        );
        let mut first = make_actor();
        let mut second = make_actor();

        // Admitted requests fail for their unknown sync hash.
        let (shard_id, sync_hash) = (ShardId::new(0), CryptoHash::hash_bytes(&[1]));
        let requester = StateRequester::Other;
        let header = StateRequestHeader { shard_id, sync_hash, requester };
        let part = StateRequestPart { shard_id, sync_hash, part_idx: 0, requester };
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
    fn handlers_admit_by_requester() {
        let clock = FakeClock::default();
        let (chain, epoch_manager, runtime, _) = setup(clock.clock());
        let make_actor = StateRequestActor::make_actor_fn(
            clock.clock(),
            runtime,
            epoch_manager,
            *chain.genesis().hash(),
            &config(2, 1, Some(1)),
        );
        let mut actor = make_actor();

        // Admitted requests fail for their unknown sync hash.
        let (shard_id, sync_hash) = (ShardId::new(0), CryptoHash::hash_bytes(&[1]));
        let header = |requester| StateRequestHeader { shard_id, sync_hash, requester };
        let part = |requester| StateRequestPart { shard_id, sync_hash, part_idx: 0, requester };
        assert_eq!(actor.handle(header(StateRequester::Other)), StateRequestResponse::Error);
        assert_eq!(actor.handle(part(StateRequester::Other)), StateRequestResponse::Busy);
        assert_eq!(actor.handle(part(StateRequester::Validator)), StateRequestResponse::Error);

        clock.advance(PERIOD + Duration::nanoseconds(1));
        assert_eq!(actor.handle(part(StateRequester::Other)), StateRequestResponse::Error);
        assert_eq!(actor.handle(header(StateRequester::Other)), StateRequestResponse::Busy);
        assert_eq!(actor.handle(header(StateRequester::Validator)), StateRequestResponse::Error);
    }

    #[test]
    fn config_limit_scales_with_threads() {
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::from_config(clock.clock(), &config(3, 4, Some(1)));
        assert_eq!(throttle.period, PERIOD);
        for _ in 0..8 {
            assert!(throttle.try_admit(StateRequester::Other));
        }
        assert!(!throttle.try_admit(StateRequester::Other));
        for _ in 0..4 {
            assert!(throttle.try_admit(StateRequester::Validator));
        }
        assert!(!throttle.try_admit(StateRequester::Validator));

        let saturated = config(usize::MAX, 4, Some(usize::MAX));
        let throttle = StateRequestThrottle::from_config(clock.clock(), &saturated);
        assert_eq!((throttle.limit, throttle.reserved_for_validators), (usize::MAX, usize::MAX));

        // Unset, a quarter of the limit of the whole node is reserved. A quarter of the limit of
        // each thread would be 7 per thread, so 28.
        let throttle = StateRequestThrottle::from_config(clock.clock(), &config(30, 4, None));
        assert_eq!((throttle.limit, throttle.reserved_for_validators), (120, 30));
    }

    #[test]
    fn concurrent_threads_admit_up_to_limit() {
        const THREADS: usize = 8;
        let clock = FakeClock::default();
        let throttle = StateRequestThrottle::new(clock.clock(), PERIOD, 100, 0);
        let admitted = AtomicUsize::new(0);
        let barrier = Barrier::new(THREADS);
        thread::scope(|scope| {
            for _ in 0..THREADS {
                let (throttle, admitted, barrier) = (throttle.clone(), &admitted, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..50 {
                        if throttle.try_admit(StateRequester::Other) {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(admitted.into_inner(), 100);
    }
}
