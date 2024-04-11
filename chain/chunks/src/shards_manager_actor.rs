use crate::{
    adapter::ShardsManagerRequestFromClient, client::ShardsManagerResponse, ShardsManager,
};
use actix::{Actor, Addr, Arbiter, ArbiterHandle, Context, Handler};
use near_async::messaging::Sender;
use near_async::time::{Clock, Duration};
use near_chain::{chunks_store::ReadOnlyChunksStore, types::Tip};
use near_epoch_manager::{shard_tracker::ShardTracker, EpochManagerAdapter};
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork, types::PeerManagerMessageRequest,
};
use near_o11y::WithSpanContext;
use near_performance_metrics_macros::perf;
use near_primitives::types::AccountId;
use near_store::{DBCol, Store, HEADER_HEAD_KEY, HEAD_KEY};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub struct ShardsManagerActor {
    shards_mgr: ShardsManager,
    chunk_request_retry_period: Duration,
}

// Needed for DerefMut.
impl Deref for ShardsManagerActor {
    type Target = ShardsManager;

    fn deref(&self) -> &Self::Target {
        &self.shards_mgr
    }
}

// Needed to convert actix `Context<ShardsManagerActor>` to `dyn DelayedActionRunner<ShardsManager>`.
impl DerefMut for ShardsManagerActor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shards_mgr
    }
}

impl ShardsManagerActor {
    fn new(shards_mgr: ShardsManager, chunk_request_retry_period: Duration) -> Self {
        Self { shards_mgr, chunk_request_retry_period }
    }
}

impl Actor for ShardsManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.shards_mgr.periodically_resend_chunk_requests(ctx, self.chunk_request_retry_period);
    }
}

impl Handler<WithSpanContext<ShardsManagerRequestFromClient>> for ShardsManagerActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ShardsManagerRequestFromClient>,
        _ctx: &mut Context<Self>,
    ) {
        self.shards_mgr.handle_client_request(msg.msg);
    }
}

impl Handler<WithSpanContext<ShardsManagerRequestFromNetwork>> for ShardsManagerActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ShardsManagerRequestFromNetwork>,
        _ctx: &mut Context<Self>,
    ) {
        self.shards_mgr.handle_network_request(msg.msg);
    }
}

pub fn start_shards_manager(
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    network_adapter: Sender<PeerManagerMessageRequest>,
    client_adapter_for_shards_manager: Sender<ShardsManagerResponse>,
    me: Option<AccountId>,
    store: Store,
    chunk_request_retry_period: Duration,
) -> (Addr<ShardsManagerActor>, ArbiterHandle) {
    let shards_manager_arbiter = Arbiter::new();
    let shards_manager_arbiter_handle = shards_manager_arbiter.handle();
    // TODO: make some better API for accessing chain properties like head.
    let chain_head = store
        .get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)
        .unwrap()
        .expect("ShardsManager must be initialized after the chain is initialized");
    let chain_header_head = store
        .get_ser::<Tip>(DBCol::BlockMisc, HEADER_HEAD_KEY)
        .unwrap()
        .expect("ShardsManager must be initialized after the chain is initialized");
    let chunks_store = ReadOnlyChunksStore::new(store);
    let shards_manager = ShardsManager::new(
        Clock::real(),
        me,
        epoch_manager,
        shard_tracker,
        network_adapter,
        client_adapter_for_shards_manager,
        chunks_store,
        chain_head,
        chain_header_head,
    );
    let shards_manager_addr =
        ShardsManagerActor::start_in_arbiter(&shards_manager_arbiter_handle, move |_| {
            ShardsManagerActor::new(shards_manager, chunk_request_retry_period)
        });
    (shards_manager_addr, shards_manager_arbiter_handle)
}
