//! Client actor orchestrates Client and facilitates network connection.
//! It should just serve as a coordinator class to handle messages and check triggers but immediately
//! pass the control to Client. This means, any real block processing or production logic should
//! be put in Client.
//! Unfortunately, this is not the case today. We are in the process of refactoring ClientActor
//! https://github.com/near/nearcore/issues/7899

use actix::{Actor, Addr, AsyncContext, Context, Handler};
use actix_rt::{Arbiter, ArbiterHandle};
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::futures::ActixArbiterHandleFutureSpawner;
use near_async::messaging::{IntoMultiSender, IntoSender, Sender};
use near_async::time::Utc;
use near_async::time::{Clock, Duration};
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::types::RuntimeAdapter;
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, ReshardingHandle};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_client_primitives::types::Error;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManagerAdapter, RngSeed};
use near_network::types::PeerManagerAdapter;
use near_o11y::{handler_debug_span, WithSpanContext};
use near_primitives::network::PeerId;
use near_primitives::validator_signer::ValidatorSigner;
use near_telemetry::TelemetryActor;
use rand::Rng;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

use crate::client_actions::{ClientActionHandler, ClientActions, ClientSenderForClient};
use crate::start_gc_actor;
use crate::sync_jobs_actions::SyncJobsActions;
use crate::sync_jobs_actor::SyncJobsActor;
use crate::{metrics, Client, ConfigUpdater, SyncAdapter};

pub struct ClientActor {
    actions: ClientActions,
}

impl Deref for ClientActor {
    type Target = ClientActions;
    fn deref(&self) -> &ClientActions {
        &self.actions
    }
}

impl DerefMut for ClientActor {
    fn deref_mut(&mut self) -> &mut ClientActions {
        &mut self.actions
    }
}

impl ClientActor {
    pub fn new(
        clock: Clock,
        client: Client,
        myself_sender: ClientSenderForClient,
        config: ClientConfig,
        node_id: PeerId,
        network_adapter: PeerManagerAdapter,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        telemetry_actor: Addr<TelemetryActor>,
        ctx: &Context<ClientActor>,
        shutdown_signal: Option<broadcast::Sender<()>>,
        adv: crate::adversarial::Controls,
        config_updater: Option<ConfigUpdater>,
    ) -> Result<Self, Error> {
        let state_parts_arbiter = Arbiter::new();
        let self_addr = ctx.address();
        let self_addr_clone = self_addr;
        let sync_jobs_actor_addr = SyncJobsActor::start_in_arbiter(
            &state_parts_arbiter.handle(),
            move |ctx: &mut Context<SyncJobsActor>| -> SyncJobsActor {
                ctx.set_mailbox_capacity(SyncJobsActor::MAILBOX_CAPACITY);
                SyncJobsActor {
                    actions: SyncJobsActions::new(
                        self_addr_clone.with_auto_span_context().into_multi_sender(),
                        ctx.address().with_auto_span_context().into_multi_sender(),
                    ),
                }
            },
        );
        let actions = ClientActions::new(
            clock,
            client,
            myself_sender,
            config,
            node_id,
            network_adapter,
            validator_signer,
            telemetry_actor.with_auto_span_context().into_sender(),
            shutdown_signal,
            adv,
            config_updater,
            sync_jobs_actor_addr.with_auto_span_context().into_multi_sender(),
            Box::new(ActixArbiterHandleFutureSpawner(state_parts_arbiter.handle())),
        )?;
        Ok(Self { actions })
    }

    // NOTE: Do not add any more functionality to ClientActor. Add to ClientActions instead.
}

impl Actor for ClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.actions.start(ctx);
    }
}

impl ClientActor {
    /// Wrapper for processing actix message which must be called after receiving it.
    ///
    /// Due to a bug in Actix library, while there are messages in mailbox, Actix
    /// will prioritize processing messages until mailbox is empty. In such case execution
    /// of any other task scheduled with `run_later` will be delayed. At the same time,
    /// we have several important functions which have to be called regularly, so we put
    /// these calls into `check_triggers` and call it here as a quick hack.
    fn wrap<Req: std::fmt::Debug + actix::Message, Res>(
        &mut self,
        msg: WithSpanContext<Req>,
        ctx: &mut Context<Self>,
        msg_type: &str,
        f: impl FnOnce(&mut Self, Req, &mut Context<Self>) -> Res,
    ) -> Res {
        let (_span, msg) = handler_debug_span!(target: "client", msg, msg_type);
        self.actions.check_triggers(ctx);
        let _span_inner = tracing::debug_span!(target: "client", "NetworkClientMessage").entered();
        metrics::CLIENT_MESSAGES_COUNT.with_label_values(&[msg_type]).inc();
        let timer =
            metrics::CLIENT_MESSAGES_PROCESSING_TIME.with_label_values(&[msg_type]).start_timer();
        let res = f(self, msg, ctx);
        timer.observe_duration();
        res
    }
}

impl<T> Handler<WithSpanContext<T>> for ClientActor
where
    T: actix::Message + Debug,
    ClientActions: ClientActionHandler<T, Result = T::Result>,
    T::Result: actix::dev::MessageResponse<ClientActor, WithSpanContext<T>>,
{
    type Result = T::Result;

    fn handle(&mut self, msg: WithSpanContext<T>, ctx: &mut Context<Self>) -> Self::Result {
        self.wrap(msg, ctx, std::any::type_name::<T>(), |this, msg, _| this.actions.handle(msg))
    }
}

/// Returns random seed sampled from the current thread
pub fn random_seed_from_thread() -> RngSeed {
    let mut rng_seed: RngSeed = [0; 32];
    rand::thread_rng().fill(&mut rng_seed);
    rng_seed
}

/// Blocks the program until given genesis time arrives.
fn wait_until_genesis(genesis_time: &Utc) {
    loop {
        let duration = *genesis_time - Clock::real().now_utc();
        if duration <= Duration::ZERO {
            break;
        }
        tracing::info!(target: "near", "Waiting until genesis: {}d {}h {}m {}s",
              duration.whole_days(),
              (duration.whole_hours() % 24),
              (duration.whole_minutes() % 60),
              (duration.whole_seconds() % 60));
        let wait = duration.min(Duration::seconds(10)).unsigned_abs();
        std::thread::sleep(wait);
    }
}

pub struct StartClientResult {
    pub client_actor: Addr<ClientActor>,
    pub client_arbiter_handle: ArbiterHandle,
    pub resharding_handle: ReshardingHandle,
    pub gc_arbiter_handle: ArbiterHandle,
}

/// Starts client in a separate Arbiter (thread).
pub fn start_client(
    clock: Clock,
    client_config: ClientConfig,
    chain_genesis: ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    node_id: PeerId,
    state_sync_adapter: Arc<RwLock<SyncAdapter>>,
    network_adapter: PeerManagerAdapter,
    shards_manager_adapter: Sender<ShardsManagerRequestFromClient>,
    validator_signer: Option<Arc<dyn ValidatorSigner>>,
    telemetry_actor: Addr<TelemetryActor>,
    snapshot_callbacks: Option<SnapshotCallbacks>,
    sender: Option<broadcast::Sender<()>>,
    adv: crate::adversarial::Controls,
    config_updater: Option<ConfigUpdater>,
) -> StartClientResult {
    let client_arbiter = Arbiter::new();
    let client_arbiter_handle = client_arbiter.handle();
    let genesis_height = chain_genesis.height;

    wait_until_genesis(&chain_genesis.time);
    let client = Client::new(
        clock.clone(),
        client_config.clone(),
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker,
        state_sync_adapter,
        runtime.clone(),
        network_adapter.clone(),
        shards_manager_adapter,
        validator_signer.clone(),
        true,
        random_seed_from_thread(),
        snapshot_callbacks,
        Arc::new(RayonAsyncComputationSpawner),
    )
    .unwrap();
    let resharding_handle = client.chain.resharding_handle.clone();

    let (_, gc_arbiter_handle) = start_gc_actor(
        runtime.store().clone(),
        genesis_height,
        client_config.clone(),
        runtime,
        epoch_manager,
    );

    let client_addr = ClientActor::start_in_arbiter(&client_arbiter_handle, move |ctx| {
        ClientActor::new(
            clock,
            client,
            ctx.address().with_auto_span_context().into_multi_sender(),
            client_config,
            node_id,
            network_adapter,
            validator_signer,
            telemetry_actor,
            ctx,
            sender,
            adv,
            config_updater,
        )
        .unwrap()
    });

    StartClientResult {
        client_actor: client_addr,
        client_arbiter_handle,
        resharding_handle,
        gc_arbiter_handle,
    }
}
