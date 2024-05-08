//! Client actor orchestrates Client and facilitates network connection.
//! It should just serve as a coordinator class to handle messages and check triggers but immediately
//! pass the control to Client. This means, any real block processing or production logic should
//! be put in Client.
//! Unfortunately, this is not the case today. We are in the process of refactoring ClientActor
//! https://github.com/near/nearcore/issues/7899

use actix::{Actor, Addr};
use actix_rt::{Arbiter, ArbiterHandle};
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::actix_wrapper::ActixWrapper;
use near_async::futures::ActixArbiterHandleFutureSpawner;
use near_async::messaging::{IntoMultiSender, LateBoundSender, Sender};
use near_async::time::Utc;
use near_async::time::{Clock, Duration};
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::types::RuntimeAdapter;
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, ReshardingHandle};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManagerAdapter, RngSeed};
use near_network::types::PeerManagerAdapter;
use near_primitives::network::PeerId;
use near_primitives::validator_signer::ValidatorSigner;
use near_telemetry::TelemetryEvent;
use rand::Rng;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

use crate::client_actions::{ClientActorInner, ClientSenderForClient};
use crate::stateless_validation::partial_witness::partial_witness_actor::PartialWitnessSenderForClient;
use crate::sync_jobs_actor::{ClientSenderForSyncJobs, SyncJobsActor};
use crate::{Client, ConfigUpdater, SyncAdapter};

pub type ClientActor = ActixWrapper<ClientActorInner>;

/// Returns random seed sampled from the current thread
fn random_seed_from_thread() -> RngSeed {
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
    telemetry_sender: Sender<TelemetryEvent>,
    snapshot_callbacks: Option<SnapshotCallbacks>,
    sender: Option<broadcast::Sender<()>>,
    adv: crate::adversarial::Controls,
    config_updater: Option<ConfigUpdater>,
    partial_witness_adapter: PartialWitnessSenderForClient,
    enable_doomslug: bool,
    seed: Option<RngSeed>,
) -> StartClientResult {
    let client_arbiter = Arbiter::new();
    let client_arbiter_handle = client_arbiter.handle();

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
        enable_doomslug,
        seed.unwrap_or_else(random_seed_from_thread),
        snapshot_callbacks,
        Arc::new(RayonAsyncComputationSpawner),
        partial_witness_adapter,
    )
    .unwrap();
    let resharding_handle = client.chain.resharding_handle.clone();

    let client_sender_for_sync_jobs = LateBoundSender::<ClientSenderForSyncJobs>::new();
    let sync_jobs_actor = SyncJobsActor::new(client_sender_for_sync_jobs.as_multi_sender());
    let (sync_jobs_actor_addr, sync_jobs_arbiter) = sync_jobs_actor.spawn_actix_actor();

    let client_sender_for_client = LateBoundSender::<ClientSenderForClient>::new();
    let client_sender_for_client_clone = client_sender_for_client.clone();
    let client_addr = ClientActor::start_in_arbiter(&client_arbiter_handle, move |_| {
        let client_actor_inner = ClientActorInner::new(
            clock,
            client,
            client_sender_for_client_clone.as_multi_sender(),
            client_config,
            node_id,
            network_adapter,
            validator_signer,
            telemetry_sender,
            sender,
            adv,
            config_updater,
            sync_jobs_actor_addr.with_auto_span_context().into_multi_sender(),
            Box::new(ActixArbiterHandleFutureSpawner(sync_jobs_arbiter)),
        )
        .unwrap();
        ActixWrapper::new(client_actor_inner)
    });

    client_sender_for_sync_jobs
        .bind(client_addr.clone().with_auto_span_context().into_multi_sender());
    client_sender_for_client.bind(client_addr.clone().with_auto_span_context().into_multi_sender());

    StartClientResult { client_actor: client_addr, client_arbiter_handle, resharding_handle }
}
