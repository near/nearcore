// FIXME(nagisa): Is there a good reason we're triggering this? Luckily though this is just test
// code so we're in the clear.
#![allow(clippy::arc_with_non_send_sync)]

use actix::{Actor, Addr, Context};
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::actix_wrapper::{ActixWrapper, spawn_actix_actor};
use near_async::futures::ActixFutureSpawner;
use near_async::messaging::{
    IntoMultiSender, IntoSender, LateBoundSender, SendAsync, Sender, noop,
};
use near_async::time::{Clock, Duration, Utc};
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::resharding::resharding_actor::ReshardingActor;
use near_chain::resharding::types::ReshardingSender;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::types::{ChainConfig, RuntimeAdapter};
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::{
    ChunkDistributionNetworkConfig, ClientConfig, Genesis, MutableConfigValue,
    MutableValidatorSigner, ReshardingConfig, ReshardingHandle, TrackedShardsConfig,
};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::shards_manager_actor::{ShardsManagerActor, start_shards_manager};
use near_chunks::test_utils::SynchronousShardsManagerAdapter;
use near_client::adversarial::Controls;
use near_client::{
    AsyncComputationMultiSpawner, Client, ClientActor, PartialWitnessActor,
    PartialWitnessSenderForClient, RpcHandler, RpcHandlerConfig, StartClientResult, SyncStatus,
    ViewClientActor, ViewClientActorInner, start_client,
};
use near_client::{RpcHandlerActor, spawn_rpc_handler_actor};
use near_crypto::{KeyType, PublicKey};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_network::client::ChunkEndorsementMessage;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_witness::PartialWitnessSenderForNetwork;
use near_network::types::{NetworkRequests, NetworkResponses, PeerManagerAdapter};
use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse};
use near_o11y::WithSpanContextExt;
use near_primitives::epoch_info::RngSeed;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, BlockHeightDelta, NumBlocks, NumSeats};
use near_primitives::validator_signer::EmptyValidatorSigner;
use near_primitives::version::{PROTOCOL_VERSION, get_protocol_upgrade_schedule};
use near_store::adapter::StoreAdapter;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_telemetry::TelemetryActor;
use nearcore::NightshadeRuntime;
use num_rational::Ratio;
use std::sync::Arc;

use crate::utils::peer_manager_mock::PeerManagerMock;

pub const TEST_SEED: RngSeed = [3; 32];

/// min block production time in milliseconds
pub const MIN_BLOCK_PROD_TIME: Duration = Duration::milliseconds(100);
/// max block production time in milliseconds
pub const MAX_BLOCK_PROD_TIME: Duration = Duration::milliseconds(200);

/// Sets up ClientActor and ViewClientActor viewing the same store/runtime.
fn setup(
    clock: Clock,
    validators: Vec<AccountId>,
    epoch_length: BlockHeightDelta,
    account_id: AccountId,
    skip_sync_wait: bool,
    min_block_prod_time: u64,
    max_block_prod_time: u64,
    enable_doomslug: bool,
    archive: bool,
    state_sync_enabled: bool,
    network_adapter: PeerManagerAdapter,
    transaction_validity_period: NumBlocks,
    genesis_time: Utc,
    chunk_distribution_config: Option<ChunkDistributionNetworkConfig>,
) -> (
    Addr<ClientActor>,
    Addr<ViewClientActor>,
    Addr<RpcHandlerActor>,
    ShardsManagerAdapterForTest,
    PartialWitnessSenderForNetwork,
    tempfile::TempDir,
) {
    let store = create_test_store();

    let num_validator_seats = validators.len() as NumSeats;

    let mut validators = validators;

    // Certain tests depend on these accounts existing so we make them available here.
    // This is mostly due to historical reasons - those tests used to use heavily mocked testing
    // environment that didn't check account existence.
    for account in ["test2", "test"].into_iter().map(|acc| acc.parse().unwrap()) {
        if !validators.contains(&account) {
            validators.push(account);
        }
    }

    let mut genesis = Genesis::test(validators, num_validator_seats);
    genesis.config.epoch_length = epoch_length;
    initialize_genesis_state(store.clone(), &genesis, None);

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);

    let tempdir = tempfile::TempDir::new().unwrap();

    let runtime = NightshadeRuntime::test(
        tempdir.path(),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );

    let chain_genesis = ChainGenesis {
        time: genesis_time,
        height: 0,
        gas_limit: 1_000_000,
        min_gas_price: 100,
        max_gas_price: 1_000_000_000,
        total_supply: 3_000_000_000_000_000_000_000_000_000_000_000,
        gas_price_adjustment_rate: Ratio::from_integer(0),
        transaction_validity_period,
        epoch_length,
        protocol_version: PROTOCOL_VERSION,
        chain_id: "integration_test".to_string(),
    };

    let signer = MutableConfigValue::new(
        Some(Arc::new(create_test_signer(account_id.as_str()))),
        "validator_signer",
    );
    let shard_tracker =
        ShardTracker::new(TrackedShardsConfig::AllShards, epoch_manager.clone(), signer.clone());
    let telemetry = ActixWrapper::new(TelemetryActor::default()).start();
    let config = {
        let mut base = ClientConfig::test(
            skip_sync_wait,
            min_block_prod_time,
            max_block_prod_time,
            num_validator_seats,
            archive,
            true,
            state_sync_enabled,
        );
        base.chunk_distribution_network = chunk_distribution_config;
        base
    };

    let adv = Controls::default();

    let view_client_addr = ViewClientActorInner::spawn_actix_actor(
        clock.clone(),
        signer.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        network_adapter.clone(),
        config.clone(),
        adv.clone(),
    );

    let client_adapter_for_partial_witness_actor = LateBoundSender::new();
    let (partial_witness_addr, _) = spawn_actix_actor(PartialWitnessActor::new(
        clock.clone(),
        network_adapter.clone(),
        client_adapter_for_partial_witness_actor.as_multi_sender(),
        signer.clone(),
        epoch_manager.clone(),
        runtime.clone(),
        Arc::new(RayonAsyncComputationSpawner),
        Arc::new(RayonAsyncComputationSpawner),
    ));
    let partial_witness_adapter = partial_witness_addr.with_auto_span_context();

    let (resharding_sender_addr, _) = spawn_actix_actor(ReshardingActor::new(
        epoch_manager.clone(),
        runtime.clone(),
        ReshardingHandle::new(),
        config.resharding_config.clone(),
    ));
    let resharding_sender = resharding_sender_addr.with_auto_span_context();

    let shards_manager_adapter_for_client = LateBoundSender::new();
    let StartClientResult { client_actor, tx_pool, chunk_endorsement_tracker, .. } = start_client(
        clock,
        config.clone(),
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        PeerId::new(PublicKey::empty(KeyType::ED25519)),
        Arc::new(ActixFutureSpawner),
        network_adapter.clone(),
        shards_manager_adapter_for_client.as_sender(),
        signer.clone(),
        telemetry.with_auto_span_context().into_sender(),
        None,
        None,
        adv,
        None,
        partial_witness_adapter.clone().into_multi_sender(),
        enable_doomslug,
        Some(TEST_SEED),
        resharding_sender.into_multi_sender(),
    );

    let rpc_handler_config = RpcHandlerConfig {
        handler_threads: config.transaction_request_handler_threads,
        tx_routing_height_horizon: config.tx_routing_height_horizon,
        epoch_length: config.epoch_length,
        transaction_validity_period,
    };

    let rpc_handler_addr = spawn_rpc_handler_actor(
        rpc_handler_config,
        tx_pool,
        chunk_endorsement_tracker,
        epoch_manager.clone(),
        shard_tracker.clone(),
        signer,
        runtime,
        network_adapter.clone(),
    );

    let validator_signer = Some(Arc::new(EmptyValidatorSigner::new(account_id)));
    let (shards_manager_addr, _) = start_shards_manager(
        epoch_manager.clone(),
        epoch_manager,
        shard_tracker,
        network_adapter.into_sender(),
        client_actor.clone().with_auto_span_context().into_sender(),
        MutableConfigValue::new(validator_signer, "validator_signer"),
        store,
        config.chunk_request_retry_period,
    );
    let shards_manager_adapter = shards_manager_addr.with_auto_span_context();
    shards_manager_adapter_for_client.bind(shards_manager_adapter.clone());

    client_adapter_for_partial_witness_actor.bind(client_actor.clone().with_auto_span_context());

    (
        client_actor,
        view_client_addr,
        rpc_handler_addr,
        shards_manager_adapter.into_multi_sender(),
        partial_witness_adapter.into_multi_sender(),
        tempdir,
    )
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock(
    clock: Clock,
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
    peer_manager_mock: Box<
        dyn FnMut(
            &PeerManagerMessageRequest,
            &mut Context<PeerManagerMock>,
            Addr<ClientActor>,
            Addr<RpcHandlerActor>,
        ) -> PeerManagerMessageResponse,
    >,
) -> ActorHandlesForTesting {
    setup_mock_with_validity_period(
        clock,
        validators,
        account_id,
        skip_sync_wait,
        enable_doomslug,
        peer_manager_mock,
        100,
    )
}

pub fn setup_mock_with_validity_period(
    clock: Clock,
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
    mut peermanager_mock: Box<
        dyn FnMut(
            &PeerManagerMessageRequest,
            &mut Context<PeerManagerMock>,
            Addr<ClientActor>,
            Addr<RpcHandlerActor>,
        ) -> PeerManagerMessageResponse,
    >,
    transaction_validity_period: NumBlocks,
) -> ActorHandlesForTesting {
    let network_adapter = LateBoundSender::new();
    let (
        client_addr,
        view_client_addr,
        rpc_handler_addr,
        shards_manager_adapter,
        partial_witness_sender,
        runtime_tempdir,
    ) = setup(
        clock.clone(),
        validators,
        10,
        account_id,
        skip_sync_wait,
        MIN_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        MAX_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        enable_doomslug,
        false,
        true,
        network_adapter.as_multi_sender(),
        transaction_validity_period,
        clock.now_utc(),
        None,
    );
    let client_addr1 = client_addr.clone();
    let rpc_handler_addr1 = rpc_handler_addr.clone();

    let network_actor = PeerManagerMock::new(move |msg, ctx| {
        peermanager_mock(&msg, ctx, client_addr1.clone(), rpc_handler_addr1.clone())
    })
    .start();

    network_adapter.bind(network_actor);

    ActorHandlesForTesting {
        client_actor: client_addr,
        view_client_actor: view_client_addr,
        rpc_handler_actor: rpc_handler_addr,
        shards_manager_adapter,
        partial_witness_sender,
        runtime_tempdir: Some(runtime_tempdir.into()),
    }
}

#[derive(Clone)]
pub struct ActorHandlesForTesting {
    pub client_actor: Addr<ClientActor>,
    pub view_client_actor: Addr<ViewClientActor>,
    pub rpc_handler_actor: Addr<RpcHandlerActor>,
    pub shards_manager_adapter: ShardsManagerAdapterForTest,
    pub partial_witness_sender: PartialWitnessSenderForNetwork,
    // If testing something with runtime that needs runtime home dir users should make sure that
    // this TempDir isn't dropped before test finishes, but is dropped after to avoid leaking temp
    // dirs.
    pub runtime_tempdir: Option<Arc<tempfile::TempDir>>,
}

/// Sets up ClientActor and ViewClientActor without network.
pub fn setup_no_network(
    clock: Clock,
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
) -> ActorHandlesForTesting {
    setup_no_network_with_validity_period(
        clock,
        validators,
        account_id,
        skip_sync_wait,
        100,
        enable_doomslug,
    )
}

pub fn setup_no_network_with_validity_period(
    clock: Clock,
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> ActorHandlesForTesting {
    let my_account_id = account_id.clone();
    setup_mock_with_validity_period(
        clock,
        validators,
        account_id,
        skip_sync_wait,
        enable_doomslug,
        Box::new(move |request, _, _client, rpc_handler| {
            // Handle network layer sending messages to self
            match request {
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::ChunkEndorsement(
                    account_id,
                    endorsement,
                )) => {
                    if account_id == &my_account_id {
                        let future = rpc_handler.send_async(
                            ChunkEndorsementMessage(endorsement.clone()).with_span_context(),
                        );
                        // Don't ignore the future or else the message may not actually be handled.
                        actix::spawn(future);
                    }
                }
                _ => {}
            };
            PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
        }),
        transaction_validity_period,
    )
}

pub fn setup_client_with_runtime(
    clock: Clock,
    num_validator_seats: NumSeats,
    enable_doomslug: bool,
    network_adapter: PeerManagerAdapter,
    shards_manager_adapter: SynchronousShardsManagerAdapter,
    chain_genesis: ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    rng_seed: RngSeed,
    archive: bool,
    save_trie_changes: bool,
    snapshot_callbacks: Option<SnapshotCallbacks>,
    partial_witness_adapter: PartialWitnessSenderForClient,
    validator_signer: MutableValidatorSigner,
    resharding_sender: ReshardingSender,
) -> Client {
    let mut config =
        ClientConfig::test(true, 10, 20, num_validator_seats, archive, save_trie_changes, true);
    config.epoch_length = chain_genesis.epoch_length;
    let protocol_upgrade_schedule = get_protocol_upgrade_schedule(&chain_genesis.chain_id);
    let multi_spawner = AsyncComputationMultiSpawner::default()
        .custom_apply_chunks(Arc::new(RayonAsyncComputationSpawner)); // Use rayon instead of the default thread pool 
    let mut client = Client::new(
        clock,
        config,
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        network_adapter,
        shards_manager_adapter.into_sender(),
        validator_signer,
        enable_doomslug,
        rng_seed,
        snapshot_callbacks,
        multi_spawner,
        partial_witness_adapter,
        resharding_sender,
        Arc::new(ActixFutureSpawner),
        noop().into_multi_sender(), // state sync ignored for these tests
        noop().into_multi_sender(), // apply chunks ping not necessary for these tests
        protocol_upgrade_schedule,
    )
    .unwrap();
    client.sync_handler.sync_status = SyncStatus::NoSync;
    client
}

pub fn setup_synchronous_shards_manager(
    clock: Clock,
    account_id: Option<AccountId>,
    client_adapter: Sender<ShardsManagerResponse>,
    network_adapter: PeerManagerAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_genesis: &ChainGenesis,
) -> SynchronousShardsManagerAdapter {
    // Initialize the chain, to make sure that if the store is empty, we write the genesis
    // into the store, and as a short cut to get the parameters needed to instantiate
    // ShardsManager. This way we don't have to wait to construct the Client first.
    // TODO(#8324): This should just be refactored so that we can construct Chain first
    // before anything else.
    let chunk_store = runtime.store().chunk_store();
    let chain = Chain::new(
        clock.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime,
        chain_genesis,
        DoomslugThresholdMode::TwoThirds, // irrelevant
        ChainConfig {
            save_trie_changes: true,
            background_migration_threads: 1,
            resharding_config: MutableConfigValue::new(
                ReshardingConfig::default(),
                "resharding_config",
            ),
        }, // irrelevant
        None,
        Default::default(),
        MutableConfigValue::new(None, "validator_signer"),
        noop().into_multi_sender(),
    )
    .unwrap();
    let chain_head = chain.head().unwrap();
    let chain_header_head = chain.header_head().unwrap();
    let validator_signer = account_id.map(|id| Arc::new(EmptyValidatorSigner::new(id)));
    let shards_manager = ShardsManagerActor::new(
        clock,
        MutableConfigValue::new(validator_signer, "validator_signer"),
        epoch_manager.clone(),
        epoch_manager,
        shard_tracker,
        network_adapter.request_sender,
        client_adapter,
        chunk_store,
        chain_head,
        chain_header_head,
        Duration::hours(1),
    );
    SynchronousShardsManagerAdapter::new(shards_manager)
}

pub fn setup_tx_request_handler(
    chain_genesis: ChainGenesis,
    client: &Client,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    network_adapter: PeerManagerAdapter,
) -> RpcHandler {
    let client_config = ClientConfig::test(true, 10, 20, 0, true, true, true);
    let config = RpcHandlerConfig {
        handler_threads: 1,
        tx_routing_height_horizon: client_config.tx_routing_height_horizon,
        epoch_length: chain_genesis.epoch_length,
        transaction_validity_period: chain_genesis.transaction_validity_period,
    };

    RpcHandler::new(
        config,
        client.chunk_producer.sharded_tx_pool.clone(),
        client.chunk_endorsement_tracker.clone(),
        epoch_manager,
        shard_tracker,
        client.validator_signer.clone(),
        runtime,
        network_adapter,
    )
}

/// A multi-sender for both the client and network parts of the ShardsManager API.
#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ShardsManagerAdapterForTest {
    pub client: Sender<ShardsManagerRequestFromClient>,
    pub network: Sender<ShardsManagerRequestFromNetwork>,
}
