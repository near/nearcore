use axum::Router;
use axum_test::TestServer;
use near_async::ActorSystem;
use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender, noop};
use near_chain::spice::chunk_application::ChunkPersistenceConfig;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::SpiceCoreWriterActor;
use near_chain::types::RuntimeAdapter;
use near_chain::{ApplyChunksSpawner, ChainGenesis};
use near_chain_configs::test_utils::TestClientConfigParams;
use near_chain_configs::{ClientConfig, Genesis, MutableConfigValue, TrackedShardsConfig};
use near_client::adversarial::Controls;
use near_client::client_actor::SpiceClientConfig;
use near_client::spice::chunk_executor_actor::ChunkExecutorActor;
use near_client::spice::chunk_validator_actor::SpiceChunkValidatorActor;
use near_client::spice::data_distributor_actor::SpiceDataDistributorActor;
use near_client::{RpcHandlerConfig, ViewClientActor, spawn_rpc_handler_actor, start_client};
use near_crypto::{KeyType, PublicKey};
use near_epoch_manager::{EpochManager, shard_tracker::ShardTracker};
use near_jsonrpc::sharded_rpc::ShardedRpcPool;
use near_jsonrpc::{RpcConfig, create_jsonrpc_app};
use near_jsonrpc_primitives::types::entity_debug::DummyEntityDebugHandler;
use near_network::tcp;
use near_primitives::epoch_info::RngSeed;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, NumSeats};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::adapter::StoreAdapter as _;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_time::Clock;
use nearcore::NightshadeRuntime;
use parking_lot::RwLock;
use std::sync::Arc;

pub const TEST_SEED: RngSeed = [3; 32];

pub enum NodeType {
    Validator,
    NonValidator,
}

/// Simplified test setup struct that only exposes what tests need
pub struct TestSetup {
    pub app: Router,
    pub server_addr: String,
    pub tempdir: Arc<tempfile::TempDir>,
    pub actor_system: Option<ActorSystem>,
    _test_server: TestServer, // Keep server alive but don't expose it
}

impl Drop for TestSetup {
    fn drop(&mut self) {
        self.actor_system.take().unwrap().stop();
    }
}

/// Create a minimal test setup with real Near actors but simplified infrastructure
///
/// This function creates:
/// - Real ClientActor, ViewClientActor, and RpcHandlerActor (no mocking)
/// - Axum Router for testing (no TCP binding)
/// - TestServer for in-memory HTTP testing
/// - Minimal dependencies (no complex network mocking)
///
/// # Parameters
/// * `node_type` - Whether to create a validator or non-validator node
pub fn create_test_setup_with_node_type(node_type: NodeType) -> TestSetup {
    let validator_account: AccountId = "test1".parse().unwrap();
    let current_account = match node_type {
        NodeType::Validator => validator_account.clone(),
        NodeType::NonValidator => "other".parse().unwrap(),
    };
    let all_accounts =
        ["test1", "test", "test2"].iter().map(|s| s.parse().unwrap()).collect::<Vec<AccountId>>();
    create_test_setup_with_accounts_and_validity(
        all_accounts,
        validator_account,
        current_account,
        100,
    )
}

/// Create test setup with multiple accounts and custom transaction validity period
pub fn create_test_setup_with_accounts_and_validity(
    all_accounts: Vec<AccountId>,
    validator_account: AccountId,
    current_account: AccountId,
    transaction_validity_period: u64,
) -> TestSetup {
    // 1. Create foundation components
    let store = create_test_store();
    let validators = [validator_account];
    let num_validator_seats = validators.len() as NumSeats;

    // Create genesis with all specified accounts
    let mut genesis = Genesis::test(all_accounts, num_validator_seats);
    genesis.config.epoch_length = 10; // Short epochs for faster tests
    genesis.config.transaction_validity_period = 10 * 2;

    initialize_genesis_state(store.clone(), &genesis, None);

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);

    // 2. Create runtime
    let tempdir = tempfile::TempDir::new().expect("Failed to create temp directory");
    let chain_store = store.chain_store();
    let runtime =
        NightshadeRuntime::test(tempdir.path(), store, &genesis.config, epoch_manager.clone());

    // 3. Create chain genesis with custom transaction validity period
    let chain_genesis = ChainGenesis::new(&genesis.config);

    // 4. Create actor system and signer
    let actor_system = ActorSystem::new();
    let signer = MutableConfigValue::new(
        Some(Arc::new(create_test_signer(current_account.as_str()))),
        "validator_signer",
    );

    let shard_tracker =
        ShardTracker::new(TrackedShardsConfig::AllShards, epoch_manager.clone(), signer.clone());

    let (block_notification_watch_sender, block_notification_watch_receiver) =
        tokio::sync::watch::channel(None);

    // 5. Create shared client config
    let client_config = ClientConfig::test(TestClientConfigParams {
        skip_sync_wait: true,
        min_block_prod_time: 100,
        max_block_prod_time: 200,
        num_block_producer_seats: num_validator_seats,
        archive: false,
        transaction_pool_size_limit: None,
    });

    // 6. Create ViewClientActor
    let adv = Controls::default();
    let view_client_actor = ViewClientActor::spawn_multithread_actor(
        Clock::real(),
        actor_system.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        noop().into_multi_sender(),
        client_config.clone(),
        adv.clone(),
        signer.clone(),
    );

    // 7. Create ClientActor
    let chunk_executor_adapter = LateBoundSender::new();
    let spice_chunk_validator_adapter = LateBoundSender::new();
    let spice_data_distributor_adapter = LateBoundSender::new();
    let spice_core_writer_adapter = LateBoundSender::new();
    let spice_client_config = if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        SpiceClientConfig {
            chunk_executor_sender: chunk_executor_adapter.as_sender(),
            spice_chunk_validator_sender: spice_chunk_validator_adapter.as_sender(),
            spice_data_distributor_sender: spice_data_distributor_adapter.as_sender(),
            spice_core_writer_sender: spice_core_writer_adapter.as_sender(),
        }
    } else {
        SpiceClientConfig {
            chunk_executor_sender: noop().into_sender(),
            spice_chunk_validator_sender: noop().into_sender(),
            spice_data_distributor_sender: noop().into_sender(),
            spice_core_writer_sender: noop().into_sender(),
        }
    };
    let client_result = start_client(
        Clock::real(),
        actor_system.clone(),
        client_config.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        PeerId::new(PublicKey::empty(KeyType::ED25519)),
        actor_system.new_future_spawner("state sync").into(),
        noop().into_multi_sender(),
        noop().into_sender(),
        signer.clone(),
        noop().into_sender(),
        None,
        None,
        adv,
        None,
        noop().into_multi_sender(),
        true,
        Some(TEST_SEED),
        noop().into_multi_sender(),
        block_notification_watch_sender,
        spice_client_config,
    );

    if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        let spice_core_reader = SpiceCoreReader::new(
            runtime.store().chain_store(),
            epoch_manager.clone(),
            chain_genesis.gas_limit,
        );
        let spice_core_writer_actor = SpiceCoreWriterActor::new(
            runtime.store().chain_store(),
            epoch_manager.clone(),
            spice_core_reader.clone(),
            chunk_executor_adapter.as_sender(),
            spice_chunk_validator_adapter.as_sender(),
        );
        let spice_core_writer_addr = actor_system.spawn_tokio_actor(spice_core_writer_actor);
        spice_core_writer_adapter.bind(spice_core_writer_addr);

        let spice_data_distributor_actor = SpiceDataDistributorActor::new(
            epoch_manager.clone(),
            runtime.store().chain_store(),
            signer.clone(),
            shard_tracker.clone(),
            spice_core_reader,
            noop().into_multi_sender(),
            chunk_executor_adapter.as_sender(),
            spice_chunk_validator_adapter.as_sender(),
            spice_chunk_validator_adapter.as_sender(),
            spice_chunk_validator_adapter.as_sender(),
        );
        let spice_data_distributor_addr =
            actor_system.spawn_tokio_actor(spice_data_distributor_actor);
        spice_data_distributor_adapter.bind(spice_data_distributor_addr);

        let chunk_executor_actor = ChunkExecutorActor::new(
            runtime.store().clone(),
            &chain_genesis,
            runtime.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            noop().into_multi_sender(),
            signer.clone(),
            {
                let thread_limit = runtime.get_shard_limit(PROTOCOL_VERSION) as usize * 3;
                ApplyChunksSpawner::default().into_spawner(thread_limit)
            },
            chunk_executor_adapter.as_sender(),
            spice_core_writer_adapter.as_sender(),
            spice_data_distributor_adapter.as_multi_sender(),
            ChunkPersistenceConfig::default(),
        );
        let chunk_executor_addr = actor_system.spawn_tokio_actor(chunk_executor_actor);
        chunk_executor_adapter.bind(chunk_executor_addr);

        let spice_chunk_validator_actor = SpiceChunkValidatorActor::new(
            runtime.store().clone(),
            &chain_genesis,
            runtime.clone(),
            epoch_manager.clone(),
            noop().into_multi_sender(),
            signer.clone(),
            spice_core_writer_adapter.as_sender(),
            ApplyChunksSpawner::default(),
        );
        let spice_chunk_validator_addr =
            actor_system.spawn_tokio_actor(spice_chunk_validator_actor);
        spice_chunk_validator_adapter.bind(spice_chunk_validator_addr);
    }

    // 8. Create RpcHandlerActor
    let rpc_handler_config = RpcHandlerConfig {
        handler_threads: client_config.transaction_request_handler_threads,
        tx_routing_height_horizon: client_config.tx_routing_height_horizon,
        epoch_length: client_config.epoch_length,
        transaction_validity_period,
        disable_tx_routing: client_config.disable_tx_routing,
        spice_pending_transaction_queue_enabled: client_config
            .spice_pending_transaction_queue_enabled(),
    };

    let rpc_handler_actor = spawn_rpc_handler_actor(
        actor_system.clone(),
        rpc_handler_config,
        client_result.tx_pool,
        client_result.pending_transaction_queue,
        epoch_manager,
        shard_tracker.clone(),
        signer,
        runtime,
        noop().into_multi_sender(),
    );

    // 9. Create Axum Router
    let rpc_config = RpcConfig {
        addr: tcp::ListenerAddr::reserve_for_test(), // Reserve a test address (won't be used)
        prometheus_addr: None,                       // No prometheus needed for testing
        cors_allowed_origins: vec!["*".to_string()],
        polling_config: Default::default(),
        limits_config: Default::default(),
        enable_debug_rpc: false,
        experimental_debug_pages_src_path: None,
        sharded_rpc: None,
    };

    let pool = Arc::new(RwLock::new(ShardedRpcPool::new(
        rpc_config.sharded_rpc.clone(),
        shard_tracker,
        chain_store,
    )));

    let app = create_jsonrpc_app(
        Clock::real(),
        rpc_config,
        genesis.config,
        client_result.client_actor.into_multi_sender(),
        view_client_actor.into_multi_sender(),
        rpc_handler_actor.into_multi_sender(),
        noop().into_multi_sender(),
        block_notification_watch_receiver,
        #[cfg(feature = "test_features")]
        noop().into_multi_sender(),
        Arc::new(DummyEntityDebugHandler {}),
        pool,
    );

    // 10. Create TestServer with real HTTP transport to get an address
    let test_server: TestServer = TestServer::builder()
        .http_transport()
        .build(app.clone())
        .expect("Failed to create TestServer");
    let server_addr = test_server.server_address().unwrap().to_string();

    TestSetup {
        app,
        server_addr,
        tempdir: Arc::new(tempdir),
        actor_system: Some(actor_system),
        _test_server: test_server,
    }
}
