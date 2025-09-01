use std::sync::Arc;

use axum::Router;
use axum_test::TestServer;
use near_async::ActorSystem;
use near_async::messaging::{IntoMultiSender, IntoSender, noop};
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, Genesis, MutableConfigValue, TrackedShardsConfig};
use near_client::{RpcHandlerConfig, ViewClientActorInner, spawn_rpc_handler_actor, start_client};
use near_crypto::KeyType;
use near_epoch_manager::{EpochManager, shard_tracker::ShardTracker};
use near_jsonrpc::{RpcConfig, create_jsonrpc_app};
use near_jsonrpc_primitives::types::entity_debug::DummyEntityDebugHandler;
use near_network::tcp;
use near_network::types::PeerManagerAdapter;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, NumSeats};
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_time::Clock;
use nearcore::NightshadeRuntime;
use num_rational::Ratio;

/// Simplified test setup struct that only exposes what tests need
pub struct TestSetup {
    pub app: Router,
    pub server_addr: String,
    pub tempdir: Arc<tempfile::TempDir>,
    pub actor_system: ActorSystem,
    _test_server: TestServer, // Keep server alive but don't expose it
}

impl TestSetup {
    /// Clean up the test setup
    pub fn cleanup(self) {
        self.actor_system.stop();
        // tempdir is automatically cleaned up when dropped
    }
}

/// Create a minimal test setup with real Near actors but simplified infrastructure
///
/// This function creates:
/// - Real ClientActor, ViewClientActor, and RpcHandlerActor (no mocking)
/// - Axum Router for testing (no TCP binding)
/// - TestServer for in-memory HTTP testing
/// - Minimal dependencies (no complex network mocking)
pub fn create_test_setup() -> TestSetup {
    create_test_setup_with_account("test1".parse().unwrap())
}

/// Create test setup with a specific validator account
pub fn create_test_setup_with_account(validator_account: AccountId) -> TestSetup {
    // 1. Create foundation components
    let store = create_test_store();
    let validators = vec![validator_account.clone()];
    let num_validator_seats = validators.len() as NumSeats;

    // Create genesis with test accounts
    let mut genesis = Genesis::test(validators, num_validator_seats);
    genesis.config.epoch_length = 10; // Short epochs for faster tests

    initialize_genesis_state(store.clone(), &genesis, None);

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);

    // 2. Create runtime
    let tempdir = tempfile::TempDir::new().expect("Failed to create temp directory");
    let runtime =
        NightshadeRuntime::test(tempdir.path(), store, &genesis.config, epoch_manager.clone());

    // 3. Create chain genesis
    let chain_genesis = ChainGenesis {
        time: Clock::real().now_utc(),
        height: 0,
        gas_limit: 1_000_000,
        min_gas_price: 100,
        max_gas_price: 1_000_000_000,
        total_supply: 3_000_000_000_000_000_000_000_000_000_000_000,
        gas_price_adjustment_rate: Ratio::from_integer(0),
        transaction_validity_period: 100,
        epoch_length: 10,
        protocol_version: near_primitives::version::PROTOCOL_VERSION,
        chain_id: "unittest".to_string(),
    };

    // 4. Create actor system and signer
    let actor_system = ActorSystem::new();
    let signer = MutableConfigValue::new(
        Some(Arc::new(create_test_signer(validator_account.as_str()))),
        "validator_signer",
    );

    let shard_tracker =
        ShardTracker::new(TrackedShardsConfig::AllShards, epoch_manager.clone(), signer.clone());

    // 5. Create minimal network adapter (no-op for testing)
    let network_adapter: PeerManagerAdapter = noop().into_multi_sender();

    // 6. Create shared client config
    let client_config = ClientConfig::test(
        true,  // skip_sync_wait
        100,   // min_block_prod_time
        200,   // max_block_prod_time
        1,     // num_validator_seats
        false, // archive
        true,  // save_trie_changes
        true,  // state_sync_enabled
    );

    // 7. Create ViewClientActor
    let view_client_actor = ViewClientActorInner::spawn_actix_actor(
        Clock::real(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        network_adapter.clone(),
        client_config.clone(),
        near_client::adversarial::Controls::default(),
        signer.clone(),
    );

    // 8. Create ClientActor
    let client_result = start_client(
        Clock::real(),
        actor_system.clone(),
        client_config,
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        near_primitives::network::PeerId::new(near_crypto::PublicKey::empty(KeyType::ED25519)),
        Arc::new(near_async::actix::futures::ActixFutureSpawner),
        network_adapter.clone(),
        noop().into_sender(), // shards_manager_adapter
        signer.clone(),
        noop().into_sender(), // telemetry_sender
        None,                 // snapshot_callbacks
        None,                 // sender
        near_client::adversarial::Controls::default(),
        None, // config_updater
        near_client::PartialWitnessSenderForClient {
            distribute_chunk_state_witness: noop().into_sender(),
        },
        true,                       // enable_doomslug
        Some([3; 32]),              // seed
        noop().into_multi_sender(), // resharding_sender
    );

    // 9. Create RpcHandlerActor
    let rpc_handler_config = RpcHandlerConfig {
        handler_threads: 1, // Single thread for testing
        tx_routing_height_horizon: 1000,
        epoch_length: 10,
        transaction_validity_period: 100,
    };

    let rpc_handler_actor = spawn_rpc_handler_actor(
        rpc_handler_config,
        client_result.tx_pool,
        client_result.chunk_endorsement_tracker,
        epoch_manager,
        shard_tracker,
        signer,
        runtime,
        network_adapter,
    );

    // 10. Create Axum Router
    let rpc_config = RpcConfig {
        addr: tcp::ListenerAddr::reserve_for_test(), // Reserve a test address (won't be used)
        prometheus_addr: None,                       // No prometheus needed for testing
        cors_allowed_origins: vec!["*".to_string()],
        polling_config: Default::default(),
        limits_config: Default::default(),
        enable_debug_rpc: false,
        experimental_debug_pages_src_path: None,
    };

    let app = create_jsonrpc_app(
        rpc_config,
        genesis.config,
        client_result.client_actor.into_multi_sender(),
        view_client_actor.into_multi_sender(),
        rpc_handler_actor.into_multi_sender(),
        noop().into_multi_sender(), // peer_manager_sender
        #[cfg(feature = "test_features")]
        noop().into_multi_sender(), // gc_sender
        Arc::new(DummyEntityDebugHandler {}),
    );

    // 11. Create TestServer with real HTTP transport to get an address
    let test_server = TestServer::builder()
        .http_transport()
        .build(app.clone())
        .expect("Failed to create TestServer");
    let server_addr = test_server.server_address().unwrap().to_string();

    TestSetup {
        app,
        server_addr,
        tempdir: Arc::new(tempdir),
        actor_system,
        _test_server: test_server,
    }
}
