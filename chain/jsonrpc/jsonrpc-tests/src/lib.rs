use std::sync::Arc;

use axum::Router;
use axum_test::TestServer;
use near_async::ActorSystem;
use near_async::messaging::{IntoMultiSender, IntoSender, noop};
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, Genesis, MutableConfigValue, TrackedShardsConfig};
use near_client::adversarial::Controls;
use near_client::{RpcHandlerConfig, ViewClientActorInner, spawn_rpc_handler_actor, start_client};
use near_crypto::{KeyType, PublicKey};
use near_epoch_manager::{EpochManager, shard_tracker::ShardTracker};
use near_jsonrpc::{RpcConfig, create_jsonrpc_app};
use near_jsonrpc_primitives::types::entity_debug::DummyEntityDebugHandler;
use near_network::tcp;
use near_primitives::epoch_info::RngSeed;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, NumSeats};
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_time::Clock;
use nearcore::NightshadeRuntime;

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

    initialize_genesis_state(store.clone(), &genesis, None);

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);

    // 2. Create runtime
    let tempdir = tempfile::TempDir::new().expect("Failed to create temp directory");
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

    // 5. Create shared client config
    let client_config = ClientConfig::test(
        true, // skip_sync_wait
        100,  // min_block_prod_time
        200,  // max_block_prod_time
        num_validator_seats,
        false, // archive
        true,  // save_trie_changes
        true,  // state_sync_enabled
    );

    // 6. Create ViewClientActor
    let adv = Controls::default();
    let view_client_actor = ViewClientActorInner::spawn_multithread_actor(
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
    let client_result = start_client(
        Clock::real(),
        actor_system.clone(),
        client_config.clone(),
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        PeerId::new(PublicKey::empty(KeyType::ED25519)),
        actor_system.new_future_spawner().into(),
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
    );

    // 8. Create RpcHandlerActor
    let rpc_handler_config = RpcHandlerConfig {
        handler_threads: client_config.transaction_request_handler_threads,
        tx_routing_height_horizon: client_config.tx_routing_height_horizon,
        epoch_length: client_config.epoch_length,
        transaction_validity_period,
    };

    let rpc_handler_actor = spawn_rpc_handler_actor(
        actor_system.clone(),
        rpc_handler_config,
        client_result.tx_pool,
        client_result.chunk_endorsement_tracker,
        epoch_manager,
        shard_tracker,
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
    };

    let app = create_jsonrpc_app(
        rpc_config,
        genesis.config,
        client_result.client_actor.into_multi_sender(),
        view_client_actor.into_multi_sender(),
        rpc_handler_actor.into_multi_sender(),
        noop().into_multi_sender(),
        #[cfg(feature = "test_features")]
        noop().into_multi_sender(),
        Arc::new(DummyEntityDebugHandler {}),
    );

    // 10. Create TestServer with real HTTP transport to get an address
    let test_server = TestServer::builder()
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
