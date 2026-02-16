use std::sync::Arc;

use axum::Router;
use axum_test::TestServer;
use near_async::ActorSystem;
use near_async::messaging::{IntoMultiSender, noop};
use near_async::test_loop::data::TestLoopData;
use near_async::time::Clock;
use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::ViewClientActor;
use near_client::adversarial::Controls;
use near_jsonrpc::RpcConfig;
use near_jsonrpc::create_jsonrpc_app;
use near_jsonrpc_primitives::types::entity_debug::DummyEntityDebugHandler;
use near_network::tcp;

use crate::setup::state::NodeExecutionData;

/// Sets up a JSON-RPC server backed by an external ViewClientActor that shares
/// the same Store as the TestLoop-managed node.
///
/// TestLoop's actors run on TestLoop's single-threaded event loop and cannot
/// receive messages from other threads (it panics). To work around this, we
/// create a *separate* ViewClientActor running on a real `ActorSystem`. This
/// external actor reads from the same underlying database as the TestLoop node,
/// so any blocks produced by TestLoop are immediately visible to HTTP queries.
pub(crate) struct TestLoopRpcServer {
    pub server_addr: String,
    actor_system: Option<ActorSystem>,
    _test_server: TestServer,
}

impl TestLoopRpcServer {
    pub fn new(
        test_loop_data: &TestLoopData,
        genesis: &Genesis,
        node_data: &NodeExecutionData,
    ) -> Self {
        let client_handle = node_data.client_sender.actor_handle();
        let client = &test_loop_data.get(&client_handle).client;

        // Create an external ViewClientActor on a real ActorSystem.
        let actor_system = ActorSystem::new();
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let view_client_actor = ViewClientActor::spawn_multithread_actor(
            Clock::real(),
            actor_system.clone(),
            chain_genesis,
            client.epoch_manager.clone(),
            client.shard_tracker.clone(),
            client.runtime_adapter.clone(),
            noop().into_multi_sender(),
            client.config.clone(),
            Controls::default(),
            client.validator_signer.clone(),
        );

        // Build the axum Router.
        let (_, block_notification_rx) = tokio::sync::watch::channel(None);
        let rpc_config = RpcConfig {
            addr: tcp::ListenerAddr::reserve_for_test(),
            prometheus_addr: None,
            cors_allowed_origins: vec!["*".to_string()],
            polling_config: Default::default(),
            limits_config: Default::default(),
            enable_debug_rpc: false,
            experimental_debug_pages_src_path: None,
        };
        let app: Router = create_jsonrpc_app(
            rpc_config,
            genesis.config.clone(),
            noop().into_multi_sender(),
            view_client_actor.into_multi_sender(),
            noop().into_multi_sender(),
            noop().into_multi_sender(),
            block_notification_rx,
            #[cfg(feature = "test_features")]
            noop().into_multi_sender(),
            Arc::new(DummyEntityDebugHandler {}),
        );

        // Create an in-memory TestServer.
        let test_server: TestServer =
            TestServer::builder().http_transport().build(app).expect("Failed to create TestServer");
        let server_addr = test_server.server_address().unwrap().to_string();

        Self { server_addr, actor_system: Some(actor_system), _test_server: test_server }
    }
}

impl Drop for TestLoopRpcServer {
    fn drop(&mut self) {
        if let Some(actor_system) = self.actor_system.take() {
            actor_system.stop();
        }
    }
}
