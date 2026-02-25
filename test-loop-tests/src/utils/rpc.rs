use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::future::BoxFuture;
use near_async::messaging::{IntoMultiSender, noop};
use near_async::test_loop::sender::TestLoopSender;
use near_chain_configs::GenesisConfig;
use near_client::gc_actor::GCActor;
use near_client_primitives::types::BlockNotificationMessage;
use near_jsonrpc::client::RpcTransport;
use near_jsonrpc::{PeerManagerSenderForRpc, RpcConfig, create_jsonrpc_app};
use near_jsonrpc_primitives::types::entity_debug::DummyEntityDebugHandler;
use tower_service::Service;

/// In-process transport that routes requests through an axum Router.
/// Used in TestLoop instead of standard network connections.
pub(crate) struct TestLoopRpcTransport {
    router: Router,
}

impl TestLoopRpcTransport {
    pub fn new(router: Router) -> Self {
        Self { router }
    }
}

impl RpcTransport for TestLoopRpcTransport {
    fn send_http_request(
        &self,
        endpoint: &str,
        body: Vec<u8>,
        response_size_limit: usize,
    ) -> BoxFuture<'static, Result<(StatusCode, Vec<u8>), String>> {
        let mut router = self.router.clone();
        let request = Request::builder()
            .method("POST")
            .uri(endpoint)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        Box::pin(async move {
            let response = router.call(request).await.map_err(|e| format!("{e}"))?;
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), response_size_limit)
                .await
                .map_err(|e| format!("failed to read response body: {e}"))?;
            Ok((status, bytes.to_vec()))
        })
    }
}

/// Creates an axum jsonrpc Router wired to testloop actors for a given node.
pub(crate) fn create_testloop_jsonrpc_router(
    clock: near_async::time::Clock,
    client_sender: &TestLoopSender<near_client::client_actor::ClientActor>,
    view_client_sender: &TestLoopSender<near_client::ViewClientActor>,
    rpc_handler_sender: &TestLoopSender<near_client::RpcHandlerActor>,
    #[allow(unused)] gc_actor_sender: &TestLoopSender<GCActor>, // used only when test_features is enabled.
    genesis_config: &GenesisConfig,
    block_notification_watcher: tokio::sync::watch::Receiver<Option<BlockNotificationMessage>>,
) -> Router {
    // TODO(rpc): figure out how to pass non-dummy values for these fields.
    // They are not needed for normal jsonrpc functionality, it's debugging/testing stuff.
    let peer_manager_sender: PeerManagerSenderForRpc = noop().into_multi_sender();
    let entity_debug_handler = Arc::new(DummyEntityDebugHandler {});

    create_jsonrpc_app(
        clock,
        RpcConfig::default(),
        genesis_config.clone(),
        client_sender.clone().into_multi_sender(),
        view_client_sender.clone().into_multi_sender(),
        rpc_handler_sender.clone().into_multi_sender(),
        peer_manager_sender,
        block_notification_watcher,
        #[cfg(feature = "test_features")]
        gc_actor_sender.clone().into_multi_sender(),
        entity_debug_handler,
        None,
        None,
    )
}
