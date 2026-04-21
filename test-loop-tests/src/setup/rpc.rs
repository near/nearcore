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
use near_jsonrpc::sharded_rpc::ShardedRpcPool;
use near_jsonrpc::{PeerManagerSenderForRpc, RpcConfig, create_jsonrpc_app};
use near_jsonrpc_primitives::types::entity_debug::DummyEntityDebugHandler;
use parking_lot::RwLock;
use std::future::pending;
use std::sync::Arc;
use tower_service::Service;

/// Simulated fault mode for an RPC peer.
#[derive(Clone, Debug)]
pub(crate) enum RpcFault {
    /// Return `Err(msg)` immediately — simulates a node that's up but
    /// can't serve the query (e.g. stale, behind on sync).
    Fail(String),
    /// Never resolve — simulates a dead / unreachable node. The
    /// coordinator's per-peer timeout must fire to recover.
    Hang,
}

/// Shared handle used by tests to inject faults into a node's inbound RPC.
pub(crate) type RpcFaultHandle = Arc<RwLock<Option<RpcFault>>>;

/// RpcTransport decorator that injects failures based on a shared handle.
/// Wraps another transport so callers see a configurable error without
/// touching the destination node.
pub(crate) struct FaultyRpcTransport {
    inner: Arc<dyn RpcTransport>,
    fault: RpcFaultHandle,
}

impl FaultyRpcTransport {
    pub(crate) fn new(inner: Arc<dyn RpcTransport>, fault: RpcFaultHandle) -> Self {
        Self { inner, fault }
    }
}

impl RpcTransport for FaultyRpcTransport {
    fn send_http_request(
        &self,
        endpoint: &str,
        body: Vec<u8>,
        response_size_limit: usize,
        extra_headers: &[(&str, &str)],
    ) -> BoxFuture<'static, Result<(StatusCode, Vec<u8>), String>> {
        match self.fault.read().clone() {
            Some(RpcFault::Fail(msg)) => Box::pin(async move { Err(msg) }),
            Some(RpcFault::Hang) => Box::pin(pending()),
            None => {
                self.inner.send_http_request(endpoint, body, response_size_limit, extra_headers)
            }
        }
    }
}

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
        extra_headers: &[(&str, &str)],
    ) -> BoxFuture<'static, Result<(StatusCode, Vec<u8>), String>> {
        let mut router = self.router.clone();
        let mut builder = Request::builder()
            .method("POST")
            .uri(endpoint)
            .header("content-type", "application/json");
        for (key, value) in extra_headers {
            builder = builder.header(*key, *value);
        }
        let request = builder.body(Body::from(body)).unwrap();
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
    pool: Arc<RwLock<ShardedRpcPool>>,
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
        pool,
    )
}
