use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use near_async::messaging::CanSendAsync;
use near_chain_configs::TrackedShardsConfig;
use near_client::Query as ClientQuery;
use near_epoch_manager::EpochManagerAdapter;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::message::{Message, from_slice};
use near_jsonrpc_primitives::types::query::{RpcQueryRequest, RpcQueryResponse};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::AccountId;
use near_primitives::views::QueryRequest;
use reqwest::Client;
use serde_json::Value;

use crate::ViewClientSenderForRpc;
use crate::api::{RpcFrom, RpcInto};
use crate::metrics;
use crate::process_query_response;

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Handle to an RPC node that can execute queries.
///
/// `LocalRpcNode` and `RemoteRpcNode` implement this for production.
/// TestLoop can provide its own implementation.
pub trait RpcNodeHandle: Send + Sync {
    fn query(&self, request: RpcQueryRequest) -> BoxFut<'_, Result<Value, RpcError>>;
}

/// Local node handle that wraps the ViewClient sender.
pub struct LocalRpcNode {
    view_client_sender: ViewClientSenderForRpc,
}

impl LocalRpcNode {
    pub fn new(view_client_sender: ViewClientSenderForRpc) -> Self {
        Self { view_client_sender }
    }
}

impl RpcNodeHandle for LocalRpcNode {
    fn query(&self, request: RpcQueryRequest) -> BoxFut<'_, Result<Value, RpcError>> {
        Box::pin(async move {
            let result: Result<RpcQueryResponse, _> = self
                .view_client_sender
                .send_async(ClientQuery::new(request.block_reference, request.request))
                .await
                .map_err(RpcFrom::rpc_from)
                .and_then(|r| r.map_err(RpcFrom::rpc_from))
                .map(RpcInto::rpc_into);
            process_query_response(result)
        })
    }
}

/// Remote node handle that forwards queries via HTTP.
pub struct RemoteRpcNode {
    client: Client,
    addr: String,
}

impl RemoteRpcNode {
    pub fn new(client: Client, addr: String) -> Self {
        Self { client, addr }
    }
}

impl RpcNodeHandle for RemoteRpcNode {
    fn query(&self, request: RpcQueryRequest) -> BoxFut<'_, Result<Value, RpcError>> {
        Box::pin(async move {
            let rpc_request = Message::request(
                "query".to_string(),
                serde_json::to_value(&request)
                    .map_err(|e| RpcError::serialization_error(e.to_string()))?,
            );

            let response = self
                .client
                .post(&self.addr)
                .header("Content-Type", "application/json")
                .header("X-Near-Pool-Forwarded", "true")
                .json(&rpc_request)
                .send()
                .await
                .map_err(|err| {
                    RpcError::new_internal_error(None, format!("Pool forward error: {}", err))
                })?;

            let bytes = response.bytes().await.map_err(|err| {
                RpcError::new_internal_error(None, format!("Pool forward payload error: {}", err))
            })?;

            let message = from_slice(&bytes).map_err(|_| {
                RpcError::parse_error("Pool forward: invalid JSON-RPC response".to_string())
            })?;

            match message {
                Message::Response(resp) => resp.result,
                _ => {
                    Err(RpcError::parse_error("Pool forward: unexpected response type".to_string()))
                }
            }
        })
    }
}

/// Routes queries to local or remote nodes based on shard assignment.
pub struct RpcPool {
    /// Returns the shard layout for the current protocol version.
    /// Called per request so it reflects any runtime changes (e.g. resharding).
    get_shard_layout: Box<dyn Fn() -> ShardLayout + Send + Sync>,
    /// Which shards the local node tracks (used to decide local vs. remote).
    tracked_shards_config: TrackedShardsConfig,
    /// Local node handle (ViewClient).
    local: Box<dyn RpcNodeHandle>,
    /// Remote peers keyed by shard_uid.
    // TODO(rpc-pool): account for peer tail
    peers: HashMap<ShardUId, Box<dyn RpcNodeHandle>>,
}

impl RpcPool {
    pub fn new(
        get_shard_layout: Box<dyn Fn() -> ShardLayout + Send + Sync>,
        tracked_shards_config: TrackedShardsConfig,
        local: Box<dyn RpcNodeHandle>,
        peers: HashMap<ShardUId, Box<dyn RpcNodeHandle>>,
    ) -> Self {
        tracing::info!(
            target: "jsonrpc",
            tracked_shards_config = ?tracked_shards_config,
            peer_shards = ?peers.keys().collect::<Vec<_>>(),
            "RPC pool initialized"
        );
        Self { get_shard_layout, tracked_shards_config, local, peers }
    }

    /// Route a query to the appropriate node.
    ///
    /// Proactive routing, local-first:
    /// 1. Extract account_id from the query request
    /// 2. Compute shard_uid via shard_layout (fetched dynamically)
    /// 3. If local node tracks that shard -> route locally
    /// 4. If a peer is configured for that shard -> route to peer
    /// 5. Otherwise -> route locally as fallback
    pub async fn query(&self, request: RpcQueryRequest) -> Result<Value, RpcError> {
        let shard_layout = (self.get_shard_layout)();

        if let Some(account_id) = extract_account_id(&request.request) {
            let shard_id = shard_layout.account_id_to_shard_id(account_id);
            let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);

            if !self.is_local_shard(&shard_layout, shard_uid) {
                if let Some(peer) = self.peers.get(&shard_uid) {
                    let shard_id_str = shard_uid.to_string();
                    metrics::RPC_POOL_FORWARD_TOTAL
                        .with_label_values(&["query", &shard_id_str])
                        .inc();
                    let timer = std::time::Instant::now();
                    let result = peer.query(request).await;
                    metrics::RPC_POOL_FORWARD_DURATION_SECONDS
                        .with_label_values(&["query"])
                        .observe(timer.elapsed().as_secs_f64());
                    if result.is_err() {
                        metrics::RPC_POOL_FORWARD_ERRORS_TOTAL
                            .with_label_values(&["query", &shard_id_str])
                            .inc();
                    }
                    return result;
                }
            }
        }

        // Route to local
        self.local.query(request).await
    }

    /// Whether the given shard is tracked locally.
    fn is_local_shard(&self, shard_layout: &ShardLayout, shard_uid: ShardUId) -> bool {
        match &self.tracked_shards_config {
            TrackedShardsConfig::AllShards => true,
            TrackedShardsConfig::NoShards => false,
            TrackedShardsConfig::Shards(shards) => shards.contains(&shard_uid),
            TrackedShardsConfig::Accounts(accounts) => accounts.iter().any(|a| {
                let sid = shard_layout.account_id_to_shard_id(a);
                ShardUId::from_shard_id_and_layout(sid, shard_layout) == shard_uid
            }),
            // Schedule, ShadowValidator: conservatively treat all as local.
            _ => true,
        }
    }
}

/// Extract account_id from a QueryRequest, if present.
fn extract_account_id(request: &QueryRequest) -> Option<&AccountId> {
    match request {
        QueryRequest::ViewAccount { account_id } => Some(account_id),
        QueryRequest::ViewCode { account_id } => Some(account_id),
        QueryRequest::ViewState { account_id, .. } => Some(account_id),
        QueryRequest::ViewAccessKey { account_id, .. } => Some(account_id),
        QueryRequest::ViewAccessKeyList { account_id } => Some(account_id),
        QueryRequest::ViewGasKeyNonces { account_id, .. } => Some(account_id),
        QueryRequest::CallFunction { account_id, .. } => Some(account_id),
        QueryRequest::ViewGlobalContractCodeByAccountId { account_id } => Some(account_id),
        // ViewGlobalContractCode uses code_hash, no account_id
        QueryRequest::ViewGlobalContractCode { .. } => None,
    }
}

/// Build an RpcPool from configuration.
pub fn build_pool(
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    tracked_shards_config: TrackedShardsConfig,
    view_client_sender: ViewClientSenderForRpc,
    peers_config: HashMap<ShardUId, String>,
    forward_timeout: Duration,
) -> RpcPool {
    let get_shard_layout = Box::new(move || {
        epoch_manager
            .get_shard_layout_from_protocol_version(near_primitives::version::PROTOCOL_VERSION)
    });

    let http_client = Client::builder()
        .timeout(forward_timeout)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .build()
        .expect("Failed to create pool HTTP client");

    let local = Box::new(LocalRpcNode::new(view_client_sender));

    let peers: HashMap<ShardUId, Box<dyn RpcNodeHandle>> = peers_config
        .into_iter()
        .map(|(shard_uid, addr)| {
            let node: Box<dyn RpcNodeHandle> =
                Box::new(RemoteRpcNode::new(http_client.clone(), addr));
            (shard_uid, node)
        })
        .collect();

    RpcPool::new(get_shard_layout, tracked_shards_config, local, peers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::types::BlockReference;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock RpcNodeHandle for testing routing logic.
    struct MockNode {
        name: String,
        call_count: AtomicUsize,
    }

    impl MockNode {
        fn new(name: &str) -> Self {
            Self { name: name.to_string(), call_count: AtomicUsize::new(0) }
        }

        fn calls(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    impl RpcNodeHandle for MockNode {
        fn query(&self, _request: RpcQueryRequest) -> BoxFut<'_, Result<Value, RpcError>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(serde_json::json!({
                    "node": self.name,
                }))
            })
        }
    }

    fn make_query_request(account_id: &str) -> RpcQueryRequest {
        RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewAccount { account_id: account_id.parse().unwrap() },
        }
    }

    fn make_test_pool(
        shard_layout: ShardLayout,
        tracked_shards_config: TrackedShardsConfig,
        local: Box<dyn RpcNodeHandle>,
        peers: HashMap<ShardUId, Box<dyn RpcNodeHandle>>,
    ) -> RpcPool {
        let get_shard_layout = Box::new(move || shard_layout.clone());
        RpcPool::new(get_shard_layout, tracked_shards_config, local, peers)
    }

    #[tokio::test]
    async fn test_local_first_routing() {
        let local = Box::new(MockNode::new("local"));
        let local_ref = &*local as *const MockNode;

        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            local,
            HashMap::new(),
        );

        // All queries should route locally when local tracks all shards
        let _ = pool.query(make_query_request("alice.near")).await;

        unsafe {
            assert_eq!((*local_ref).calls(), 1);
        }
    }

    #[tokio::test]
    async fn test_remote_routing() {
        let shard_layout = ShardLayout::single_shard();

        let local = Box::new(MockNode::new("local"));
        let local_ref = &*local as *const MockNode;

        let remote = Box::new(MockNode::new("remote"));
        let remote_ref = &*remote as *const MockNode;

        // Map the single shard to the remote node
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let mut peers: HashMap<ShardUId, Box<dyn RpcNodeHandle>> = HashMap::new();
        peers.insert(shard_uid, remote);

        let pool = make_test_pool(shard_layout, TrackedShardsConfig::NoShards, local, peers);

        let result = pool.query(make_query_request("alice.near")).await.unwrap();
        assert_eq!(result["node"], "remote");

        unsafe {
            assert_eq!((*local_ref).calls(), 0);
            assert_eq!((*remote_ref).calls(), 1);
        }
    }

    #[tokio::test]
    async fn test_fallback_to_local() {
        let local = Box::new(MockNode::new("local"));
        let local_ref = &*local as *const MockNode;

        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::NoShards,
            local,
            HashMap::new(),
        );

        // Should fallback to local when no peer is configured
        let _ = pool.query(make_query_request("alice.near")).await;

        unsafe {
            assert_eq!((*local_ref).calls(), 1);
        }
    }

    #[tokio::test]
    async fn test_no_account_id_routes_local() {
        let shard_layout = ShardLayout::single_shard();

        let local = Box::new(MockNode::new("local"));
        let local_ref = &*local as *const MockNode;

        let remote = Box::new(MockNode::new("remote"));
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let mut peers: HashMap<ShardUId, Box<dyn RpcNodeHandle>> = HashMap::new();
        peers.insert(shard_uid, remote);

        let pool = make_test_pool(shard_layout, TrackedShardsConfig::NoShards, local, peers);

        // ViewGlobalContractCode has no account_id -> routes to local
        let request = RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewGlobalContractCode { code_hash: Default::default() },
        };
        let _ = pool.query(request).await;

        unsafe {
            assert_eq!((*local_ref).calls(), 1);
        }
    }
}
