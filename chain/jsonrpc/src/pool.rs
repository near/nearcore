use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use near_chain_configs::TrackedShardsConfig;
use near_epoch_manager::EpochManagerAdapter;
use near_jsonrpc_client_internal::JsonRpcClient;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::message::Message;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::AccountId;
use near_primitives::types::ShardId;
use reqwest::Client;
use serde_json::Value;

use crate::metrics;

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Handle to a remote RPC node that can forward JSON-RPC requests.
///
/// `JsonRpcClient` implements this for production.
/// TestLoop can provide its own implementation.
pub trait RpcNodeHandle: Send + Sync {
    fn forward(&self, request: &Message) -> BoxFut<'_, Result<Value, RpcError>>;
}

impl RpcNodeHandle for JsonRpcClient {
    fn forward(&self, request: &Message) -> BoxFut<'_, Result<Value, RpcError>> {
        let request = request.clone();
        let transport = self.transport.clone();
        Box::pin(async move {
            let message = transport.send_jsonrpc_request(request).await?;

            match message {
                Message::Response(resp) => resp.result,
                _ => {
                    Err(RpcError::parse_error("Pool forward: unexpected response type".to_string()))
                }
            }
        })
    }
}

/// Routing decision for a JSON-RPC request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingDecision {
    /// Execute locally on this node.
    Local,
    /// Forward to a single shard's peer node.
    Forward(ShardUId),
    /// Fan out to all unique peers and merge results.
    FanOut,
}

/// Routes requests to local or remote nodes based on method and shard assignment.
pub struct RpcPool {
    /// Returns the shard layout for the current protocol version.
    /// Called per request so it reflects any runtime changes (e.g. resharding).
    get_shard_layout: Box<dyn Fn() -> ShardLayout + Send + Sync>,
    /// Which shards the local node tracks (used to decide local vs. remote).
    tracked_shards_config: TrackedShardsConfig,
    /// Remote peers keyed by shard_uid.
    // TODO(rpc-pool): account for peer tail
    peers: HashMap<ShardUId, Arc<dyn RpcNodeHandle>>,
    /// Deduplicated list of unique peers (for fan-out).
    /// Multiple shards may map to the same physical node.
    unique_peers: Vec<Arc<dyn RpcNodeHandle>>,
}

impl RpcPool {
    pub fn new(
        get_shard_layout: Box<dyn Fn() -> ShardLayout + Send + Sync>,
        tracked_shards_config: TrackedShardsConfig,
        peers: HashMap<ShardUId, Arc<dyn RpcNodeHandle>>,
        unique_peers: Vec<Arc<dyn RpcNodeHandle>>,
    ) -> Self {
        tracing::info!(
            target: "jsonrpc",
            tracked_shards_config = ?tracked_shards_config,
            peer_shards = ?peers.keys().collect::<Vec<_>>(),
            num_unique_peers = unique_peers.len(),
            "RPC pool initialized"
        );
        Self { get_shard_layout, tracked_shards_config, peers, unique_peers }
    }

    /// Determine how to route a request based on method name and params.
    pub fn route(&self, method: &str, params: &Value) -> RoutingDecision {
        match method {
            // --- Local-only methods ---
            "health"
            | "status"
            | "network_info"
            | "client_config"
            | "block"
            | "gas_price"
            | "validators"
            | "EXPERIMENTAL_validators_ordered"
            | "EXPERIMENTAL_protocol_config"
            | "genesis_config"
            | "EXPERIMENTAL_genesis_config"
            | "EXPERIMENTAL_split_storage_info"
            | "next_light_client_block"
            | "EXPERIMENTAL_light_client_block_proof"
            | "send_tx"
            | "broadcast_tx_async"
            | "broadcast_tx_commit"
            | "EXPERIMENTAL_receipt" => RoutingDecision::Local,

            // --- Forward by account_id (query-style) ---
            "query" => self.route_by_query_account_id(params),
            "EXPERIMENTAL_view_account"
            | "EXPERIMENTAL_view_code"
            | "EXPERIMENTAL_view_state"
            | "EXPERIMENTAL_view_access_key"
            | "EXPERIMENTAL_view_access_key_list"
            | "EXPERIMENTAL_call_function" => self.route_by_account_id_field(params),

            // --- Forward by signer account_id ---
            "tx" | "EXPERIMENTAL_tx_status" => self.route_by_tx_signer(params),

            // --- Forward by account_id ---
            "maintenance_windows" | "EXPERIMENTAL_maintenance_windows" => {
                self.route_by_account_id_field(params)
            }

            // --- Forward by sender/receiver account_id ---
            "light_client_proof" | "EXPERIMENTAL_light_client_proof" => {
                self.route_by_light_client_proof_account(params)
            }

            // --- Forward by shard_id ---
            "chunk" | "EXPERIMENTAL_congestion_level" => self.route_by_shard_id(params),

            // --- Fan-out (all shards, merge results) ---
            "block_effects"
            | "EXPERIMENTAL_changes_in_block"
            | "changes"
            | "EXPERIMENTAL_changes" => RoutingDecision::FanOut,

            // Unknown methods fall through to local (will get UNSUPPORTED_METHOD error)
            _ => RoutingDecision::Local,
        }
    }

    /// Forward a request to a specific shard's peer node.
    ///
    /// Returns `None` if the shard is local or no peer is configured (caller should execute locally).
    /// Returns `Some(result)` if the request was forwarded to a peer.
    pub async fn forward_to_shard(
        &self,
        shard_uid: ShardUId,
        request: &Message,
        method: &str,
    ) -> Option<Result<Value, RpcError>> {
        let shard_layout = (self.get_shard_layout)();
        if self.is_local_shard(&shard_layout, shard_uid) {
            return None;
        }
        let peer = self.peers.get(&shard_uid)?;
        let shard_id_str = shard_uid.to_string();
        metrics::RPC_POOL_FORWARD_TOTAL.with_label_values(&[method, &shard_id_str]).inc();
        let timer = std::time::Instant::now();
        let result = peer.forward(request).await;
        metrics::RPC_POOL_FORWARD_DURATION_SECONDS
            .with_label_values(&[method])
            .observe(timer.elapsed().as_secs_f64());
        if result.is_err() {
            metrics::RPC_POOL_FORWARD_ERRORS_TOTAL
                .with_label_values(&[method, &shard_id_str])
                .inc();
        }
        Some(result)
    }

    /// Forward a request to all unique peers concurrently.
    ///
    /// Returns a Vec of results (one per unique peer). Errors for individual peers
    /// are returned as-is; the caller is responsible for merging and handling partial failures.
    pub async fn forward_to_all(
        &self,
        request: &Message,
        _method: &str,
    ) -> Vec<Result<Value, RpcError>> {
        let mut handles = Vec::with_capacity(self.unique_peers.len());
        for peer in &self.unique_peers {
            let req = request.clone();
            let peer = Arc::clone(peer);
            handles.push(tokio::spawn(async move { peer.forward(&req).await }));
        }
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(RpcError::new_internal_error(
                    None,
                    format!("Pool fan-out task failed: {}", e),
                ))),
            }
        }
        results
    }

    /// Whether this pool has any unique peers (used to decide if fan-out is useful).
    pub fn has_peers(&self) -> bool {
        !self.unique_peers.is_empty()
    }

    /// Resolve account_id to a RoutingDecision.
    fn resolve_shard_for_account(&self, account_id: &str) -> RoutingDecision {
        let Ok(account_id) = account_id.parse::<AccountId>() else {
            return RoutingDecision::Local;
        };
        let shard_layout = (self.get_shard_layout)();
        let shard_uid = shard_layout.account_id_to_shard_uid(&account_id);
        RoutingDecision::Forward(shard_uid)
    }

    /// Route `query` method by extracting account_id from query params.
    ///
    /// Supports both named params (object with `request_type` + `account_id`)
    /// and the structured `QueryRequest` format.
    fn route_by_query_account_id(&self, params: &Value) -> RoutingDecision {
        // Named params format: {"request_type": "view_account", "account_id": "alice.near", ...}
        if let Some(account_id) = params.get("account_id").and_then(|v| v.as_str()) {
            return self.resolve_shard_for_account(account_id);
        }
        // Legacy array format: ["account/alice.near"] — extract from path
        if let Some(arr) = params.as_array() {
            if let Some(path) = arr.first().and_then(|v| v.as_str()) {
                // Path format: "command/account_id/..."
                let parts: Vec<&str> = path.splitn(3, '/').collect();
                if parts.len() >= 2 {
                    return self.resolve_shard_for_account(parts[1]);
                }
            }
        }
        RoutingDecision::Local
    }

    /// Route by top-level `account_id` field in params.
    fn route_by_account_id_field(&self, params: &Value) -> RoutingDecision {
        if let Some(account_id) = params.get("account_id").and_then(|v| v.as_str()) {
            return self.resolve_shard_for_account(account_id);
        }
        RoutingDecision::Local
    }

    /// Route tx/EXPERIMENTAL_tx_status by signer account_id.
    ///
    /// Supports:
    /// - Object with `sender_account_id` field
    /// - Array format: [tx_hash, sender_account_id]
    fn route_by_tx_signer(&self, params: &Value) -> RoutingDecision {
        // Object format: {"tx_hash": ..., "sender_account_id": "alice.near"}
        if let Some(account_id) = params.get("sender_account_id").and_then(|v| v.as_str()) {
            return self.resolve_shard_for_account(account_id);
        }
        // Array format: [tx_hash_or_signed_tx, sender_account_id]
        if let Some(arr) = params.as_array() {
            if arr.len() >= 2 {
                if let Some(account_id) = arr[1].as_str() {
                    return self.resolve_shard_for_account(account_id);
                }
            }
        }
        RoutingDecision::Local
    }

    /// Route light_client_proof by sender_id or receiver_id.
    fn route_by_light_client_proof_account(&self, params: &Value) -> RoutingDecision {
        // TransactionOrReceiptId::Transaction has sender_id
        if let Some(account_id) = params.get("sender_id").and_then(|v| v.as_str()) {
            return self.resolve_shard_for_account(account_id);
        }
        // TransactionOrReceiptId::Receipt has receiver_id
        if let Some(account_id) = params.get("receiver_id").and_then(|v| v.as_str()) {
            return self.resolve_shard_for_account(account_id);
        }
        RoutingDecision::Local
    }

    /// Route by shard_id field in params (for chunk, congestion_level).
    fn route_by_shard_id(&self, params: &Value) -> RoutingDecision {
        let shard_id_val = if let Some(v) = params.get("shard_id") {
            v
        } else if let Some(arr) = params.as_array() {
            // Array format: [[block_id, shard_id]]
            if let Some(inner) = arr.first().and_then(|v| v.as_array()) {
                if inner.len() >= 2 {
                    &inner[1]
                } else {
                    return RoutingDecision::Local;
                }
            } else {
                return RoutingDecision::Local;
            }
        } else {
            return RoutingDecision::Local;
        };

        let Some(n) = shard_id_val.as_u64() else {
            return RoutingDecision::Local;
        };

        let shard_id = ShardId::new(n);
        let shard_layout = (self.get_shard_layout)();
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        RoutingDecision::Forward(shard_uid)
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

/// Build an RpcPool from configuration.
pub fn build_pool(
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    tracked_shards_config: TrackedShardsConfig,
    peers_config: HashMap<ShardUId, String>,
    forward_timeout: Duration,
) -> RpcPool {
    let get_shard_layout = Box::new(move || {
        epoch_manager
            .get_static_shard_layout_for_protocol_version(
                near_primitives::version::PROTOCOL_VERSION,
            )
            .expect("shard layout must exist for current protocol version")
    });

    let http_client = Client::builder()
        .timeout(forward_timeout)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .build()
        .expect("Failed to create pool HTTP client");

    // Deduplicate peers by address: multiple shards may map to the same physical node.
    let mut nodes_by_addr: HashMap<String, Arc<dyn RpcNodeHandle>> = HashMap::new();
    let mut peers: HashMap<ShardUId, Arc<dyn RpcNodeHandle>> = HashMap::new();

    for (shard_uid, addr) in peers_config {
        let node = nodes_by_addr
            .entry(addr.clone())
            .or_insert_with(|| Arc::new(JsonRpcClient::new(&addr, http_client.clone())));
        peers.insert(shard_uid, Arc::clone(node));
    }

    let unique_peers: Vec<Arc<dyn RpcNodeHandle>> = nodes_by_addr.into_values().collect();

    RpcPool::new(get_shard_layout, tracked_shards_config, peers, unique_peers)
}

#[cfg(test)]
mod tests {
    use super::*;
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
        fn forward(&self, _request: &Message) -> BoxFut<'_, Result<Value, RpcError>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(serde_json::json!({
                    "node": self.name,
                }))
            })
        }
    }

    fn make_test_pool(
        shard_layout: ShardLayout,
        tracked_shards_config: TrackedShardsConfig,
        peers: HashMap<ShardUId, Arc<dyn RpcNodeHandle>>,
        unique_peers: Vec<Arc<dyn RpcNodeHandle>>,
    ) -> RpcPool {
        let get_shard_layout = Box::new(move || shard_layout.clone());
        RpcPool::new(get_shard_layout, tracked_shards_config, peers, unique_peers)
    }

    // --- Routing decision tests ---

    #[test]
    fn test_route_local_methods() {
        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            HashMap::new(),
            vec![],
        );

        let null = Value::Null;
        for method in &[
            "health",
            "status",
            "network_info",
            "client_config",
            "block",
            "gas_price",
            "validators",
            "EXPERIMENTAL_validators_ordered",
            "EXPERIMENTAL_protocol_config",
            "genesis_config",
            "EXPERIMENTAL_genesis_config",
            "EXPERIMENTAL_split_storage_info",
            "next_light_client_block",
            "EXPERIMENTAL_light_client_block_proof",
            "send_tx",
            "broadcast_tx_async",
            "broadcast_tx_commit",
            "EXPERIMENTAL_receipt",
        ] {
            assert_eq!(pool.route(method, &null), RoutingDecision::Local, "method: {}", method);
        }
    }

    #[test]
    fn test_route_forward_by_account_query() {
        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            HashMap::new(),
            vec![],
        );

        // Named params format
        let params = serde_json::json!({
            "request_type": "view_account",
            "account_id": "alice.near",
            "finality": "final"
        });
        match pool.route("query", &params) {
            RoutingDecision::Forward(_) => {}
            other => panic!("Expected Forward, got {:?}", other),
        }

        // Legacy array format
        let params = serde_json::json!(["account/alice.near"]);
        match pool.route("query", &params) {
            RoutingDecision::Forward(_) => {}
            other => panic!("Expected Forward, got {:?}", other),
        }

        // No account_id -> Local
        let params = serde_json::json!({});
        assert_eq!(pool.route("query", &params), RoutingDecision::Local);
    }

    #[test]
    fn test_route_forward_by_account_experimental() {
        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            HashMap::new(),
            vec![],
        );

        let params = serde_json::json!({"account_id": "alice.near"});
        for method in &[
            "EXPERIMENTAL_view_account",
            "EXPERIMENTAL_view_code",
            "EXPERIMENTAL_view_state",
            "EXPERIMENTAL_view_access_key",
            "EXPERIMENTAL_view_access_key_list",
            "EXPERIMENTAL_call_function",
            "maintenance_windows",
            "EXPERIMENTAL_maintenance_windows",
        ] {
            match pool.route(method, &params) {
                RoutingDecision::Forward(_) => {}
                other => panic!("Expected Forward for {}, got {:?}", method, other),
            }
        }
    }

    #[test]
    fn test_route_forward_by_tx_signer() {
        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            HashMap::new(),
            vec![],
        );

        // Object format
        let params = serde_json::json!({"tx_hash": "abc", "sender_account_id": "alice.near"});
        match pool.route("tx", &params) {
            RoutingDecision::Forward(_) => {}
            other => panic!("Expected Forward, got {:?}", other),
        }

        // Array format
        let params = serde_json::json!(["abc", "alice.near"]);
        match pool.route("EXPERIMENTAL_tx_status", &params) {
            RoutingDecision::Forward(_) => {}
            other => panic!("Expected Forward, got {:?}", other),
        }

        // No signer -> Local
        let params = serde_json::json!({"signed_tx_base64": "abc"});
        assert_eq!(pool.route("tx", &params), RoutingDecision::Local);
    }

    #[test]
    fn test_route_forward_by_shard_id() {
        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            HashMap::new(),
            vec![],
        );

        // Object format with shard_id
        let params = serde_json::json!({"block_id": 123, "shard_id": 0});
        match pool.route("chunk", &params) {
            RoutingDecision::Forward(_) => {}
            other => panic!("Expected Forward, got {:?}", other),
        }

        // No shard_id (chunk_hash format) -> Local
        let params = serde_json::json!({"chunk_id": "abc"});
        assert_eq!(pool.route("chunk", &params), RoutingDecision::Local);
    }

    #[test]
    fn test_route_fanout() {
        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            HashMap::new(),
            vec![],
        );

        let null = Value::Null;
        for method in
            &["block_effects", "EXPERIMENTAL_changes_in_block", "changes", "EXPERIMENTAL_changes"]
        {
            assert_eq!(pool.route(method, &null), RoutingDecision::FanOut, "method: {}", method);
        }
    }

    #[test]
    fn test_route_light_client_proof() {
        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            HashMap::new(),
            vec![],
        );

        // Transaction variant (sender_id)
        let params = serde_json::json!({
            "type": "transaction",
            "transaction_hash": "abc",
            "sender_id": "alice.near",
            "light_client_head": "def"
        });
        match pool.route("light_client_proof", &params) {
            RoutingDecision::Forward(_) => {}
            other => panic!("Expected Forward, got {:?}", other),
        }

        // Receipt variant (receiver_id)
        let params = serde_json::json!({
            "type": "receipt",
            "receipt_id": "abc",
            "receiver_id": "bob.near",
            "light_client_head": "def"
        });
        match pool.route("EXPERIMENTAL_light_client_proof", &params) {
            RoutingDecision::Forward(_) => {}
            other => panic!("Expected Forward, got {:?}", other),
        }
    }

    // --- Forward/fan-out execution tests ---

    #[tokio::test]
    async fn test_forward_to_shard_remote() {
        let shard_layout = ShardLayout::single_shard();
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let remote = Arc::new(MockNode::new("remote"));
        let remote_ref = Arc::clone(&remote);

        let mut peers: HashMap<ShardUId, Arc<dyn RpcNodeHandle>> = HashMap::new();
        peers.insert(shard_uid, remote);

        let pool = make_test_pool(shard_layout, TrackedShardsConfig::NoShards, peers, vec![]);

        let msg = Message::request("query".to_string(), Value::Null);
        let result = pool.forward_to_shard(shard_uid, &msg, "query").await;

        assert!(result.is_some());
        assert_eq!(result.unwrap().unwrap()["node"], "remote");
        assert_eq!(remote_ref.calls(), 1);
    }

    #[tokio::test]
    async fn test_forward_to_shard_local_returns_none() {
        let shard_layout = ShardLayout::single_shard();
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let remote = Arc::new(MockNode::new("remote"));
        let mut peers: HashMap<ShardUId, Arc<dyn RpcNodeHandle>> = HashMap::new();
        peers.insert(shard_uid, remote);

        // AllShards => shard is local, forward returns None
        let pool = make_test_pool(shard_layout, TrackedShardsConfig::AllShards, peers, vec![]);

        let msg = Message::request("query".to_string(), Value::Null);
        let result = pool.forward_to_shard(shard_uid, &msg, "query").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_forward_to_all_dedup() {
        let peer_a = Arc::new(MockNode::new("peer_a"));
        let peer_b = Arc::new(MockNode::new("peer_b"));
        let peer_a_ref = Arc::clone(&peer_a);
        let peer_b_ref = Arc::clone(&peer_b);

        let unique_peers: Vec<Arc<dyn RpcNodeHandle>> = vec![peer_a, peer_b];

        let pool = make_test_pool(
            ShardLayout::single_shard(),
            TrackedShardsConfig::AllShards,
            HashMap::new(),
            unique_peers,
        );

        let msg = Message::request("changes".to_string(), Value::Null);
        let results = pool.forward_to_all(&msg, "changes").await;

        assert_eq!(results.len(), 2);
        assert_eq!(peer_a_ref.calls(), 1);
        assert_eq!(peer_b_ref.calls(), 1);
    }

    #[tokio::test]
    async fn test_forward_to_shard_fallback_no_peer() {
        let shard_layout = ShardLayout::single_shard();
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        // No peers configured
        let pool =
            make_test_pool(shard_layout, TrackedShardsConfig::NoShards, HashMap::new(), vec![]);

        let msg = Message::request("query".to_string(), Value::Null);
        let result = pool.forward_to_shard(shard_uid, &msg, "query").await;

        // No peer -> None, caller should execute locally
        assert!(result.is_none());
    }
}
