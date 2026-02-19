use futures::FutureExt;
use futures::future::BoxFuture;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::message::Message;
use near_jsonrpc_primitives::types::changes::{
    RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockByTypeResponse,
};
use near_jsonrpc_primitives::types::transactions::{
    RpcSendTransactionRequest, RpcTransactionResponse, RpcTransactionStatusRequest,
};
use near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockId, BlockReference, EpochReference, MaybeBlockId, ShardId};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, GasPriceView, StatusResponse,
};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum ChunkId {
    BlockShardId(BlockId, ShardId),
    Hash(CryptoHash),
}

/// Timeout for establishing connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Max size of the payload JsonRpcClient can receive. Be careful adjusting this value since
/// smaller values can raise overflow messages.
const PAYLOAD_LIMIT: usize = 100 * 1024 * 1024;

type HttpRequest<T> = BoxFuture<'static, Result<T, String>>;
type RpcRequest<T> = BoxFuture<'static, Result<T, RpcError>>;

/// Trait which allows to send an RPC request and receive a response.
/// Implementors must provide implementation for `send_http_request`.
/// Provides a method to query jsonrpc, in the future rosetta will also be added.
/// Useful in tests, where normal network connections might not be available.
pub trait RpcTransport: Send + Sync {
    /// Sends an HTTP POST request to `endpoint` with the given body bytes.
    /// Returns the response status and body.
    fn send_http_request(
        &self,
        endpoint: &str,
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<(StatusCode, Vec<u8>), String>>;

    /// Sends a jsonrpc request and returns the parsed response message.
    fn send_jsonrpc_request(
        &self,
        request: Message,
    ) -> BoxFuture<'static, Result<Message, RpcError>> {
        let body_bytes: Vec<u8> = match serde_json::to_vec(&request) {
            Ok(serialized) => serialized,
            Err(e) => {
                return Box::pin(async move {
                    Err(RpcError::serialization_error(format!(
                        "request serialization failed: {}",
                        e
                    )))
                });
            }
        };

        let request_future = self.send_http_request("/", body_bytes);
        Box::pin(async move {
            let (_status, bytes) = request_future.await.map_err(|err| {
                RpcError::new_internal_error(
                    None,
                    format!("sending jsonrpc request failed: {}", err),
                )
            })?;

            // TODO(rpc): do we really need a payload limit?
            if bytes.len() > PAYLOAD_LIMIT {
                return Err(RpcError::parse_error(format!(
                    "response payload too large: {} bytes, limit: {} bytes",
                    bytes.len(),
                    PAYLOAD_LIMIT
                )));
            }

            let msg: Message =
                near_jsonrpc_primitives::message::from_slice(&bytes).map_err(|err| {
                    RpcError::parse_error(format!(
                        "parsing jsonrpc response message failed: {:?}",
                        err
                    ))
                })?;
            Ok(msg)
        })
    }
}

/// RpcTransport implementation backed by reqwest HTTP client.
struct ReqwestTransport {
    client: Client,
    server_addr: String,
}

impl RpcTransport for ReqwestTransport {
    fn send_http_request(
        &self,
        endpoint: &str,
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<(StatusCode, Vec<u8>), String>> {
        let url = url::Url::parse(&self.server_addr)
            .map_err(|e| format!("parsing server_addr '{}' failed: {}", self.server_addr, e))
            .and_then(|u| {
                u.join(endpoint).map_err(|e| {
                    format!("joining url '{}' and '{}' failed: {}", self.server_addr, endpoint, e)
                })
            });
        let client = self.client.clone();
        async move {
            let response = client
                .post(url?)
                .header("Content-Type", "application/json")
                .body(body)
                .send()
                .await
                .map_err(|err| format!("http request failed: {}", err))?;
            let status = response.status();
            let response_body = response
                .bytes()
                .await
                .map_err(|err| format!("failed to retrieve http response body: {}", err))?;
            Ok((status, response_body.to_vec()))
        }
        .boxed()
    }
}

/// Prepare a `RPCRequest` with a given transport, method and parameters.
fn call_method<P, R>(transport: &Arc<dyn RpcTransport>, method: &str, params: P) -> RpcRequest<R>
where
    P: serde::Serialize,
    R: serde::de::DeserializeOwned + 'static,
{
    let request = Message::request(method.to_string(), serde_json::to_value(&params).unwrap());
    let future = transport.send_jsonrpc_request(request);

    async move {
        let message = future.await?;

        match message {
            Message::Response(resp) => resp.result.and_then(|x| {
                serde_json::from_value(x)
                    .map_err(|err| RpcError::parse_error(format!("Failed to parse: {:?}", err)))
            }),
            _ => Err(RpcError::parse_error("Failed to parse JSON RPC response".to_string())),
        }
    }
    .boxed()
}

/// Prepare a `HttpRequest` with a given client, server address and parameters.
fn call_http_get<R, P>(
    client: &Client,
    server_addr: &str,
    method: &str,
    _params: P,
) -> HttpRequest<R>
where
    P: serde::Serialize,
    R: serde::de::DeserializeOwned + 'static,
{
    let client = client.clone();
    let server_addr = Url::parse(server_addr).expect("Invalid server address");
    let url = server_addr.join(method).unwrap();

    async move {
        let response = client.get(url).send().await.map_err(|err| err.to_string())?;
        let bytes = response.bytes().await.map_err(|err| format!("Payload error: {err}"))?;
        serde_json::from_slice(&bytes).map_err(|err| err.to_string())
    }
    .boxed()
}

/// JsonRPC client with a pluggable transport.
pub struct JsonRpcClient {
    pub transport: Arc<dyn RpcTransport>,
    /// Server address, available when using the default reqwest transport.
    pub server_addr: String,
}

impl JsonRpcClient {
    /// Creates a new RPC client backed by reqwest HTTP client.
    pub fn new(server_addr: &str, client: Client) -> Self {
        let transport = Arc::new(ReqwestTransport { client, server_addr: server_addr.to_string() });
        JsonRpcClient { transport, server_addr: server_addr.to_string() }
    }

    /// Creates a new RPC client backed by the given transport implementation.
    /// Useful for testing, e.g. in TestLoop with TestLoopRpcTransport.
    pub fn new_with_transport(transport: Arc<dyn RpcTransport>) -> Self {
        JsonRpcClient { transport, server_addr: String::new() }
    }

    pub fn broadcast_tx_async(&self, tx: String) -> RpcRequest<String> {
        call_method(&self.transport, "broadcast_tx_async", [tx])
    }

    pub fn broadcast_tx_commit(&self, tx: String) -> RpcRequest<RpcTransactionResponse> {
        call_method(&self.transport, "broadcast_tx_commit", [tx])
    }

    pub fn status(&self) -> RpcRequest<StatusResponse> {
        call_method(&self.transport, "status", [] as [(); 0])
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_genesis_config(&self) -> RpcRequest<serde_json::Value> {
        call_method(&self.transport, "EXPERIMENTAL_genesis_config", [] as [(); 0])
    }

    pub fn genesis_config(&self) -> RpcRequest<serde_json::Value> {
        call_method(&self.transport, "genesis_config", [] as [(); 0])
    }

    pub fn health(&self) -> RpcRequest<()> {
        call_method(&self.transport, "health", [] as [(); 0])
    }

    pub fn chunk(&self, id: ChunkId) -> RpcRequest<ChunkView> {
        call_method(&self.transport, "chunk", [id])
    }

    pub fn gas_price(&self, block_id: MaybeBlockId) -> RpcRequest<GasPriceView> {
        call_method(&self.transport, "gas_price", [block_id])
    }
    /// This is a soft-deprecated method to do query RPC request with a path and data positional
    /// parameters.
    pub fn query_by_path(
        &self,
        path: String,
        data: String,
    ) -> RpcRequest<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
        call_method(&self.transport, "query", [path, data])
    }

    pub fn query(
        &self,
        request: near_jsonrpc_primitives::types::query::RpcQueryRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
        call_method(&self.transport, "query", request)
    }

    pub fn block_by_id(&self, block_id: BlockId) -> RpcRequest<BlockView> {
        call_method(&self.transport, "block", [block_id])
    }

    pub fn block(&self, request: BlockReference) -> RpcRequest<BlockView> {
        call_method(&self.transport, "block", request)
    }

    pub fn tx(&self, request: RpcTransactionStatusRequest) -> RpcRequest<RpcTransactionResponse> {
        call_method(&self.transport, "tx", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_tx_status(
        &self,
        request: RpcTransactionStatusRequest,
    ) -> RpcRequest<RpcTransactionResponse> {
        call_method(&self.transport, "EXPERIMENTAL_tx_status", request)
    }

    #[deprecated(since = "2.7.0", note = "Use `changes` method instead")]
    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_changes(
        &self,
        request: RpcStateChangesInBlockByTypeRequest,
    ) -> RpcRequest<RpcStateChangesInBlockByTypeResponse> {
        call_method(&self.transport, "EXPERIMENTAL_changes", request)
    }

    #[allow(non_snake_case)]
    pub fn changes(
        &self,
        request: RpcStateChangesInBlockByTypeRequest,
    ) -> RpcRequest<RpcStateChangesInBlockByTypeResponse> {
        call_method(&self.transport, "changes", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_validators_ordered(
        &self,
        request: RpcValidatorsOrderedRequest,
    ) -> RpcRequest<Vec<ValidatorStakeView>> {
        call_method(&self.transport, "EXPERIMENTAL_validators_ordered", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_receipt(
        &self,
        request: near_jsonrpc_primitives::types::receipts::RpcReceiptRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::receipts::RpcReceiptResponse> {
        call_method(&self.transport, "EXPERIMENTAL_receipt", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_protocol_config(
        &self,
        request: near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse> {
        call_method(&self.transport, "EXPERIMENTAL_protocol_config", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_split_storage_info(
        &self,
        request: near_jsonrpc_primitives::types::split_storage::RpcSplitStorageInfoRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::split_storage::RpcSplitStorageInfoResponse>
    {
        call_method(&self.transport, "EXPERIMENTAL_split_storage_info", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_view_account(
        &self,
        request: near_jsonrpc_primitives::types::view_account::RpcViewAccountRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::view_account::RpcViewAccountResponse> {
        call_method(&self.transport, "EXPERIMENTAL_view_account", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_view_code(
        &self,
        request: near_jsonrpc_primitives::types::view_code::RpcViewCodeRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::view_code::RpcViewCodeResponse> {
        call_method(&self.transport, "EXPERIMENTAL_view_code", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_view_state(
        &self,
        request: near_jsonrpc_primitives::types::view_state::RpcViewStateRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::view_state::RpcViewStateResponse> {
        call_method(&self.transport, "EXPERIMENTAL_view_state", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_view_access_key(
        &self,
        request: near_jsonrpc_primitives::types::view_access_key::RpcViewAccessKeyRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::view_access_key::RpcViewAccessKeyResponse> {
        call_method(&self.transport, "EXPERIMENTAL_view_access_key", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_view_access_key_list(
        &self,
        request: near_jsonrpc_primitives::types::view_access_key_list::RpcViewAccessKeyListRequest,
    ) -> RpcRequest<
        near_jsonrpc_primitives::types::view_access_key_list::RpcViewAccessKeyListResponse,
    > {
        call_method(&self.transport, "EXPERIMENTAL_view_access_key_list", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_call_function(
        &self,
        request: near_jsonrpc_primitives::types::call_function::RpcCallFunctionRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::call_function::RpcCallFunctionResponse> {
        call_method(&self.transport, "EXPERIMENTAL_call_function", request)
    }

    pub fn validators(
        &self,
        epoch_id_or_block_id: Option<EpochReference>,
    ) -> RpcRequest<EpochValidatorInfo> {
        let epoch_reference = match epoch_id_or_block_id {
            Some(epoch_reference) => epoch_reference,
            _ => EpochReference::Latest,
        };
        call_method(&self.transport, "validators", epoch_reference)
    }

    pub fn send_tx(
        &self,
        signed_transaction: near_primitives::transaction::SignedTransaction,
        wait_until: near_primitives::views::TxExecutionStatus,
    ) -> RpcRequest<RpcTransactionResponse> {
        let request = RpcSendTransactionRequest { signed_transaction, wait_until };
        call_method(&self.transport, "send_tx", request)
    }
}

fn create_client() -> Client {
    Client::builder()
        .timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client")
}

/// Create new JSON RPC client that connects to the given address.
pub fn new_client(server_addr: &str) -> JsonRpcClient {
    JsonRpcClient::new(server_addr, create_client())
}

/// HTTP client for simple REST endpoints
pub struct HttpClient {
    server_addr: String,
    client: Client,
}

impl HttpClient {
    /// Creates a new HTTP client backed by the given transport implementation.
    pub fn new(server_addr: &str, client: Client) -> Self {
        HttpClient { server_addr: server_addr.to_string(), client }
    }

    pub fn status(&self) -> HttpRequest<StatusResponse> {
        call_http_get(&self.client, &self.server_addr, "status", [] as [(); 0])
    }
}

/// Create new HTTP client that connects to the given address.
pub fn new_http_client(server_addr: &str) -> HttpClient {
    HttpClient::new(server_addr, create_client())
}
