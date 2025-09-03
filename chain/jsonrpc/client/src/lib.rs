use futures::FutureExt;
use futures::future::BoxFuture;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::message::{Message, from_slice};
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
use reqwest::Client;
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

/// Prepare a `RPCRequest` with a given client, server address, method and parameters.
fn call_method<P, R>(client: &Client, server_addr: &str, method: &str, params: P) -> RpcRequest<R>
where
    P: serde::Serialize,
    R: serde::de::DeserializeOwned + 'static,
{
    let request = Message::request(method.to_string(), serde_json::to_value(&params).unwrap());
    let client = client.clone();
    let server_addr = server_addr.to_string();

    async move {
        let response = client
            .post(&server_addr)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|err| RpcError::new_internal_error(None, format!("{:?}", err)))?;

        let bytes = response.bytes().await.map_err(|err| {
            RpcError::parse_error(format!("Failed to retrieve payload: {:?}", err))
        })?;

        if bytes.len() > PAYLOAD_LIMIT {
            return Err(RpcError::parse_error(format!(
                "Response payload too large: {} bytes, limit: {} bytes",
                bytes.len(),
                PAYLOAD_LIMIT
            )));
        }

        let message = from_slice(&bytes)
            .map_err(|err| RpcError::parse_error(format!("Error {:?} in {:?}", err, bytes)))?;

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

/// JsonRPC client that uses reqwest for HTTP transport
pub struct JsonRpcClient {
    pub server_addr: String,
    pub client: Client,
}

impl JsonRpcClient {
    /// Creates a new RPC client backed by reqwest HTTP client
    pub fn new(server_addr: &str, client: Client) -> Self {
        JsonRpcClient { server_addr: server_addr.to_string(), client }
    }

    pub fn broadcast_tx_async(&self, tx: String) -> RpcRequest<String> {
        call_method(&self.client, &self.server_addr, "broadcast_tx_async", [tx])
    }

    pub fn broadcast_tx_commit(&self, tx: String) -> RpcRequest<RpcTransactionResponse> {
        call_method(&self.client, &self.server_addr, "broadcast_tx_commit", [tx])
    }

    pub fn status(&self) -> RpcRequest<StatusResponse> {
        call_method(&self.client, &self.server_addr, "status", [] as [(); 0])
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_genesis_config(&self) -> RpcRequest<serde_json::Value> {
        call_method(&self.client, &self.server_addr, "EXPERIMENTAL_genesis_config", [] as [(); 0])
    }

    pub fn genesis_config(&self) -> RpcRequest<serde_json::Value> {
        call_method(&self.client, &self.server_addr, "genesis_config", [] as [(); 0])
    }

    pub fn health(&self) -> RpcRequest<()> {
        call_method(&self.client, &self.server_addr, "health", [] as [(); 0])
    }

    pub fn chunk(&self, id: ChunkId) -> RpcRequest<ChunkView> {
        call_method(&self.client, &self.server_addr, "chunk", [id])
    }

    pub fn gas_price(&self, block_id: MaybeBlockId) -> RpcRequest<GasPriceView> {
        call_method(&self.client, &self.server_addr, "gas_price", [block_id])
    }
    /// This is a soft-deprecated method to do query RPC request with a path and data positional
    /// parameters.
    pub fn query_by_path(
        &self,
        path: String,
        data: String,
    ) -> RpcRequest<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
        call_method(&self.client, &self.server_addr, "query", [path, data])
    }

    pub fn query(
        &self,
        request: near_jsonrpc_primitives::types::query::RpcQueryRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
        call_method(&self.client, &self.server_addr, "query", request)
    }

    pub fn block_by_id(&self, block_id: BlockId) -> RpcRequest<BlockView> {
        call_method(&self.client, &self.server_addr, "block", [block_id])
    }

    pub fn block(&self, request: BlockReference) -> RpcRequest<BlockView> {
        call_method(&self.client, &self.server_addr, "block", request)
    }

    pub fn tx(&self, request: RpcTransactionStatusRequest) -> RpcRequest<RpcTransactionResponse> {
        call_method(&self.client, &self.server_addr, "tx", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_tx_status(
        &self,
        request: RpcTransactionStatusRequest,
    ) -> RpcRequest<RpcTransactionResponse> {
        call_method(&self.client, &self.server_addr, "EXPERIMENTAL_tx_status", request)
    }

    #[deprecated(since = "2.7.0", note = "Use `changes` method instead")]
    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_changes(
        &self,
        request: RpcStateChangesInBlockByTypeRequest,
    ) -> RpcRequest<RpcStateChangesInBlockByTypeResponse> {
        call_method(&self.client, &self.server_addr, "EXPERIMENTAL_changes", request)
    }

    #[allow(non_snake_case)]
    pub fn changes(
        &self,
        request: RpcStateChangesInBlockByTypeRequest,
    ) -> RpcRequest<RpcStateChangesInBlockByTypeResponse> {
        call_method(&self.client, &self.server_addr, "changes", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_validators_ordered(
        &self,
        request: RpcValidatorsOrderedRequest,
    ) -> RpcRequest<Vec<ValidatorStakeView>> {
        call_method(&self.client, &self.server_addr, "EXPERIMENTAL_validators_ordered", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_receipt(
        &self,
        request: near_jsonrpc_primitives::types::receipts::RpcReceiptRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::receipts::RpcReceiptResponse> {
        call_method(&self.client, &self.server_addr, "EXPERIMENTAL_receipt", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_protocol_config(
        &self,
        request: near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse> {
        call_method(&self.client, &self.server_addr, "EXPERIMENTAL_protocol_config", request)
    }

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_split_storage_info(
        &self,
        request: near_jsonrpc_primitives::types::split_storage::RpcSplitStorageInfoRequest,
    ) -> RpcRequest<near_jsonrpc_primitives::types::split_storage::RpcSplitStorageInfoResponse>
    {
        call_method(&self.client, &self.server_addr, "EXPERIMENTAL_split_storage_info", request)
    }

    pub fn validators(
        &self,
        epoch_id_or_block_id: Option<EpochReference>,
    ) -> RpcRequest<EpochValidatorInfo> {
        let epoch_reference = match epoch_id_or_block_id {
            Some(epoch_reference) => epoch_reference,
            _ => EpochReference::Latest,
        };
        call_method(&self.client, &self.server_addr, "validators", epoch_reference)
    }

    pub fn send_tx(
        &self,
        signed_transaction: near_primitives::transaction::SignedTransaction,
        wait_until: near_primitives::views::TxExecutionStatus,
    ) -> RpcRequest<RpcTransactionResponse> {
        let request = RpcSendTransactionRequest { signed_transaction, wait_until };
        call_method(&self.client, &self.server_addr, "send_tx", request)
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
