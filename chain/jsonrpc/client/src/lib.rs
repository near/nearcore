use awc::{Client, Connector};
use futures::{future, future::LocalBoxFuture, FutureExt, TryFutureExt};
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::message::{from_slice, Message};
use near_jsonrpc_primitives::types::changes::{
    RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockByTypeResponse,
};
use near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockId, BlockReference, MaybeBlockId, ShardId};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, GasPriceView,
    StatusResponse,
};
use std::time::Duration;

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

type HttpRequest<T> = LocalBoxFuture<'static, Result<T, String>>;
type RpcRequest<T> = LocalBoxFuture<'static, Result<T, RpcError>>;

/// Prepare a `RPCRequest` with a given client, server address, method and parameters.
fn call_method<P, R>(client: &Client, server_addr: &str, method: &str, params: P) -> RpcRequest<R>
where
    P: serde::Serialize,
    R: serde::de::DeserializeOwned + 'static,
{
    let request = Message::request(method.to_string(), serde_json::to_value(&params).unwrap());
    // TODO: simplify this.
    client
        .post(server_addr)
        .insert_header(("Content-Type", "application/json"))
        .send_json(&request)
        .map_err(|err| RpcError::new_internal_error(None, format!("{:?}", err)))
        .and_then(|mut response| {
            response.body().limit(PAYLOAD_LIMIT).map(|body| match body {
                Ok(bytes) => from_slice(&bytes).map_err(|err| {
                    RpcError::parse_error(format!("Error {:?} in {:?}", err, bytes))
                }),
                Err(err) => {
                    Err(RpcError::parse_error(format!("Failed to retrieve payload: {:?}", err)))
                }
            })
        })
        .and_then(|message| {
            future::ready(match message {
                Message::Response(resp) => resp.result.and_then(|x| {
                    serde_json::from_value(x)
                        .map_err(|err| RpcError::parse_error(format!("Failed to parse: {:?}", err)))
                }),
                _ => Err(RpcError::parse_error("Failed to parse JSON RPC response".to_string())),
            })
        })
        .boxed_local()
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
    // TODO: url encode params.
    client
        .get(format!("{}/{}", server_addr, method))
        .send()
        .map_err(|err| err.to_string())
        .and_then(|mut response| {
            response.body().map(|body| match body {
                Ok(bytes) => serde_json::from_slice(&bytes).map_err(|err| err.to_string()),
                Err(err) => Err(format!("Payload error: {err}")),
            })
        })
        .boxed_local()
}

/// Expands a variable list of parameters into its serializable form. Is needed to make the params
/// of a nullary method equal to `[]` instead of `()` and thus make sure it serializes to `[]`
/// instead of `null`.
#[doc(hidden)]
macro_rules! expand_params {
    () => ([] as [(); 0]);
    ($($arg_name:ident,)+) => (($($arg_name,)+))
}

/// Generates a simple HTTP client with automatic serialization and deserialization.
/// Method calls get correct types automatically.
macro_rules! http_client {
    (
        $(#[$struct_attr:meta])*
        pub struct $struct_name:ident {$(
            $(#[$attr:meta])*
            pub fn $method:ident(&mut $selff:ident $(, $arg_name:ident: $arg_ty:ty)*)
                -> HttpRequest<$return_ty:ty>;
        )*}
    ) => (
        $(#[$struct_attr])*
        pub struct $struct_name {
            server_addr: String,
            client: Client,
        }

        impl $struct_name {
            /// Creates a new HTTP client backed by the given transport implementation.
            pub fn new(server_addr: &str, client: Client) -> Self {
                $struct_name { server_addr: server_addr.to_string(), client }
            }

            $(
                $(#[$attr])*
                pub fn $method(&$selff $(, $arg_name: $arg_ty)*)
                    -> HttpRequest<$return_ty>
                {
                    let method = stringify!($method);
                    let params = expand_params!($($arg_name,)*);
                    call_http_get(&$selff.client, &$selff.server_addr, &method, params)
                }
            )*
        }
    )
}

/// Generates JSON-RPC 2.0 client structs with automatic serialization
/// and deserialization. Method calls get correct types automatically.
macro_rules! jsonrpc_client {
    (
        $(#[$struct_attr:meta])*
        pub struct $struct_name:ident {$(
            $(#[$attr:meta])*
            pub fn $method:ident(&$selff:ident $(, $arg_name:ident: $arg_ty:ty)*)
                -> RpcRequest<$return_ty:ty>;
        )*}
    ) => (
        $(#[$struct_attr])*
        pub struct $struct_name {
            pub server_addr: String,
            pub client: Client,
        }

        impl $struct_name {
            /// Creates a new RPC client backed by the given transport implementation.
            pub fn new(server_addr: &str, client: Client) -> Self {
                $struct_name { server_addr: server_addr.to_string(), client }
            }

            $(
                $(#[$attr])*
                pub fn $method(&$selff $(, $arg_name: $arg_ty)*)
                    -> RpcRequest<$return_ty>
                {
                    let method = stringify!($method);
                    let params = expand_params!($($arg_name,)*);
                    call_method(&$selff.client, &$selff.server_addr, &method, params)
                }
            )*
        }
    )
}

jsonrpc_client!(pub struct JsonRpcClient {
    pub fn broadcast_tx_async(&self, tx: String) -> RpcRequest<String>;
    pub fn broadcast_tx_commit(&self, tx: String) -> RpcRequest<FinalExecutionOutcomeView>;
    pub fn status(&self) -> RpcRequest<StatusResponse>;
    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_check_tx(&self, tx: String) -> RpcRequest<serde_json::Value>;
    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_genesis_config(&self) -> RpcRequest<serde_json::Value>;
    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_broadcast_tx_sync(&self, tx: String) -> RpcRequest<serde_json::Value>;
    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_tx_status(&self, tx: String) -> RpcRequest<serde_json::Value>;
    pub fn health(&self) -> RpcRequest<()>;
    pub fn tx(&self, hash: String, account_id: AccountId) -> RpcRequest<FinalExecutionOutcomeView>;
    pub fn chunk(&self, id: ChunkId) -> RpcRequest<ChunkView>;
    pub fn validators(&self, block_id: MaybeBlockId) -> RpcRequest<EpochValidatorInfo>;
    pub fn gas_price(&self, block_id: MaybeBlockId) -> RpcRequest<GasPriceView>;
});

impl JsonRpcClient {
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

    #[allow(non_snake_case)]
    pub fn EXPERIMENTAL_changes(
        &self,
        request: RpcStateChangesInBlockByTypeRequest,
    ) -> RpcRequest<RpcStateChangesInBlockByTypeResponse> {
        call_method(&self.client, &self.server_addr, "EXPERIMENTAL_changes", request)
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
}

fn create_client() -> Client {
    Client::builder()
        .timeout(CONNECT_TIMEOUT)
        .connector(
            Connector::new()
                .conn_lifetime(Duration::from_secs(u64::max_value()))
                .conn_keep_alive(Duration::from_secs(30)),
        )
        .finish()
}

/// Create new JSON RPC client that connects to the given address.
pub fn new_client(server_addr: &str) -> JsonRpcClient {
    JsonRpcClient::new(server_addr, create_client())
}

http_client!(pub struct HttpClient {
    pub fn status(&mut self) -> HttpRequest<StatusResponse>;
});

/// Create new HTTP client that connects to the given address.
pub fn new_http_client(server_addr: &str) -> HttpClient {
    HttpClient::new(server_addr, create_client())
}
