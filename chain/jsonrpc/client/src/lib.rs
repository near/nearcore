use std::time::Duration;

use actix_web::client::{Client, Connector};
use futures::Future;
use serde::Deserialize;
use serde::Serialize;

use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, GasPriceView,
    QueryResponse, StatusResponse,
};

use crate::message::{from_slice, Message, RpcError};

pub mod message;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BlockId {
    Height(BlockHeight),
    Hash(CryptoHash),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChunkId {
    BlockShardId(BlockId, ShardId),
    Hash(CryptoHash),
}

/// Timeout for establishing connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

type HttpRequest<T> = Box<dyn Future<Item = T, Error = String>>;
type RpcRequest<T> = Box<dyn Future<Item = T, Error = RpcError>>;

/// Prepare a `RPCRequest` with a given client, server address, method and parameters.
fn call_method<P, R>(client: &Client, server_addr: &str, method: &str, params: P) -> RpcRequest<R>
where
    P: Serialize,
    R: serde::de::DeserializeOwned + 'static,
{
    let request =
        Message::request(method.to_string(), Some(serde_json::to_value(&params).unwrap()));
    // TODO: simplify this.
    Box::new(
        client
            .post(server_addr)
            .header("Content-Type", "application/json")
            .send_json(&request)
            .map_err(|_| RpcError::invalid_request())
            .and_then(|mut response| {
                response.body().then(|body| match body {
                    Ok(bytes) => from_slice(&bytes).map_err(|err| {
                        RpcError::parse_error(format!("Error {:?} in {:?}", err, bytes))
                    }),
                    Err(err) => {
                        Err(RpcError::parse_error(format!("Failed to retrieve payload: {:?}", err)))
                    }
                })
            })
            .and_then(|message| match message {
                Message::Response(resp) => resp.result.and_then(|x| {
                    serde_json::from_value(x)
                        .map_err(|err| RpcError::parse_error(format!("Failed to parse: {:?}", err)))
                }),
                _ => Err(RpcError::invalid_request()),
            }),
    )
}

/// Prepare a `HttpRequest` with a given client, server address and parameters.
fn call_http_get<R, P>(
    client: &Client,
    server_addr: &str,
    method: &str,
    _params: P,
) -> HttpRequest<R>
where
    P: Serialize,
    R: serde::de::DeserializeOwned + 'static,
{
    // TODO: url encode params.
    let response = client
        .get(format!("{}/{}", server_addr, method))
        .send()
        .map_err(|err| err.to_string())
        .and_then(|mut response| {
            response.body().then(|body| match body {
                Ok(bytes) => String::from_utf8(bytes.to_vec())
                    .map_err(|err| format!("Error {:?} in {:?}", err, bytes))
                    .and_then(|s| serde_json::from_str(&s).map_err(|err| err.to_string())),
                Err(_) => Err("Payload error: {:?}".to_string()),
            })
        });
    Box::new(response)
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
                pub fn $method(&mut $selff $(, $arg_name: $arg_ty)*)
                    -> HttpRequest<$return_ty>
                {
                    let method = String::from(stringify!($method));
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
            pub fn $method:ident(&mut $selff:ident $(, $arg_name:ident: $arg_ty:ty)*)
                -> RpcRequest<$return_ty:ty>;
        )*}
    ) => (
        $(#[$struct_attr])*
        pub struct $struct_name {
            server_addr: String,
            client: Client,
        }

        impl $struct_name {
            /// Creates a new RPC client backed by the given transport implementation.
            pub fn new(server_addr: &str, client: Client) -> Self {
                $struct_name { server_addr: server_addr.to_string(), client }
            }

            $(
                $(#[$attr])*
                pub fn $method(&mut $selff $(, $arg_name: $arg_ty)*)
                    -> RpcRequest<$return_ty>
                {
                    let method = String::from(stringify!($method));
                    let params = expand_params!($($arg_name,)*);
                    call_method(&$selff.client, &$selff.server_addr, &method, params)
                }
            )*
        }
    )
}

jsonrpc_client!(pub struct JsonRpcClient {
    pub fn broadcast_tx_async(&mut self, tx: String) -> RpcRequest<String>;
    pub fn broadcast_tx_commit(&mut self, tx: String) -> RpcRequest<FinalExecutionOutcomeView>;
    pub fn query(&mut self, path: String, data: String) -> RpcRequest<QueryResponse>;
    pub fn status(&mut self) -> RpcRequest<StatusResponse>;
    pub fn health(&mut self) -> RpcRequest<()>;
    pub fn tx(&mut self, hash: String, account_id: String) -> RpcRequest<FinalExecutionOutcomeView>;
    pub fn block(&mut self, id: BlockId) -> RpcRequest<BlockView>;
    pub fn chunk(&mut self, id: ChunkId) -> RpcRequest<ChunkView>;
    pub fn validators(&mut self, block_hash: String) -> RpcRequest<EpochValidatorInfo>;
    pub fn gas_price(&mut self, id: Option<BlockId>) -> RpcRequest<GasPriceView>;
});

fn create_client() -> Client {
    Client::build()
        .timeout(CONNECT_TIMEOUT)
        .connector(
            Connector::new()
                .conn_lifetime(Duration::from_secs(u64::max_value()))
                .conn_keep_alive(Duration::from_secs(30))
                .finish(),
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
