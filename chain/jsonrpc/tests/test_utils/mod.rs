use actix::Addr;
use futures::{future, future::LocalBoxFuture, FutureExt, TryFutureExt};
use serde_json::json;

use near_chain_configs::GenesisConfig;
use near_client::test_utils::setup_no_network_with_validity_period;
use near_client::ViewClientActor;
use near_jsonrpc::{start_http, RpcConfig};
use near_network::test_utils::open_port;
use near_primitives::types::NumBlocks;

use near_jsonrpc_primitives::message::{from_slice, Message};

lazy_static::lazy_static! {
    pub static ref TEST_GENESIS_CONFIG: GenesisConfig =
        GenesisConfig::from_json(include_str!("../../../../neard/res/genesis_config.json"));
}

pub enum NodeType {
    Validator,
    NonValidator,
}

pub fn start_all(node_type: NodeType) -> (Addr<ViewClientActor>, String) {
    start_all_with_validity_period(node_type, 100, false)
}

pub fn start_all_with_validity_period(
    node_type: NodeType,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (Addr<ViewClientActor>, String) {
    let (client_addr, view_client_addr) = setup_no_network_with_validity_period(
        vec!["test1", "test2"],
        if let NodeType::Validator = node_type { "test1" } else { "other" },
        true,
        transaction_validity_period,
        enable_doomslug,
    );

    let addr = format!("127.0.0.1:{}", open_port());

    start_http(
        RpcConfig::new(&addr),
        TEST_GENESIS_CONFIG.clone(),
        client_addr.clone(),
        view_client_addr.clone(),
    );
    (view_client_addr, addr)
}

#[allow(unused_macros)] // Suppress Rustc warnings even though this macro is used.
macro_rules! test_with_client {
    ($node_type:expr, $client:ident, $block:expr) => {
        init_test_logger();

        System::builder()
            .stop_on_panic(true)
            .run(|| {
                let (_view_client_addr, addr) = test_utils::start_all($node_type);

                let $client = new_client(&format!("http://{}", addr));

                actix::spawn(async move {
                    $block.await;
                    System::current().stop();
                });
            })
            .unwrap();
    };
}

type RpcRequest<T> = LocalBoxFuture<'static, Result<T, near_jsonrpc_primitives::errors::RpcError>>;

/// Prepare a `RPCRequest` with a given client, server address, method and parameters.
pub fn call_method<R>(
    client: &actix_web::client::Client,
    server_addr: &str,
    method: &str,
    params: serde_json::Value,
) -> RpcRequest<R>
where
    R: serde::de::DeserializeOwned + 'static,
{
    let request = json!({
        "jsonrpc": "2.0",
        "method": method,
        "id": "dontcare",
        "params": params,
    });
    // TODO: simplify this.
    client
        .post(server_addr)
        .header("Content-Type", "application/json")
        .send_json(&request)
        .map_err(|err| {
            near_jsonrpc_primitives::errors::RpcError::server_error(Some(format!("{:?}", err)))
        })
        .and_then(|mut response| {
            response.body().map(|body| match body {
                Ok(bytes) => from_slice(&bytes).map_err(|err| {
                    near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                        "Error {:?} in {:?}",
                        err, bytes
                    ))
                }),
                Err(err) => Err(near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                    "Failed to retrieve payload: {:?}",
                    err
                ))),
            })
        })
        .and_then(|message| {
            future::ready(match message {
                Message::Response(resp) => resp.result.and_then(|x| {
                    serde_json::from_value(x).map_err(|err| {
                        near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                            "Failed to parse: {:?}",
                            err
                        ))
                    })
                }),
                _ => Err(near_jsonrpc_primitives::errors::RpcError::invalid_request()),
            })
        })
        .boxed_local()
}
