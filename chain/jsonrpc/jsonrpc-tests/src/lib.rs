use std::sync::Arc;

use actix::Addr;
use futures::{future, future::LocalBoxFuture, FutureExt, TryFutureExt};
use near_async::time::Clock;
use near_async::{
    actix::AddrWithAutoSpanContextExt,
    messaging::{noop, IntoMultiSender},
};
use near_chain_configs::GenesisConfig;
use near_client::test_utils::setup_no_network_with_validity_period_and_no_epoch_sync;
use near_client::ViewClientActor;
use near_jsonrpc::{start_http, RpcConfig};
use near_jsonrpc_primitives::{
    message::{from_slice, Message},
    types::entity_debug::DummyEntityDebugHandler,
};
use near_network::tcp;
use near_primitives::types::NumBlocks;
use once_cell::sync::Lazy;
use serde_json::json;

pub static TEST_GENESIS_CONFIG: Lazy<GenesisConfig> =
    Lazy::new(|| GenesisConfig::from_json(include_str!("../res/genesis_config.json")));

pub enum NodeType {
    Validator,
    NonValidator,
}

pub fn start_all(clock: Clock, node_type: NodeType) -> (Addr<ViewClientActor>, tcp::ListenerAddr) {
    start_all_with_validity_period_and_no_epoch_sync(clock, node_type, 100, false)
}

pub fn start_all_with_validity_period_and_no_epoch_sync(
    clock: Clock,
    node_type: NodeType,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (Addr<ViewClientActor>, tcp::ListenerAddr) {
    let actor_handles = setup_no_network_with_validity_period_and_no_epoch_sync(
        clock,
        vec!["test1".parse().unwrap()],
        if let NodeType::Validator = node_type {
            "test1".parse().unwrap()
        } else {
            "other".parse().unwrap()
        },
        true,
        transaction_validity_period,
        enable_doomslug,
    );

    let addr = tcp::ListenerAddr::reserve_for_test();
    start_http(
        RpcConfig::new(addr),
        TEST_GENESIS_CONFIG.clone(),
        actor_handles.client_actor.clone().with_auto_span_context().into_multi_sender(),
        actor_handles.view_client_actor.clone().with_auto_span_context().into_multi_sender(),
        noop().into_multi_sender(),
        #[cfg(feature = "test_features")]
        noop().into_multi_sender(),
        Arc::new(DummyEntityDebugHandler {}),
    );
    (actor_handles.view_client_actor, addr)
}

#[macro_export]
macro_rules! test_with_client {
    ($node_type:expr, $client:ident, $block:expr) => {
        init_test_logger();

        near_actix_test_utils::run_actix(async {
            let (_view_client_addr, addr) =
                test_utils::start_all(near_async::time::Clock::real(), $node_type);

            let $client = new_client(&format!("http://{}", addr));

            actix::spawn(async move {
                $block.await;
                System::current().stop();
            });
        });
    };
}

type RpcRequest<T> = LocalBoxFuture<'static, Result<T, near_jsonrpc_primitives::errors::RpcError>>;

/// Prepare a `RPCRequest` with a given client, server address, method and parameters.
pub fn call_method<R>(
    client: &awc::Client,
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
        .insert_header(("Content-Type", "application/json"))
        .send_json(&request)
        .map_err(|err| {
            near_jsonrpc_primitives::errors::RpcError::new_internal_error(
                None,
                format!("{:?}", err),
            )
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
                _ => Err(near_jsonrpc_primitives::errors::RpcError::parse_error(
                    "Failed to parse JSON RPC response".to_string(),
                )),
            })
        })
        .boxed_local()
}
