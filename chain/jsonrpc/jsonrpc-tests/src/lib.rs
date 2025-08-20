use std::sync::Arc;

use futures::{FutureExt, future::LocalBoxFuture};
use integration_tests::env::setup::setup_no_network_with_validity_period;
use near_async::ActorSystem;
use near_async::messaging::{IntoMultiSender, noop};
use near_async::multithread::MultithreadRuntimeHandle;
use near_chain_configs::GenesisConfig;
use near_client::ViewClientActorInner;
use near_jsonrpc::{RpcConfig, start_http};
use near_jsonrpc_primitives::{
    message::{Message, from_slice},
    types::entity_debug::DummyEntityDebugHandler,
};
use near_network::tcp;
use near_primitives::types::NumBlocks;
use near_time::Clock;
use serde_json::json;

pub static TEST_GENESIS_CONFIG: std::sync::LazyLock<GenesisConfig> =
    std::sync::LazyLock::new(|| {
        GenesisConfig::from_json(include_str!("../res/genesis_config.json"))
    });

pub enum NodeType {
    Validator,
    NonValidator,
}

pub fn start_all(
    clock: Clock,
    actor_system: ActorSystem,
    node_type: NodeType,
) -> (MultithreadRuntimeHandle<ViewClientActorInner>, tcp::ListenerAddr, Arc<tempfile::TempDir>) {
    start_all_with_validity_period(clock, actor_system, node_type, 100, false)
}

pub fn start_all_with_validity_period(
    clock: Clock,
    actor_system: ActorSystem,
    node_type: NodeType,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (MultithreadRuntimeHandle<ViewClientActorInner>, tcp::ListenerAddr, Arc<tempfile::TempDir>) {
    let actor_handles = setup_no_network_with_validity_period(
        clock,
        actor_system,
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
        actor_handles.client_actor.clone().into_multi_sender(),
        actor_handles.view_client_actor.clone().into_multi_sender(),
        actor_handles.rpc_handler_actor.clone().into_multi_sender(),
        noop().into_multi_sender(),
        #[cfg(feature = "test_features")]
        noop().into_multi_sender(),
        Arc::new(DummyEntityDebugHandler {}),
    );
    // setup_no_network_with_validity_period should use runtime_tempdir together with real runtime.
    (actor_handles.view_client_actor, addr, actor_handles.runtime_tempdir.unwrap())
}

#[macro_export]
macro_rules! test_with_client {
    ($node_type:expr, $client:ident, $block:expr) => {
        init_test_logger();

        let actor_system = near_async::ActorSystem::new();
        let (_view_client_addr, addr, _runtime_tempdir) =
            test_utils::start_all(near_time::Clock::real(), actor_system.clone(), $node_type);

        let $client = new_client(&format!("http://{}", addr));

        $block.await;
        actor_system.stop();
        near_store::db::rocksdb::RocksDB::block_until_all_instances_are_dropped();
    };
}

type RpcRequest<T> = LocalBoxFuture<'static, Result<T, near_jsonrpc_primitives::errors::RpcError>>;

/// Prepare a `RPCRequest` with a given client, server address, method and parameters.
pub fn call_method<R>(
    client: &reqwest::Client,
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

    let client = client.clone();
    let server_addr = server_addr.to_string();

    async move {
        let response = client
            .post(&server_addr)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|err| {
                near_jsonrpc_primitives::errors::RpcError::new_internal_error(
                    None,
                    format!("{:?}", err),
                )
            })?;

        let bytes = response.bytes().await.map_err(|err| {
            near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                "Failed to retrieve payload: {:?}",
                err
            ))
        })?;

        let message = from_slice(&bytes).map_err(|err| {
            near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                "Error {:?} in {:?}",
                err, bytes
            ))
        })?;

        match message {
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
        }
    }
    .boxed_local()
}
