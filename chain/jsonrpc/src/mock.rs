use actix::Addr;
use actix_web::web;
use near_chain_configs::GenesisConfig;
use near_client::{ClientActor, ViewClientActor};
use near_jsonrpc_primitives::errors::RpcError;
use serde_json::Value;

use crate::{JsonRpcHandler, RpcConfig};

#[derive(Clone)]
pub struct MockHttpServer {
    handler: web::Data<JsonRpcHandler>,
}

impl MockHttpServer {
    pub async fn status(
        &self,
    ) -> Result<
        near_jsonrpc_primitives::types::status::RpcStatusResponse,
        near_jsonrpc_primitives::types::status::RpcStatusError,
    > {
        self.handler.status().await
    }

    // TODO: make this a function that lets you make a generic RPC call. Right now
    // we're only using this one, so just implement get_block() as a proof of concept for now.
    pub async fn get_latest_block(&self) -> Result<Value, RpcError> {
        let req = serde_json::from_str(
            "{\"jsonrpc\": \"2.0\", \"method\": \"block\", \
        \"params\": {\"finality\": \"optimistic\"}, \"id\": \"dontcare\"}",
        )
        .unwrap();
        self.handler.process_request(req).await
    }
}

pub fn start_http(
    config: RpcConfig,
    genesis_config: GenesisConfig,
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
    #[cfg(feature = "test_features")] peer_manager_addr: Addr<near_network::PeerManagerActor>,
    #[cfg(feature = "test_features")] routing_table_addr: Addr<near_network::RoutingTableActor>,
) -> MockHttpServer {
    let RpcConfig { polling_config, enable_debug_rpc, .. } = config;
    let handler = web::Data::new(JsonRpcHandler {
        client_addr,
        view_client_addr,
        polling_config,
        genesis_config,
        enable_debug_rpc,
        #[cfg(feature = "test_features")]
        peer_manager_addr: peer_manager_addr,
        #[cfg(feature = "test_features")]
        routing_table_addr: routing_table_addr,
    });
    MockHttpServer { handler }
}
