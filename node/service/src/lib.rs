extern crate client;
extern crate futures;
extern crate jsonrpc_core;
#[macro_use]
extern crate jsonrpc_macros;
extern crate jsonrpc_minihttp_server;
extern crate network;
extern crate parking_lot;
extern crate primitives;
extern crate tokio;

pub mod config;
pub mod network_handler;
pub mod rpc;

use futures::future;
use std::sync::Arc;

use client::Client;
use network::protocol::ProtocolHandler;
use network::service::{generate_service_task, Service as NetworkService};
use network::test_utils::init_logger;
use primitives::traits::{Block, GenericResult, Header as BlockHeader};
use rpc::api::RpcImpl;

pub fn run_service<B: Block, H: ProtocolHandler, Header: BlockHeader>(
    client: Arc<Client>,
    network: &NetworkService<B, H>,
) -> GenericResult {
    init_logger(true);
    let network_task =
        generate_service_task::<B, H, Header>(network.network.clone(), network.protocol.clone());

    let rpc_impl = RpcImpl { client };
    let rpc_handler = rpc::api::get_handler(rpc_impl);
    let server = rpc::server::get_server(rpc_handler);
    let task = future::lazy(|| {
        tokio::spawn(network_task);
        tokio::spawn(future::lazy(|| {
            server.wait().unwrap();
            Ok(())
        }));
        Ok(())
    });
    Ok(tokio::run(task))
}
