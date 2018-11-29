extern crate client;
extern crate futures;
extern crate jsonrpc_core;
extern crate jsonrpc_http_server;
#[macro_use]
extern crate jsonrpc_macros;
extern crate network;
extern crate parking_lot;
extern crate primitives;
#[macro_use]
extern crate serde_derive;
extern crate tokio;
extern crate serde;

use client::Client;
use futures::future;
use network::protocol::ProtocolHandler;
use network::service::{generate_service_task, Service as NetworkService};
use network::test_utils::init_logger;
use primitives::traits::{Block, GenericResult, Header as BlockHeader};
use rpc::api::RpcImpl;
use std::sync::Arc;
use tokio::runtime;

pub mod config;
pub mod network_handler;
pub mod rpc;

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

    let mut background_thread = runtime::Runtime::new().unwrap();
    let mut current_thread = runtime::current_thread::Runtime::new().unwrap();
    background_thread.spawn(future::lazy(|| {
        server.wait();
        Ok(())
    }));
    Ok(current_thread.block_on(network_task).unwrap())
}
