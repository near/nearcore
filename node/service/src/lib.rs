extern crate client;
extern crate futures;
extern crate jsonrpc_core;
#[macro_use]
extern crate jsonrpc_macros;
extern crate jsonrpc_minihttp_server;
extern crate network;
extern crate parking_lot;
extern crate primitives;
extern crate storage;
extern crate tokio;

use client::Client;
use futures::future;
use network::protocol::ProtocolHandler;
use network::service::{generate_service_task, Service as NetworkService};
use network::test_utils::init_logger;
<<<<<<< 557b54efff24836676c6c041836779029c548d5a
use primitives::traits::{Block, Header as BlockHeader};
use rpc::api::RpcImpl;
use std::sync::Arc;

pub fn run_service<B: Block, H: ProtocolHandler, Header: BlockHeader>(
    client: Arc<Client>,
    network: &NetworkService<B, H>,
) {
=======
use primitives::traits::Block;
use primitives::traits::GenericResult;
use rpc::api::RpcImpl;
use std::sync::Arc;

pub mod config;
pub mod network_handler;
mod rpc;

pub fn run_service<B: Block, T: Transaction, H: ProtocolHandler<T>>(
    client: Arc<Client>,
    network: &NetworkService<B, T, H>,
) -> GenericResult {
>>>>>>> cli: add chain spec arg, add default chain spec
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
