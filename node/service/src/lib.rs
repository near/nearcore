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

pub mod config;
pub mod network_handler;
mod rpc;

use client::Client;
use futures::future;
use network::protocol::{ProtocolHandler, Transaction};
use network::service::{generate_service_task, Service as NetworkService};
use network::test_utils::init_logger;
use primitives::traits::Block;
use rpc::api::RpcImpl;
use std::sync::Arc;

pub fn run_service<B: Block, T: Transaction, H: ProtocolHandler>(
    client: Arc<Client>,
    network: &NetworkService<B, H>,
) {
    init_logger(true);
    let network_task =
        generate_service_task::<B, T, H>(network.network.clone(), network.protocol.clone());

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
    tokio::run(task);
}
