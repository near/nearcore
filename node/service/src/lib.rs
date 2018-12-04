extern crate client;
extern crate env_logger;
extern crate futures;
extern crate jsonrpc_core;
extern crate jsonrpc_http_server;
#[macro_use]
extern crate jsonrpc_macros;
#[macro_use]
extern crate log;
extern crate network;
extern crate parking_lot;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

use client::Client;
use env_logger::Builder;
use futures::{future, Future};
use primitives::traits::GenericResult;
use produce_blocks::generate_produce_blocks_task;
use rpc::api::RpcImpl;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime;
use futures::sync::mpsc::channel;
use futures::stream::Stream;

pub mod network_handler;
mod produce_blocks;
pub mod rpc;
#[cfg(feature = "test-utils")]
pub mod test_utils;

pub fn run_service(
    client: &Arc<Client>,
    network_task: impl Future<Item=(), Error=()>,
    produce_blocks_interval: Duration,
) -> GenericResult {
    let mut builder = Builder::new();
    builder.filter(Some("runtime"), log::LevelFilter::Debug);
    builder.filter(Some("service"), log::LevelFilter::Debug);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();

    let (submit_txn_tx, submit_txn_rx) = channel(1024);
    let rpc_impl = RpcImpl::new(client.clone(), submit_txn_tx);
    let rpc_handler = rpc::api::get_handler(rpc_impl);
    let server = rpc::server::get_server(rpc_handler);

    let mut background_thread = runtime::Runtime::new().unwrap();
    background_thread.spawn(future::lazy(|| {
        server.wait();
        Ok(())
    }));

    let submit_txn_task = submit_txn_rx.fold(client.clone(), |client, t| {
        client.receive_transaction(t);
        future::ok(client.clone())
    }).map(|_| ());
    background_thread.spawn(submit_txn_task);

    let mut current_thread = runtime::current_thread::Runtime::new().unwrap();
    let produce_blocks_task = generate_produce_blocks_task(
        &client,
        produce_blocks_interval,
    );
    let tasks: Vec<Box<Future<Item=(), Error=()>>> = vec![
        Box::new(network_task),
        Box::new(produce_blocks_task),
    ];
    let task = futures::select_all(tasks)
        .and_then(move |_| {
            info!("Service task failed");
            Ok(())
        }).map_err(|(r, _, _)| r)
        .map_err(|e| {
            debug!(target: "service", "service error: {:?}", e);
        });
    Ok(current_thread.block_on(task).unwrap())
}
