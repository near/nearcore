extern crate client;
extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate log;
extern crate network;
extern crate node_cli;
extern crate primitives;
extern crate service;
extern crate storage;
extern crate tokio;

use std::sync::Arc;
use std::time::Duration;

use env_logger::Builder;
use futures::{future, Future, Stream};
use tokio::timer::Interval;

use client::Client;
use network::client::Client as NetworkClient;
use node_cli::chain_spec::get_default_chain_spec;
use primitives::signer::InMemorySigner;
use service::rpc::api::{get_handler, RpcImpl};
use service::rpc::server::get_server;

const BLOCK_PROD_PERIOD: Duration = Duration::from_secs(2);

fn main() {
    let mut builder = Builder::new();
    builder.filter(Some("runtime"), log::LevelFilter::Debug);
    builder.filter(Some("main"), log::LevelFilter::Debug);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();

    let storage = Arc::new(storage::test_utils::create_memory_db());
    let signer = Arc::new(InMemorySigner::new());
    let chain_spec = get_default_chain_spec().unwrap();
    let client = Arc::new(Client::new(&chain_spec, storage, signer));
    let rpc_impl = RpcImpl { client: client.clone() };
    let rpc_handler = get_handler(rpc_impl);
    let server = get_server(rpc_handler);

    let block_prod_task = Interval::new_interval(BLOCK_PROD_PERIOD)
        .for_each({
            let client = client.clone();
            move |_| {
                let block = client.prod_block();
                if !block.transactions.is_empty() {
                    info!(target: "main", "Transactions: {:?}", block.transactions);
                }
                Ok(())
            }
        }).map_err(|_| ());

    let task = future::lazy(|| {
        tokio::spawn(block_prod_task);
        tokio::spawn(future::lazy(|| {
            server.wait();
            Ok(())
        }));
        Ok(())
    });
    tokio::run(task);
}
