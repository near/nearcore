extern crate client;
extern crate futures;
extern crate network;
extern crate service;
extern crate storage;
extern crate tokio;

use client::Client;
use network::client::Client as NetworkClient;
use service::rpc::api::{get_handler, RpcImpl};
use service::rpc::server::get_server;
use storage::MemoryStorage;

use futures::{future, Future, Stream};
use std::sync::Arc;
use std::time::Duration;
use tokio::timer::Interval;

const BLOCK_PROD_PERIOD: Duration = Duration::from_secs(2);

fn main() {
    let storage = Arc::new(MemoryStorage::new());
    let client = Arc::new(Client::new(storage.clone()));
    let rpc_impl = RpcImpl {
        client: client.clone(),
    };
    let rpc_handler = get_handler(rpc_impl);
    let server = get_server(rpc_handler);

    let block_prod_task = Interval::new_interval(BLOCK_PROD_PERIOD)
        .for_each({
            let client = client.clone();
            move |_| {
                let block = client.prod_block();
                println!("Block produced: {:?}", block);
                Ok(())
            }
        }).map_err(|_| ());

    let task = future::lazy(|| {
        tokio::spawn(block_prod_task);
        tokio::spawn(future::lazy(|| {
            server.wait().unwrap();
            Ok(())
        }));
        Ok(())
    });
    tokio::run(task);
}
