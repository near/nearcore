extern crate client;
extern crate env_logger;
extern crate futures;
extern crate jsonrpc_core;
extern crate jsonrpc_http_server;
#[macro_use]
extern crate jsonrpc_macros;
extern crate network;
extern crate parking_lot;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

mod api;
mod server;
mod types;


use client::Client;
use env_logger::Builder;
use futures::{future, Future};
use api::RpcImpl;
use std::sync::Arc;


/// A task that handles RPC using the client.
pub fn create_rpc_task(client: Arc<Client>) -> Box<Future<Item=(), Error=()> + Send> {
    let res = future::lazy(|| {
        let mut builder = Builder::new();
        builder.filter(Some("runtime"), log::LevelFilter::Debug);
        builder.filter(Some("service"), log::LevelFilter::Debug);
        builder.filter(None, log::LevelFilter::Info);
        builder.init();

        let rpc_impl = RpcImpl { client };
        let rpc_handler = api::get_handler(rpc_impl);
        let server = server::get_server(rpc_handler);
        server.wait();
        Ok(())
    });
    Box::new(res)
}
