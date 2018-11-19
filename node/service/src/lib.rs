extern crate client;
extern crate jsonrpc_core;
#[macro_use]
extern crate jsonrpc_macros;
extern crate jsonrpc_minihttp_server;
extern crate primitives;
extern crate storage;

pub mod config;
mod rpc;

use client::Client;

pub struct Service {
    client: Client,
}

impl Service {
    pub fn new(client: Client) -> Self {
        Service { client }
    }
    pub fn run(self) {
        let handler = rpc::api::get_handler(self.client);
        rpc::server::run_server(handler);
    }
}
