extern crate client;
extern crate jsonrpc_core;
#[macro_use]
extern crate jsonrpc_macros;
extern crate jsonrpc_minihttp_server;
extern crate primitives;

mod rpc;
pub mod config;

use client::Client;

pub struct Service {
    client: Client,
}

impl Service {
    pub fn new(client: Client) -> Self {
        Service {
            client
        }
    }
    pub fn run(self) {
        let handler = rpc::api::get_handler(self.client);
        rpc::server::run_server(handler);
    }
}
