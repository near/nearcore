extern crate client;
extern crate jsonrpc_core;
#[macro_use]
extern crate jsonrpc_macros;
extern crate jsonrpc_minihttp_server;
extern crate primitives;

mod rpc;

use client::Client;

#[derive(Default)]
pub struct Service {
    client: Client,
}

impl Service {
    pub fn run(self) {
        let handler = rpc::api::get_handler(self.client);
        rpc::server::run_server(handler);
    }
}
