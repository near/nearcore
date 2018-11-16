extern crate client;
extern crate jsonrpc_core;
#[macro_use]
extern crate jsonrpc_macros;
extern crate jsonrpc_minihttp_server;
extern crate primitives;

mod rpc;

use rpc::api::{RpcImpl, TransactionApi};

#[derive(Default)]
pub struct Service {
    rpc_impl: RpcImpl,
}

impl Service {
    pub fn run(&self) {
        let io = self.get_io();
        rpc::server::run_server(io);
    }

    fn get_io(&self) -> jsonrpc_core::IoHandler {
        let mut io = jsonrpc_core::IoHandler::new();
        io.extend_with(self.rpc_impl.to_delegate());
        io
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate jsonrpc_test;
    extern crate primitives;

    use self::jsonrpc_test::Rpc;
    use primitives::types::TransactionBody;

    #[test]
    fn test_call() {
        let service = Service::default();
        let io = service.get_io();
        let rpc = Rpc::from(io);
        let t = TransactionBody {
            nonce: 0,
            sender: 1,
            receiver: 0,
            amount: 0,
        };
        assert_eq!(rpc.request("receive_transaction", &[t]), "null");
    }
}
