use std::sync::Arc;

use jsonrpc_core::{IoHandler, Result as JsonRpcResult};

use client::Client;
use primitives::types::{SignedTransaction, TransactionBody, ViewCall, ViewCallResult};

build_rpc_trait! {
    pub trait TransactionApi {
        /// Receive new transaction.
        #[rpc(name = "receive_transaction")]
        fn rpc_receive_transaction(&self, TransactionBody) -> JsonRpcResult<()>;
        /// Call view function.
        #[rpc(name = "view")]
        fn rpc_view(&self, ViewCall) -> JsonRpcResult<ViewCallResult>;
    }
}

pub struct RpcImpl {
    pub client: Arc<Client>,
}

impl TransactionApi for RpcImpl {
    fn rpc_receive_transaction(&self, t: TransactionBody) -> JsonRpcResult<()> {
        let transaction = SignedTransaction::new(123, t);
        Ok(self.client.receive_transaction(transaction))
    }

    fn rpc_view(&self, v: ViewCall) -> JsonRpcResult<ViewCallResult> {
        Ok(self.client.view_call(&v))
    }
}

pub fn get_handler(rpc_impl: RpcImpl) -> IoHandler {
    let mut io = IoHandler::new();
    io.extend_with(rpc_impl.to_delegate());
    io
}

#[cfg(test)]
mod tests {
    use client::test_utils::generate_test_client;
    use primitives::hash::hash;
    use primitives::types::TransactionBody;

    use super::*;

    use self::jsonrpc_test::Rpc;

    extern crate jsonrpc_test;
    extern crate primitives;

    #[test]
    fn test_call() {
        let client = Arc::new(generate_test_client());
        let rpc_impl = RpcImpl { client };
        let handler = get_handler(rpc_impl);
        let rpc = Rpc::from(handler);
        let t = TransactionBody {
            nonce: 0,
            sender: hash(b"bob"),
            receiver: hash(b"alice"),
            amount: 0,
            method_name: String::new(),
            args: vec![],
        };
        assert_eq!(rpc.request("receive_transaction", &[t]), "null");
    }
}
