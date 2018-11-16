use jsonrpc_core::Result as JsonRpcResult;

use client::Client;
use primitives::types::TransactionBody;

build_rpc_trait! {
    pub trait TransactionApi {
        /// Receive new transaction
        #[rpc(name = "receive_transaction")]
        fn rpc_receive_transaction(&self, TransactionBody) -> JsonRpcResult<()>;
    }
}

#[derive(Default, Copy, Clone)]
pub struct RpcImpl {
    client: Client,
}

impl TransactionApi for RpcImpl {
    fn rpc_receive_transaction(&self, t: TransactionBody) -> JsonRpcResult<()> {
        Ok(self.client.receive_transaction(&t))
    }
}
