use futures::sync::mpsc::Sender;
use jsonrpc_core::{IoHandler, Result as JsonRpcResult, Error, ErrorCode};
use node_runtime::StateDbViewer;
use primitives::types::{SignedTransaction, TransactionBody, ViewCall, StructSignature,};
use primitives::hash::CryptoHash;
use primitives::signature::{PublicKey, SecretKey, get_keypair, sign};
use primitives::utils::concat;
use types::{
    CallViewFunctionRequest, CallViewFunctionResponse,
    DeployContractRequest, PreparedTransactionBodyResponse,
    ScheduleFunctionCallRequest, SendMoneyRequest,
    ViewAccountRequest, ViewAccountResponse,
};

build_rpc_trait! {
    pub trait TransactionApi {
        /// Receive new transaction.
        #[rpc(name = "send_money")]
        fn rpc_send_money(
            &self,
            SendMoneyRequest
        ) -> JsonRpcResult<(PreparedTransactionBodyResponse)>;

        /// Deploy smart contract.
        #[rpc(name = "deploy_contract")]
        fn rpc_deploy_contract(
            &self,
            DeployContractRequest
        ) -> JsonRpcResult<(PreparedTransactionBodyResponse)>;

        /// Call method on smart contract.
        #[rpc(name = "schedule_function_call")]
        fn rpc_schedule_function_call(
            &self,
            ScheduleFunctionCallRequest
        ) -> JsonRpcResult<(PreparedTransactionBodyResponse)>;

        /// Call view function on smart contract.
        #[rpc(name = "call_view_function")]
        fn rpc_call_view_function(
            &self,
            CallViewFunctionRequest
        ) -> JsonRpcResult<(CallViewFunctionResponse)>;

        /// View account.
        #[rpc(name = "view_account")]
        fn rpc_view_account(
            &self,
            ViewAccountRequest
        ) -> JsonRpcResult<ViewAccountResponse>;

        /// Submit transaction.
        #[rpc(name = "submit_transaction")]
        fn rpc_submit_transaction(
            &self,
            SignedTransaction
        ) -> JsonRpcResult<()>;
    }
}

pub struct RpcImpl {
    state_db_viewer: StateDbViewer,
    submit_txn_sender: Sender<SignedTransaction>,
    // TODO: This is a temporary solution. We should be using a real signer to sign it.
    // Instead we temporarily sign the transactions here.
    keypair: (PublicKey, SecretKey),
}

impl RpcImpl {
    pub fn new(
        state_db_viewer: StateDbViewer,
        submit_txn_sender: Sender<SignedTransaction>,
    ) -> Self {
        RpcImpl {
            state_db_viewer,
            submit_txn_sender,
            keypair: get_keypair(),
        }
    }

    // TOOD: Remove once we have a proper signer.
    fn sign_hash(&self, hash: &CryptoHash) -> StructSignature {
        sign(hash.as_ref(), &self.keypair.1)
    }
}

impl TransactionApi for RpcImpl {
    fn rpc_deploy_contract(
        &self,
        r: DeployContractRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        let body = TransactionBody {
            nonce: r.nonce,
            sender: r.sender_account_id,
            receiver: r.contract_account_id,
            amount: 0,
            method_name: "deploy".into(),
            args: r.wasm_byte_array,
        };
        let transaction = SignedTransaction::new(body.clone(), |h| self.sign_hash(h));
        self.submit_txn_sender.clone().try_send(transaction).map_err(
            |e| {
                error!("Error deploying contract {:?}", e);
                Error::new(ErrorCode::InternalError)
            })?;
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_send_money(
        &self,
        r: SendMoneyRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        let body = TransactionBody {
            nonce: r.nonce,
            sender: r.sender_account_id,
            receiver: r.receiver_account_id,
            amount: r.amount,
            method_name: vec![],
            args: Vec::new(),
        };
        let transaction = SignedTransaction::new(body.clone(), |h| self.sign_hash(h));
        self.submit_txn_sender.clone().try_send(transaction).map_err(
            |e| {
                error!("Error sending money {:?}", e);
                Error::new(ErrorCode::InternalError)
            })?;
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_schedule_function_call(
        &self,
        r: ScheduleFunctionCallRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        let body = TransactionBody {
            nonce: r.nonce,
            sender: r.sender_account_id,
            receiver: r.contract_account_id,
            amount: 0,
            method_name: r.method_name.into_bytes(),
            args: concat(r.args),
        };
        let transaction = SignedTransaction::new(body.clone(), |h| self.sign_hash(h));
        self.submit_txn_sender.clone().try_send(transaction).map_err(
            |e| {
                error!("Error scheduling function call {:?}", e);
                Error::new(ErrorCode::InternalError)
            })?;
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_view_account(&self, r: ViewAccountRequest) -> JsonRpcResult<ViewAccountResponse> {
        let call = ViewCall { account: r.account_id, method_name: String::new(), args: Vec::new() };
        let result = self.state_db_viewer.view(&call);
        let response = ViewAccountResponse {
            account_id: result.account,
            amount: result.amount,
            nonce: result.nonce,
        };
        Ok(response)
    }

    fn rpc_call_view_function(
        &self,
        r: CallViewFunctionRequest,
    ) -> JsonRpcResult<(CallViewFunctionResponse)> {
        let call =
            ViewCall { account: r.contract_account_id, method_name: r.method_name, args: r.args };
        let result = self.state_db_viewer.view(&call);
        let response = CallViewFunctionResponse {
            account_id: result.account,
            amount: result.amount,
            nonce: result.nonce,
            result: result.result,
        };
        Ok(response)
    }

    fn rpc_submit_transaction(&self, r: SignedTransaction) -> JsonRpcResult<()> {
        self.submit_txn_sender.clone().try_send(r).map_err(
            |e| {
                error!("Error submitting transaction {:?}", e);
                Error::new(ErrorCode::InternalError)
            })?;
        Ok(())
    }
}

pub fn get_handler(rpc_impl: RpcImpl) -> IoHandler {
    let mut io = IoHandler::new();
    io.extend_with(rpc_impl.to_delegate());
    io
}

#[cfg(test)]
mod tests {
    extern crate jsonrpc_test;
    extern crate serde_json;

    use futures::sync::mpsc::channel;
    use primitives::hash::hash;
    use self::jsonrpc_test::Rpc;
    use super::*;
    use node_runtime::test_utils::get_test_state_db_viewer;

    #[test]
    #[ignore]
    fn test_call() {
        let db_state_viewer= get_test_state_db_viewer();
        let (submit_txn_sender, _) = channel(1024);
        let rpc_impl = RpcImpl::new(db_state_viewer, submit_txn_sender);
        let handler = get_handler(rpc_impl);
        let rpc = Rpc::from(handler);
        let t = SendMoneyRequest {
            nonce: 0,
            sender_account_id: hash(b"alice"),
            receiver_account_id: hash(b"bob"),
            amount: 1,
        };
        let expected = PreparedTransactionBodyResponse {
            body: TransactionBody {
                nonce: 0,
                sender: hash(b"alice"),
                receiver: hash(b"bob"),
                amount: 1,
                method_name: vec![],
                args: Vec::new(),
            },
        };
        let raw = &rpc.request("send_money", &[t]);
        let response = serde_json::from_str(&raw).unwrap();
        assert_eq!(expected, response);
    }
}
