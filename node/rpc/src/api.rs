use futures::sync::mpsc::Sender;
use jsonrpc_core::{IoHandler, Result as JsonRpcResult};
use jsonrpc_core::types::{Error, ErrorCode, Value};

use node_runtime::state_viewer::StateDbViewer;
use primitives::traits::Encode;
use primitives::utils::bs58_vec2str;
use primitives::types::{
    CreateAccountTransaction, DeployContractTransaction,
    FunctionCallTransaction, SendMoneyTransaction, SignedTransaction,
    StakeTransaction, SwapKeyTransaction, TransactionBody,
};
use types::{
    CallViewFunctionRequest, CallViewFunctionResponse,
    CreateAccountRequest, DeployContractRequest,
    PreparedTransactionBodyResponse, ScheduleFunctionCallRequest, SendMoneyRequest,
    StakeRequest, SwapKeyRequest, ViewAccountRequest,
    ViewAccountResponse, ViewStateRequest,
    ViewStateResponse,
};

build_rpc_trait! {
    pub trait TransactionApi {
        /// Receive new transaction.
        #[rpc(name = "send_money")]
        fn rpc_send_money(
            &self,
            SendMoneyRequest
        ) -> JsonRpcResult<(PreparedTransactionBodyResponse)>;

        /// Receive new transaction.
        #[rpc(name = "stake")]
        fn rpc_stake(
            &self,
            StakeRequest
        ) -> JsonRpcResult<(PreparedTransactionBodyResponse)>;

        /// Deploy smart contract.
        #[rpc(name = "deploy_contract")]
        fn rpc_deploy_contract(
            &self,
            DeployContractRequest
        ) -> JsonRpcResult<(PreparedTransactionBodyResponse)>;

        /// Create account.
        #[rpc(name = "create_account")]
        fn rpc_create_account(
            &self,
            CreateAccountRequest
        ) -> JsonRpcResult<(PreparedTransactionBodyResponse)>;

        /// swap keys for account.
        #[rpc(name = "swap_key")]
        fn rpc_swap_key(
            &self,
            SwapKeyRequest
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

        /// View contract state.
        #[rpc(name = "view_state")]
        fn rpc_view_state(
            &self,
            ViewStateRequest
        ) -> JsonRpcResult<ViewStateResponse>;
    }
}

pub struct RpcImpl {
    state_db_viewer: StateDbViewer,
    submit_txn_sender: Sender<SignedTransaction>,
}

impl RpcImpl {
    pub fn new(
        state_db_viewer: StateDbViewer,
        submit_txn_sender: Sender<SignedTransaction>,
    ) -> Self {
        RpcImpl {
            state_db_viewer,
            submit_txn_sender,
        }
    }
}

impl TransactionApi for RpcImpl {
    fn rpc_create_account(
        &self,
        r: CreateAccountRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        let body = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: r.nonce,
            sender: r.sender,
            new_account_id: r.new_account_id,
            amount: r.amount,
            public_key: r.public_key.encode().unwrap(),
        });
        debug!(target: "near-rpc", "Create account transaction {:?}", r.new_account_id);
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_deploy_contract(
        &self,
        r: DeployContractRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        let body = TransactionBody::DeployContract(DeployContractTransaction {
            nonce: r.nonce,
            sender: r.sender_account_id,
            contract_id: r.contract_account_id,
            wasm_byte_array: r.wasm_byte_array,
            public_key: r.public_key.encode().unwrap(),
        });
        debug!(target: "near-rpc", "Deploy contract transaction {:?}", r.contract_account_id);
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_swap_key(
        &self,
        r: SwapKeyRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        let body = TransactionBody::SwapKey(SwapKeyTransaction {
            nonce: r.nonce,
            sender: r.account,
            cur_key: r.current_key.encode().unwrap(),
            new_key: r.new_key.encode().unwrap(),
        });
        debug!(target: "near-rpc", "Swap key transaction {:?}", r.account);
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_send_money(
        &self,
        r: SendMoneyRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        let body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: r.nonce,
            sender: r.sender_account_id,
            receiver: r.receiver_account_id,
            amount: r.amount,
        });
        debug!(target: "near-rpc", "Send money transaction {:?}->{:?}, amount: {:?}",
               r.sender_account_id, r.receiver_account_id, r.amount);
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_stake(
        &self,
        r: StakeRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        let body = TransactionBody::Stake(StakeTransaction {
            nonce: r.nonce,
            staker: r.staker_account_id,
            amount: r.amount,
        });
        debug!(target: "near-rpc", "Stake money transaction {:?}, amount: {:?}",
               r.staker_account_id, r.amount);
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_schedule_function_call(
        &self,
        r: ScheduleFunctionCallRequest,
    ) -> JsonRpcResult<(PreparedTransactionBodyResponse)> {
        debug!(target: "near-rpc", "Schedule function call transaction {:?}.{:?}",
               r.contract_account_id, r.method_name);
        let body = TransactionBody::FunctionCall(FunctionCallTransaction {
            nonce: r.nonce,
            originator: r.originator_account_id,
            contract_id: r.contract_account_id,
            method_name: r.method_name.into_bytes(),
            args: r.args,
            amount: r.amount,
        });
        Ok(PreparedTransactionBodyResponse { body })
    }

    fn rpc_view_account(&self, r: ViewAccountRequest) -> JsonRpcResult<ViewAccountResponse> {
        debug!(target: "near-rpc", "View account {:?}", r.account_id);
        match self.state_db_viewer.view_account(r.account_id) {
            Ok(r) => {
                Ok(ViewAccountResponse {
                    account_id: r.account,
                    amount: r.amount,
                    stake: r.stake,
                    code_hash: r.code_hash,
                    nonce: r.nonce,
                })
            }
            Err(e) => {
                let mut error = Error::new(ErrorCode::InvalidRequest);
                error.data = Some(Value::String(e.to_string()));
                Err(error)
            }
        }
    }

    fn rpc_call_view_function(
        &self,
        r: CallViewFunctionRequest,
    ) -> JsonRpcResult<(CallViewFunctionResponse)> {
        debug!(target: "near-rpc", "Call view function {:?}{:?}", r.contract_account_id, r.method_name);
        match self.state_db_viewer.call_function(
            r.originator_id,
            r.contract_account_id,
            &r.method_name,
            &r.args,
        ) {
            Ok(result) => {
                Ok(CallViewFunctionResponse { result })
            }
            Err(e) => {
                let mut error = Error::new(ErrorCode::InvalidRequest);
                error.data = Some(Value::String(e.to_string()));
                Err(error)
            }
        }
    }

    fn rpc_submit_transaction(&self, r: SignedTransaction) -> JsonRpcResult<()> {
        debug!(target: "near-rpc", "Received transaction {:?}", r);
        self.submit_txn_sender.clone().try_send(r).unwrap();
        Ok(())
    }

    fn rpc_view_state(&self, r: ViewStateRequest) -> JsonRpcResult<ViewStateResponse> {
        debug!(target: "near-rpc", "View state {:?}", r.contract_account_id);
        let result = self.state_db_viewer.view_state(r.contract_account_id);
        let response = ViewStateResponse {
            contract_account_id: r.contract_account_id,
            values: result.values.iter().map(|(k, v)| (bs58_vec2str(k), v.clone())).collect()
        };
        Ok(response)
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

    use node_runtime::test_utils::get_test_state_db_viewer;
    use primitives::hash::hash;

    use super::*;

    use self::jsonrpc_test::Rpc;

    #[test]
    fn test_call() {
        let db_state_viewer = get_test_state_db_viewer();
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
            body: TransactionBody::SendMoney(SendMoneyTransaction {
                nonce: 0,
                sender: hash(b"alice"),
                receiver: hash(b"bob"),
                amount: 1,
            }),
        };
        let raw = &rpc.request("send_money", &[t]);
        let response = serde_json::from_str(&raw).unwrap();
        assert_eq!(expected, response);
    }
}
