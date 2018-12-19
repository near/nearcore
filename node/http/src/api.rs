use futures::sync::mpsc::Sender;

use node_runtime::state_viewer::StateDbViewer;
use primitives::traits::Encode;
use primitives::types::{
    CreateAccountTransaction, DeployContractTransaction,
    FunctionCallTransaction, SendMoneyTransaction, SignedTransaction,
    StakeTransaction, SwapKeyTransaction, TransactionBody,
};
use primitives::utils::bs58_vec2str;
use types::{
    CallViewFunctionRequest, CallViewFunctionResponse,
    CreateAccountRequest, DeployContractRequest,
    PreparedTransactionBodyResponse, ScheduleFunctionCallRequest, SendMoneyRequest,
    StakeRequest, SwapKeyRequest, ViewAccountRequest,
    ViewAccountResponse, ViewStateRequest,
    ViewStateResponse,
};

pub struct HttpApi {
    state_db_viewer: StateDbViewer,
    submit_txn_sender: Sender<SignedTransaction>,
}

impl HttpApi {
    pub fn new(
        state_db_viewer: StateDbViewer,
        submit_txn_sender: Sender<SignedTransaction>,
    ) -> HttpApi {
        HttpApi {
            state_db_viewer,
            submit_txn_sender,
        }
    }
}

impl HttpApi {
    pub fn create_account(
        &self,
        r: &CreateAccountRequest,
    ) -> Result<PreparedTransactionBodyResponse, ()> {
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

    pub fn deploy_contract(
        &self,
        r: DeployContractRequest,
    ) -> Result<PreparedTransactionBodyResponse, ()> {
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

    pub fn swap_key(
        &self,
        r: &SwapKeyRequest,
    ) -> Result<PreparedTransactionBodyResponse, ()> {
        let body = TransactionBody::SwapKey(SwapKeyTransaction {
            nonce: r.nonce,
            sender: r.account,
            cur_key: r.current_key.encode().unwrap(),
            new_key: r.new_key.encode().unwrap(),
        });
        debug!(target: "near-rpc", "Swap key transaction {:?}", r.account);
        Ok(PreparedTransactionBodyResponse { body })
    }

    pub fn send_money(
        &self,
        r: &SendMoneyRequest,
    ) -> Result<PreparedTransactionBodyResponse, ()> {
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

    pub fn stake(
        &self,
        r: &StakeRequest,
    ) -> Result<PreparedTransactionBodyResponse, ()> {
        let body = TransactionBody::Stake(StakeTransaction {
            nonce: r.nonce,
            staker: r.staker_account_id,
            amount: r.amount,
        });
        debug!(target: "near-rpc", "Stake money transaction {:?}, amount: {:?}",
               r.staker_account_id, r.amount);
        Ok(PreparedTransactionBodyResponse { body })
    }

    pub fn schedule_function_call(
        &self,
        r: ScheduleFunctionCallRequest,
    ) -> Result<PreparedTransactionBodyResponse, ()> {
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

    pub fn view_account(
        &self,
        r: &ViewAccountRequest,
    ) -> Result<ViewAccountResponse, String> {
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
            Err(e) => { Err(e.to_string()) }
        }
    }

    pub fn call_view_function(
        &self,
        r: &CallViewFunctionRequest,
    ) -> Result<CallViewFunctionResponse, String> {
        debug!(
            target: "near-rpc",
            "Call view function {:?}{:?}",
            r.contract_account_id,
            r.method_name,
        );
        match self.state_db_viewer.call_function(
            r.originator_id,
            r.contract_account_id,
            &r.method_name,
            &r.args,
        ) {
            Ok(result) => {
                Ok(CallViewFunctionResponse { result })
            }
            Err(e) => { Err(e.to_string()) }
        }
    }

    pub fn submit_transaction(&self, r: SignedTransaction) -> Result<(), &str> {
        debug!(target: "near-rpc", "Received transaction {:?}", r);
        self.submit_txn_sender.clone().try_send(r).map_err(|_| {
            "transaction channel is full"
        })
    }

    pub fn view_state(&self, r: &ViewStateRequest) -> Result<ViewStateResponse, ()> {
        debug!(target: "near-rpc", "View state {:?}", r.contract_account_id);
        let result = self.state_db_viewer.view_state(r.contract_account_id);
        let response = ViewStateResponse {
            contract_account_id: r.contract_account_id,
            values: result.values.iter().map(|(k, v)| (bs58_vec2str(k), v.clone())).collect()
        };
        Ok(response)
    }
}
