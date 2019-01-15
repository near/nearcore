use std::sync::Arc;

use futures::sync::mpsc::Sender;

use beacon::types::BeaconBlockChain;
use node_runtime::state_viewer::StateDbViewer;
use primitives::traits::Encode;
use primitives::types::{
    BlockId, CreateAccountTransaction, DeployContractTransaction,
    FunctionCallTransaction, SendMoneyTransaction, SignedTransaction,
    StakeTransaction, SwapKeyTransaction, TransactionBody,
};
use primitives::utils::bs58_vec2str;
use shard::ShardBlockChain;
use crate::types::{
    CallViewFunctionRequest, CallViewFunctionResponse,
    CreateAccountRequest, DeployContractRequest, GetBlockByHashRequest,
    GetBlocksByIndexRequest, PreparedTransactionBodyResponse, ScheduleFunctionCallRequest,
    SendMoneyRequest, SignedBeaconBlockResponse, SignedShardBlockResponse,
    SignedShardBlocksResponse, StakeRequest, SwapKeyRequest, ViewAccountRequest,
    ViewAccountResponse, ViewStateRequest, ViewStateResponse,
};

pub struct HttpApi {
    state_db_viewer: StateDbViewer,
    submit_txn_sender: Sender<SignedTransaction>,
    beacon_chain: Arc<BeaconBlockChain>,
    shard_chain: Arc<ShardBlockChain>,
}

impl HttpApi {
    pub fn new(
        state_db_viewer: StateDbViewer,
        submit_txn_sender: Sender<SignedTransaction>,
        beacon_chain: Arc<BeaconBlockChain>,
        shard_chain: Arc<ShardBlockChain>,
    ) -> HttpApi {
        HttpApi {
            state_db_viewer,
            submit_txn_sender,
            beacon_chain,
            shard_chain,
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
            originator: r.originator.clone(),
            new_account_id: r.new_account_id.clone(),
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
            originator: r.originator.clone(),
            contract_id: r.contract_account_id.clone(),
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
            originator: r.account.clone(),
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
            originator: r.originator.clone(),
            receiver: r.receiver_account_id.clone(),
            amount: r.amount,
        });
        debug!(target: "near-rpc", "Send money transaction {:?}->{:?}, amount: {:?}",
               r.originator, r.receiver_account_id, r.amount);
        Ok(PreparedTransactionBodyResponse { body })
    }

    pub fn stake(
        &self,
        r: &StakeRequest,
    ) -> Result<PreparedTransactionBodyResponse, ()> {
        let body = TransactionBody::Stake(StakeTransaction {
            nonce: r.nonce,
            originator: r.originator.clone(),
            amount: r.amount,
        });
        debug!(target: "near-rpc", "Stake money transaction {:?}, amount: {:?}",
               r.originator, r.amount);
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
            originator: r.originator.clone(),
            contract_id: r.contract_account_id.clone(),
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
        match self.state_db_viewer.view_account(&r.account_id) {
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
            &r.originator,
            &r.contract_account_id,
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
        let result = self.state_db_viewer.view_state(&r.contract_account_id);
        let response = ViewStateResponse {
            contract_account_id: r.contract_account_id.clone(),
            values: result.values.iter().map(|(k, v)| (bs58_vec2str(k), v.clone())).collect()
        };
        Ok(response)
    }

    pub fn view_latest_beacon_block(&self) -> Result<SignedBeaconBlockResponse, ()> {
        Ok(self.beacon_chain.best_block().into())
    }

    pub fn get_beacon_block_by_hash(
        &self,
        r: &GetBlockByHashRequest,
    ) -> Result<SignedBeaconBlockResponse, &str> {
        match self.beacon_chain.get_block(&BlockId::Hash(r.hash)) {
            Some(block) => Ok(block.into()),
            None => Err("block not found"),
        }
    }

    pub fn view_latest_shard_block(&self) -> Result<SignedShardBlockResponse, ()> {
        Ok(self.shard_chain.best_block().into())
    }

    pub fn get_shard_block_by_hash(
        &self,
        r: &GetBlockByHashRequest,
    ) -> Result<SignedShardBlockResponse, &str> {
        match self.shard_chain.get_block(&BlockId::Hash(r.hash)) {
            Some(block) => Ok(block.into()),
            None => Err("block not found"),
        }
    }

    pub fn get_shard_blocks_by_index(
        &self,
        r: &GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        let start = r.start.unwrap_or_else(|| { self.shard_chain.best_index() });
        let limit = r.limit.unwrap_or(25);
        self.shard_chain.get_blocks_by_index(start, limit).map(|blocks| {
            SignedShardBlocksResponse {
                blocks: blocks.into_iter().map(|x| x.into()).collect(),
            }
        })
    }
}
