use std::sync::Arc;

use futures::sync::mpsc::Sender;

use client::Client;
use primitives::types::BlockId;
use primitives::utils::bs58_vec2str;
use transaction::{SignedTransaction, verify_transaction_signature};

use crate::types::{
    CallViewFunctionRequest, CallViewFunctionResponse, GetBlockByHashRequest,
    GetBlocksByIndexRequest, GetTransactionRequest, SignedBeaconBlockResponse,
    SignedBeaconBlocksResponse, SignedShardBlockResponse, SignedShardBlocksResponse,
    SubmitTransactionRequest, SubmitTransactionResponse, TransactionInfoResponse,
    TransactionResultResponse, ViewAccountRequest, ViewAccountResponse, ViewStateRequest,
    ViewStateResponse,
};

pub struct HttpApi {
    client: Arc<Client>,
    submit_txn_sender: Sender<SignedTransaction>,
}

impl HttpApi {
    pub fn new(client: Arc<Client>, submit_txn_sender: Sender<SignedTransaction>) -> HttpApi {
        HttpApi { client, submit_txn_sender }
    }
}

pub enum RPCError {
    BadRequest(String),
    NotFound,
    ServiceUnavailable(String),
}

impl HttpApi {
    pub fn view_account(&self, r: &ViewAccountRequest) -> Result<ViewAccountResponse, String> {
        debug!(target: "near-rpc", "View account {:?}", r.account_id);
        let mut state_update = self.client.shard_chain.get_state_update();
        match self.client.shard_chain.statedb_viewer.view_account(
            &mut state_update,
            &r.account_id
        ) {
            Ok(r) => Ok(ViewAccountResponse {
                account_id: r.account,
                amount: r.amount,
                stake: r.stake,
                code_hash: r.code_hash,
                nonce: r.nonce,
            }),
            Err(e) => Err(e.to_string()),
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
        let state_update = self.client.shard_chain.get_state_update();
        let best_index = self.client.shard_chain.chain.best_index();
        match self.client.shard_chain.statedb_viewer.call_function(
            state_update,
            best_index, 
            &r.contract_account_id,
            &r.method_name,
            &r.args
        ) {
            Ok(result) => Ok(CallViewFunctionResponse { result }),
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn submit_transaction(
        &self,
        r: &SubmitTransactionRequest,
    ) -> Result<SubmitTransactionResponse, RPCError> {
        let transaction: SignedTransaction = r.transaction.clone().into();
        debug!(target: "near-rpc", "Received transaction {:?}", transaction);
        let originator = transaction.body.get_originator();
        let mut state_update = self.client.shard_chain.get_state_update();
        let public_keys = self.client.shard_chain.statedb_viewer
            .get_public_keys_for_account(&mut state_update, &originator)
            .map_err(RPCError::BadRequest)?;
        if !verify_transaction_signature(&transaction, &public_keys) {
            let msg =
                format!("transaction not signed with a public key of originator {:?}", originator,);
            return Err(RPCError::BadRequest(msg));
        }

        self.submit_txn_sender
            .clone()
            .try_send(transaction.clone())
            .map_err(|_| RPCError::ServiceUnavailable("transaction channel is full".to_string()))?;
        Ok(SubmitTransactionResponse { hash: transaction.get_hash() })
    }

    pub fn view_state(&self, r: &ViewStateRequest) -> Result<ViewStateResponse, String> {
        debug!(target: "near-rpc", "View state {:?}", r.contract_account_id);
        let state_update = self.client.shard_chain.get_state_update();
        let result = self.client.shard_chain.statedb_viewer
            .view_state(&state_update, &r.contract_account_id)?;
        let response = ViewStateResponse {
            contract_account_id: r.contract_account_id.clone(),
            values: result.values.iter().map(|(k, v)| (bs58_vec2str(k), v.clone())).collect(),
        };
        Ok(response)
    }

    pub fn view_latest_beacon_block(&self) -> Result<SignedBeaconBlockResponse, ()> {
        Ok(self.client.beacon_chain.chain.best_block().into())
    }

    pub fn get_beacon_block_by_hash(
        &self,
        r: &GetBlockByHashRequest,
    ) -> Result<SignedBeaconBlockResponse, &str> {
        match self.client.beacon_chain.chain.get_block(&BlockId::Hash(r.hash)) {
            Some(block) => Ok(block.into()),
            None => Err("block not found"),
        }
    }

    pub fn view_latest_shard_block(&self) -> Result<SignedShardBlockResponse, ()> {
        Ok(self.client.shard_chain.chain.best_block().into())
    }

    pub fn get_shard_block_by_hash(
        &self,
        r: &GetBlockByHashRequest,
    ) -> Result<SignedShardBlockResponse, &str> {
        match self.client.shard_chain.chain.get_block(&BlockId::Hash(r.hash)) {
            Some(block) => Ok(block.into()),
            None => Err("block not found"),
        }
    }

    pub fn get_shard_blocks_by_index(
        &self,
        r: &GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        let start = r.start.unwrap_or_else(|| self.client.shard_chain.chain.best_index());
        let limit = r.limit.unwrap_or(25);
        self.client.shard_chain.chain.get_blocks_by_index(start, limit).map(|blocks| {
            SignedShardBlocksResponse {
                blocks: blocks.into_iter().map(|x| x.into()).collect(),
            }
        })
    }

    pub fn get_beacon_blocks_by_index(
        &self,
        r: &GetBlocksByIndexRequest,
    ) -> Result<SignedBeaconBlocksResponse, String> {
        let start = r.start.unwrap_or_else(|| self.client.beacon_chain.chain.best_index());
        let limit = r.limit.unwrap_or(25);
        self.client.beacon_chain.chain.get_blocks_by_index(start, limit).map(|blocks| {
            SignedBeaconBlocksResponse {
                blocks: blocks.into_iter().map(|x| x.into()).collect(),
            }
        })
    }

    pub fn get_transaction_info(
        &self,
        r: &GetTransactionRequest,
    ) -> Result<TransactionInfoResponse, RPCError> {
        match self.client.shard_chain.get_transaction_info(&r.hash) {
            Some(info) => Ok(TransactionInfoResponse {
                transaction: info.transaction.into(),
                block_index: info.block_index,
                result: info.result
            }),
            None => Err(RPCError::NotFound),
        }

    }

    pub fn get_transaction_result(
        &self,
        r: &GetTransactionRequest,
    ) -> Result<TransactionResultResponse, ()> {
        let result = self.client.shard_chain.get_transaction_final_result(&r.hash);
        Ok(TransactionResultResponse { result })
    }
}
