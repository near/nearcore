use std::convert::TryInto;
use std::sync::Arc;

use client::Client;
use primitives::logging::pretty_utf8;
use primitives::types::BlockId;
use primitives::utils::bs58_vec2str;

use crate::types::*;
use primitives::transaction::verify_transaction_signature;
use primitives::transaction::SignedTransaction;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub struct HttpApi<T> {
    client: Arc<Client<T>>,
}

impl<T> HttpApi<T> {
    pub fn new(client: Arc<Client<T>>) -> Self {
        HttpApi { client }
    }
}

pub enum RPCError {
    BadRequest(String),
    NotFound,
    ServiceUnavailable(String),
}

impl<T> HttpApi<T> {
    pub fn view_account(&self, r: &ViewAccountRequest) -> Result<ViewAccountResponse, String> {
        debug!(target: "near-rpc", "View account {}", r.account_id);
        let mut state_update = self.client.shard_client.get_state_update();
        match self.client.shard_client.trie_viewer.view_account(&mut state_update, &r.account_id) {
            Ok(r) => Ok(ViewAccountResponse {
                account_id: r.account,
                amount: r.amount,
                stake: r.stake,
                code_hash: r.code_hash,
                public_keys: r.public_keys,
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
            "Call view function {}.{}({})",
            r.contract_account_id,
            r.method_name,
            pretty_utf8(&r.args),
        );
        let state_update = self.client.shard_client.get_state_update();
        let best_index = self.client.shard_client.chain.best_index();
        let mut logs = vec![];
        match self.client.shard_client.trie_viewer.call_function(
            state_update,
            best_index,
            &r.contract_account_id,
            &r.method_name,
            &r.args,
            &mut logs,
        ) {
            Ok(result) => Ok(CallViewFunctionResponse { result, logs }),
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn submit_transaction(
        &self,
        r: &SubmitTransactionRequest,
    ) -> Result<SubmitTransactionResponse, RPCError> {
        let transaction: SignedTransaction =
            r.transaction.clone().try_into().map_err(RPCError::BadRequest)?;
        debug!(target: "near-rpc", "Received transaction {:#?}", transaction);
        let originator = transaction.body.get_originator();
        let mut state_update = self.client.shard_client.get_state_update();
        let public_keys = self
            .client
            .shard_client
            .trie_viewer
            .get_public_keys_for_account(&mut state_update, &originator)
            .map_err(RPCError::BadRequest)?;
        if !verify_transaction_signature(&transaction, &public_keys) {
            let msg =
                format!("transaction not signed with a public key of originator {:?}", originator,);
            return Err(RPCError::BadRequest(msg));
        }
        let hash = transaction.get_hash();
        if let Some(pool) = &self.client.shard_client.pool {
            pool.write()
                .expect(POISONED_LOCK_ERR)
                .add_transaction(transaction)
                .map_err(RPCError::BadRequest)?;
        } else {
            // TODO(822): Relay to validator.
        }
        Ok(SubmitTransactionResponse { hash })
    }

    pub fn view_state(&self, r: &ViewStateRequest) -> Result<ViewStateResponse, String> {
        debug!(target: "near-rpc", "View state {:?}", r.contract_account_id);
        let state_update = self.client.shard_client.get_state_update();
        let result = self
            .client
            .shard_client
            .trie_viewer
            .view_state(&state_update, &r.contract_account_id)?;
        let response = ViewStateResponse {
            contract_account_id: r.contract_account_id.clone(),
            values: result.values.iter().map(|(k, v)| (bs58_vec2str(k), v.clone())).collect(),
        };
        Ok(response)
    }

    pub fn view_latest_beacon_block(&self) -> Result<SignedBeaconBlockResponse, ()> {
        Ok(self.client.beacon_client.chain.best_header().into())
    }

    pub fn get_beacon_block_by_hash(
        &self,
        r: &GetBlockByHashRequest,
    ) -> Result<SignedBeaconBlockResponse, &str> {
        match self.client.beacon_client.chain.get_header(&BlockId::Hash(r.hash)) {
            Some(header) => Ok(header.into()),
            None => Err("header not found"),
        }
    }

    pub fn view_latest_shard_block(&self) -> Result<SignedShardBlockResponse, ()> {
        match self.client.shard_client.chain.best_block() {
            Some(block) => Ok(block.into()),
            None => Err(()),
        }
    }

    pub fn get_shard_block_by_hash(
        &self,
        r: &GetBlockByHashRequest,
    ) -> Result<SignedShardBlockResponse, &str> {
        match self.client.shard_client.chain.get_block(&BlockId::Hash(r.hash)) {
            Some(block) => Ok(block.into()),
            None => Err("block not found"),
        }
    }

    pub fn get_shard_blocks_by_index(
        &self,
        r: &GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        let start = r.start.unwrap_or_else(|| self.client.shard_client.chain.best_index());
        let limit = r.limit.unwrap_or(25);
        let blocks = self.client.shard_client.chain.get_blocks_by_indices(start, limit);
        Ok(SignedShardBlocksResponse {
            blocks: blocks.into_iter().map(std::convert::Into::into).collect(),
        })
    }

    pub fn get_beacon_blocks_by_index(
        &self,
        r: &GetBlocksByIndexRequest,
    ) -> Result<SignedBeaconBlocksResponse, String> {
        let start = r.start.unwrap_or_else(|| self.client.beacon_client.chain.best_index());
        let limit = r.limit.unwrap_or(25);
        let headers = self.client.beacon_client.chain.get_headers_by_indices(start, limit);
        Ok(SignedBeaconBlocksResponse {
            blocks: headers.into_iter().map(std::convert::Into::into).collect(),
        })
    }

    pub fn get_transaction_info(
        &self,
        r: &GetTransactionRequest,
    ) -> Result<TransactionInfoResponse, RPCError> {
        match self.client.shard_client.get_transaction_info(&r.hash) {
            Some(info) => Ok(TransactionInfoResponse {
                transaction: info.transaction.into(),
                block_index: info.block_index,
                result: info.result,
            }),
            None => Err(RPCError::NotFound),
        }
    }

    pub fn get_receipt_info(
        &self,
        r: &GetTransactionRequest,
    ) -> Result<ReceiptInfoResponse, RPCError> {
        match self.client.shard_client.get_receipt_info(&r.hash) {
            Some(info) => Ok(ReceiptInfoResponse {
                receipt: info.receipt.into(),
                block_index: info.block_index,
                result: info.result,
            }),
            None => Err(RPCError::NotFound),
        }
    }

    pub fn get_transaction_final_result(
        &self,
        r: &GetTransactionRequest,
    ) -> Result<TransactionFinalResultResponse, ()> {
        let result = self.client.shard_client.get_transaction_final_result(&r.hash);
        Ok(TransactionFinalResultResponse { result })
    }

    pub fn get_transaction_result(
        &self,
        r: &GetTransactionRequest,
    ) -> Result<TransactionResultResponse, ()> {
        let result = self.client.shard_client.get_transaction_result(&r.hash);
        Ok(TransactionResultResponse { result })
    }

    pub fn healthz(&self) -> Result<HealthzResponse, ()> {
        let genesis_hash = self.client.beacon_client.chain.genesis_hash();
        let latest_block_index = self.client.beacon_client.chain.best_index();
        Ok(HealthzResponse { genesis_hash, latest_block_index })
    }
}
