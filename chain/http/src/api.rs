use std::convert::TryInto;
use std::sync::Arc;

use protobuf::parse_from_bytes;
use serde_json::json;

use client::Client;
use primitives::hash::CryptoHash;
use primitives::logging::pretty_utf8;
use primitives::rpc::ABCIQueryResponse;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, BlockId};
use primitives::utils::bs58_vec2str;
use verifier::TransactionVerifier;

use crate::types::*;
use node_runtime::state_viewer::AccountViewCallResult;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Adapter for querying runtime.
pub trait RuntimeAdapter {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String>;
    fn call_function(
        &self,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, String>;
}

/// Facade to query given client with <path> + <data> at <block height> with optional merkle prove request.
/// Given implementation only supports latest height, thus ignoring it.
pub fn query_client(
    adapter: &RuntimeAdapter,
    path: &str,
    data: &[u8],
    _height: u64,
    _prove: bool,
) -> Result<ABCIQueryResponse, String> {
    let path_parts: Vec<&str> = path.split('/').collect();
    if path_parts.is_empty() {
        return Err("Path must contain at least single token".to_string());
    }
    match path_parts[0] {
        "account" => match adapter.view_account(&AccountId::from(path_parts[1])) {
            Ok(r) => Ok(ABCIQueryResponse::account(path, r)),
            Err(e) => Err(e),
        },
        "call" => {
            let mut logs = vec![];
            match adapter.call_function(
                &AccountId::from(path_parts[1]),
                path_parts[2],
                &data,
                &mut logs,
            ) {
                Ok(result) => Ok(ABCIQueryResponse::result(path, result, logs)),
                Err(e) => Ok(ABCIQueryResponse::result_err(path, e, logs)),
            }
        }
        _ => Err(format!("Unknown path {}", path)),
    }
}

pub struct HttpApi<T> {
    client: Arc<Client<T>>,
}

impl<T> RuntimeAdapter for HttpApi<T> {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let state_update = self.client.shard_client.get_state_update();
        self.client.shard_client.trie_viewer.view_account(&state_update, account_id)
    }

    fn call_function(
        &self,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, String> {
        let best_index = self.client.shard_client.chain.best_index();
        let state_update = self.client.shard_client.get_state_update();
        self.client.shard_client.trie_viewer.call_function(
            state_update,
            best_index,
            contract_id,
            method_name,
            args,
            logs,
        )
    }
}

impl<T> HttpApi<T> {
    pub fn new(client: Arc<Client<T>>) -> Self {
        HttpApi { client }
    }
}

fn decode_transaction(value: &serde_json::Value) -> Result<SignedTransaction, RPCError> {
    let data = base64::decode(
        value.as_str().ok_or_else(|| RPCError::BadRequest("Param should be bytes".to_string()))?,
    )
    .map_err(|e| RPCError::BadRequest(format!("Failed to decode base64: {}", e)))?;
    let transaction = parse_from_bytes::<near_protos::signed_transaction::SignedTransaction>(&data)
        .map_err(|e| RPCError::BadRequest(format!("Failed to parse protobuf: {}", e)))?;
    transaction.try_into().map_err(RPCError::BadRequest)
}

impl<T> HttpApi<T> {
    pub fn jsonrpc(&self, r: &JsonRpcRequest) -> JsonRpcResponse {
        let mut resp = JsonRpcResponse {
            jsonrpc: r.jsonrpc.clone(),
            id: r.id.clone(),
            result: None,
            error: None,
        };
        match self.jsonrpc_matcher(&r.method, &r.params) {
            Ok(value) => resp.result = Some(value),
            Err(err) => resp.error = Some(err.into()),
        }
        resp
    }

    fn jsonrpc_matcher(
        &self,
        method: &String,
        params: &Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, RPCError> {
        match method.as_ref() {
            "abci_query" => {
                if params.len() != 4 {
                    return Err(RPCError::BadRequest(
                        "Invalid number of arguments, must be 3".to_string(),
                    ));
                }
                let path = params[0].as_str().ok_or_else(|| {
                    RPCError::BadRequest("Path param should be string".to_string())
                })?;
                let data_str = params[1].as_str().ok_or_else(|| {
                    RPCError::BadRequest("Path param should be string".to_string())
                })?;
                let data = hex::decode(data_str)
                    .map_err(|e| RPCError::BadRequest(format!("Failed to parse base64: {}", e)))?;

                let response =
                    query_client(self, path, &data, 0, false).map_err(RPCError::BadRequest)?;
                Ok(json!({"response": {
                   "log": response.log,
                    "height": response.height,
                    "proof": response.proof,
                    "value": base64::encode(&response.value),
                    "key": base64::encode(&response.key),
                    "index": response.index,
                    "code": response.code
                }}))
            }
            "broadcast_tx_async" => {
                if params.len() != 1 {
                    return Err(RPCError::BadRequest(
                        "Invalid number of arguments, must be 1".to_string(),
                    ));
                }
                let transaction = decode_transaction(&params[0])?;
                let hash = self.submit_tx(transaction)?;
                Ok(json!({"hash": hash, "log": "", "data": "", "code": "0"}))
            }
            "tx" => {
                if params.len() != 2 {
                    return Err(RPCError::BadRequest(
                        "Invalid number of arguments, must be 2".to_string(),
                    ));
                }
                let hash_str = params[0].as_str().ok_or_else(|| {
                    RPCError::BadRequest("Hash param must be base64 string".to_string())
                })?;
                let hash = base64::decode(hash_str)
                    .map_err(|e| RPCError::BadRequest(format!("Failed to decode base64: {}", e)))?
                    .try_into()
                    .map_err(|e| RPCError::BadRequest(format!("Bad hash: {}", e)))?;
                let result = self.client.shard_client.get_transaction_final_result(&hash);
                // TODO: include merkle proof if requested, tx bytes, index and block height in final result.
                Ok(json!({
                    "proof": "null",
                    "tx": "null",
                    "tx_result": {
                        "log": result.final_log(),
                        "data": base64::encode(&result.last_result()),
                        "code": result.status.to_code(),
                    },
                    "index": 0,
                    "height": 0,
                    "hash": hash_str,
                }))
            }
            _ => Err(RPCError::MethodNotFound(format!("{} not found", method))),
        }
    }

    pub fn view_account(&self, r: &ViewAccountRequest) -> Result<ViewAccountResponse, String> {
        debug!(target: "near-rpc", "View account {}", r.account_id);
        let state_update = self.client.shard_client.get_state_update();
        match self.client.shard_client.trie_viewer.view_account(&state_update, &r.account_id) {
            Ok(r) => Ok(ViewAccountResponse {
                account_id: r.account_id,
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

    fn submit_tx(&self, transaction: SignedTransaction) -> Result<CryptoHash, RPCError> {
        debug!(target: "near-rpc", "Received transaction {:#?}", transaction);
        let state_update = self.client.shard_client.get_state_update();
        let verifier = TransactionVerifier::new(&state_update);
        verifier.verify_transaction(&transaction).map_err(RPCError::BadRequest)?;
        let hash = transaction.get_hash();
        if let Some(pool) = &self.client.shard_client.pool {
            pool.write()
                .expect(POISONED_LOCK_ERR)
                .add_transaction(transaction)
                .map_err(RPCError::BadRequest)?;
        } else {
            // TODO(822): Relay to validator.
        }
        Ok(hash)
    }

    pub fn submit_transaction(
        &self,
        r: &SubmitTransactionRequest,
    ) -> Result<SubmitTransactionResponse, RPCError> {
        let transaction: SignedTransaction =
            r.transaction.clone().try_into().map_err(RPCError::BadRequest)?;
        self.submit_tx(transaction).map(|hash| SubmitTransactionResponse { hash })
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
