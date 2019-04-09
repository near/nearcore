use std::sync::Arc;

use client::Client;
use node_http::types::{
    GetBlocksByIndexRequest, SignedBeaconBlockResponse, SignedShardBlocksResponse,
    SubmitTransactionRequest, SubmitTransactionResponse, ViewAccountRequest, ViewAccountResponse,
};
use node_runtime::state_viewer::AccountViewCallResult;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub trait NodeUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String>;

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        Ok(self.view_account(account_id)?.amount)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String>;

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64>;

    fn get_best_block_index(&self) -> u64;

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String>;
}

pub struct RpcNodeUser {
    pub url: String,
}

pub struct ThreadNodeUser {
    pub client: Arc<Client<InMemorySigner>>,
}

impl RpcNodeUser {
    pub fn new(rpc_port: u16) -> RpcNodeUser {
        RpcNodeUser { url: format!("http://127.0.0.1:{}", rpc_port) }
    }
}

impl ThreadNodeUser {
    pub fn new(client: Arc<Client<InMemorySigner>>) -> ThreadNodeUser {
        ThreadNodeUser { client }
    }
}

impl NodeUser for RpcNodeUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let client = reqwest::Client::new();
        let body = ViewAccountRequest { account_id: account_id.clone() };
        let url = format!("{}{}", self.url, "/view_account");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let response: ViewAccountResponse = response.json().unwrap();
        let result = AccountViewCallResult {
            account: response.account_id,
            nonce: response.nonce,
            amount: response.amount,
            stake: response.stake,
            code_hash: response.code_hash,
        };
        Ok(result)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        let client = reqwest::Client::new();
        let body = SubmitTransactionRequest { transaction: transaction.into() };
        let url = format!("{}{}", self.url, "/submit_transaction");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let _response: SubmitTransactionResponse = response.json().unwrap();
        Ok(())
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        Some(self.view_account(account_id).ok()?.nonce)
    }

    fn get_best_block_index(&self) -> u64 {
        let client = reqwest::Client::new();
        let url = format!("{}{}", self.url, "/view_latest_beacon_block");
        let mut response = client.post(url.as_str()).send().unwrap();
        let response: SignedBeaconBlockResponse = response.json().unwrap();
        response.header.index
    }

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        let client = reqwest::Client::new();
        let url = format!("{}{}", self.url, "/get_shard_blocks_by_index");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&r).unwrap()).send().unwrap();
        let response: SignedShardBlocksResponse = response.json().unwrap();
        Ok(response)
    }
}

impl NodeUser for ThreadNodeUser {
    fn view_account(&self, account_id: &String) -> Result<AccountViewCallResult, String> {
        let mut state_update = self.client.shard_client.get_state_update();
        self.client.shard_client.trie_viewer.view_account(&mut state_update, account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.client.shard_client.pool.clone().expect("Must have pool").write().expect(POISONED_LOCK_ERR).add_transaction(transaction)
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        self.client.shard_client.get_account_nonce(account_id.clone())
    }

    fn get_best_block_index(&self) -> u64 {
        self.client.beacon_client.chain.best_index()
    }
    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        let start = r.start.unwrap_or_else(|| self.client.shard_client.chain.best_index());
        let limit = r.limit.unwrap_or(25);
        let blocks = self.client.shard_client.chain.get_blocks_by_indices(start, limit);
        Ok(SignedShardBlocksResponse {
            blocks: blocks.into_iter().map(std::convert::Into::into).collect(),
        })
    }
}
