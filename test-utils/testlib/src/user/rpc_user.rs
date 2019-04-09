use crate::user::User;
use primitives::types::AccountId;
use node_runtime::state_viewer::AccountViewCallResult;
use node_http::types::{ViewAccountRequest, ViewAccountResponse, SubmitTransactionRequest, SubmitTransactionResponse, SignedBeaconBlockResponse, GetBlocksByIndexRequest, SignedShardBlocksResponse};
use primitives::transaction::SignedTransaction;

pub struct RpcUser {
    pub url: String,
}

impl RpcUser {
    pub fn new(rpc_port: u16) -> RpcUser {
        RpcUser { url: format!("http://127.0.0.1:{}", rpc_port) }
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let client = reqwest::Client::new();
        let body: ViewAccountRequest = ViewAccountRequest { account_id: account_id.clone() };
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
