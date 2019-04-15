use crate::user::User;
use node_http::types::{
    GetBlocksByIndexRequest, GetTransactionRequest, ReceiptInfoResponse, SignedBeaconBlockResponse,
    SignedShardBlockResponse, SignedShardBlocksResponse, SubmitTransactionRequest,
    SubmitTransactionResponse, TransactionResultResponse, ViewAccountRequest, ViewAccountResponse,
    ViewStateRequest, ViewStateResponse,
};
use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};
use primitives::hash::CryptoHash;
use primitives::transaction::{ReceiptTransaction, SignedTransaction, TransactionResult};
use primitives::types::AccountId;
use shard::ReceiptInfo;
use std::convert::TryInto;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;

pub struct RpcUser {
    pub addr: SocketAddr,
}

impl RpcUser {
    pub fn new(addr: SocketAddr) -> RpcUser {
        RpcUser { addr }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn client(&self) -> Result<reqwest::Client, String> {
        // We need to timeout the request in order to not block the thread. 1 sec is enough for
        // most practical applications.
        reqwest::Client::builder()
            .timeout(Some(Duration::from_secs(1)))
            .build()
            .map_err(|e| e.description().to_owned())
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let client = self.client()?;
        let body: ViewAccountRequest = ViewAccountRequest { account_id: account_id.clone() };
        let url = format!("{}{}", self.url(), "/view_account");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let response: ViewAccountResponse = response.json().unwrap();
        let result = AccountViewCallResult {
            account: response.account_id,
            nonce: response.nonce,
            amount: response.amount,
            public_keys: response.public_keys,
            stake: response.stake,
            code_hash: response.code_hash,
        };
        Ok(result)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        let client = reqwest::Client::new();
        let body = ViewStateRequest { contract_account_id: account_id.clone() };
        let url = format!("{}{}", self.url(), "/view_state");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let response: ViewStateResponse = response.json().unwrap();
        let result = ViewStateResult {
            values: response
                .values
                .into_iter()
                .map(|(s, v)| (bs58::decode(s).into_vec().unwrap(), v))
                .collect(),
        };
        Ok(result)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        let client = self.client()?;
        let body = SubmitTransactionRequest { transaction: transaction.into() };
        let url = format!("{}{}", self.url(), "/submit_transaction");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let _response: SubmitTransactionResponse = response.json().unwrap();
        Ok(())
    }

    fn add_receipt(&self, _receipt: ReceiptTransaction) -> Result<(), String> {
        unimplemented!("add receipt should not be implemented for RpcUser");
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        Some(self.view_account(account_id).ok()?.nonce)
    }

    fn get_best_block_index(&self) -> Option<u64> {
        let client = self.client().ok()?;
        let url = format!("{}{}", self.url(), "/view_latest_beacon_block");
        let mut response = client.post(url.as_str()).send().ok()?;
        let response: SignedBeaconBlockResponse = response.json().ok()?;
        Some(response.header.index)
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        let client = reqwest::Client::new();
        let body = GetTransactionRequest { hash: *hash };
        let url = format!("{}{}", self.url(), "/get_transaction_result");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let response: TransactionResultResponse = response.json().unwrap();
        response.result
    }

    fn get_state_root(&self) -> CryptoHash {
        let client = reqwest::Client::new();
        let url = format!("{}{}", self.url(), "/view_latest_shard_block");
        let mut response = client.post(url.as_str()).send().unwrap();
        let response: SignedShardBlockResponse = response.json().unwrap();
        response.body.header.merkle_root_state
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        let client = reqwest::Client::new();
        let body = GetTransactionRequest { hash: *hash };
        let url = format!("{}{}", self.url(), "/get_transaction_result");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let response: ReceiptInfoResponse = response.json().unwrap();
        Some(ReceiptInfo {
            receipt: response.receipt.body.try_into().ok()?,
            block_index: response.block_index,
            result: response.result,
        })
    }

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        let client = self.client()?;
        let url = format!("{}{}", self.url(), "/get_shard_blocks_by_index");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&r).unwrap()).send().unwrap();
        let response: SignedShardBlocksResponse = response.json().unwrap();
        Ok(response)
    }
}
