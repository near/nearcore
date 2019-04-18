use crate::user::{AsyncUser, User};
use futures::Future;
use node_http::types::{
    GetBlocksByIndexRequest, GetTransactionRequest, ReceiptInfoResponse, SignedBeaconBlockResponse,
    SignedShardBlockResponse, SignedShardBlocksResponse, SubmitTransactionRequest,
    SubmitTransactionResponse, TransactionFinalResultResponse, TransactionResultResponse,
    ViewAccountRequest, ViewAccountResponse, ViewStateRequest, ViewStateResponse,
};
use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};
use primitives::hash::CryptoHash;
use primitives::transaction::{
    FinalTransactionResult, ReceiptTransaction, SignedTransaction, TransactionResult,
};
use primitives::types::AccountId;
use reqwest::r#async::Client;
use shard::ReceiptInfo;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::time::Duration;

pub struct RpcUser {
    url: String,
}

/// Timeout for establishing connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

impl RpcUser {
    pub fn new(addr: SocketAddr) -> RpcUser {
        RpcUser { url: format!("http://{}", addr) }
    }

    /// Attempts to initialize the client.
    fn client(&self) -> Result<Client, String> {
        Client::builder()
            .use_rustls_tls()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|err| format!("{}", err))
    }
}

impl AsyncUser for RpcUser {
    fn view_account(
        &self,
        account_id: &String,
    ) -> Box<dyn Future<Item = AccountViewCallResult, Error = String>> {
        let url = format!("{}{}", self.url, "/view_account");
        let body: ViewAccountRequest = ViewAccountRequest { account_id: account_id.clone() };
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };

        let response = client
            .post(url.as_str())
            .body(serde_json::to_string(&body).unwrap())
            .send()
            .and_then(|mut resp| resp.json())
            .map(|response: ViewAccountResponse| AccountViewCallResult {
                account_id: response.account_id,
                nonce: response.nonce,
                amount: response.amount,
                public_keys: response.public_keys,
                stake: response.stake,
                code_hash: response.code_hash,
            })
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    fn view_state(
        &self,
        account_id: &String,
    ) -> Box<dyn Future<Item = ViewStateResult, Error = String>> {
        let url = format!("{}{}", self.url, "/view_state");
        let body = ViewStateRequest { contract_account_id: account_id.clone() };
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };
        let response = client
            .post(url.as_str())
            .body(serde_json::to_string(&body).unwrap())
            .send()
            .and_then(|mut resp| resp.json())
            .map(|response: ViewStateResponse| ViewStateResult {
                values: response
                    .values
                    .into_iter()
                    .map(|(s, v)| (bs58::decode(s).into_vec().unwrap(), v))
                    .collect(),
            })
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let url = format!("{}{}", self.url, "/submit_transaction");
        let body = SubmitTransactionRequest { transaction: transaction.into() };
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };
        let response = client
            .post(url.as_str())
            .body(serde_json::to_string(&body).unwrap())
            .send()
            .and_then(|mut resp| resp.json::<SubmitTransactionResponse>())
            .map(|_| ())
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    fn add_receipt(
        &self,
        _receipt: ReceiptTransaction,
    ) -> Box<dyn Future<Item = (), Error = String>> {
        unimplemented!()
    }

    fn get_account_nonce(
        &self,
        account_id: &String,
    ) -> Box<dyn Future<Item = u64, Error = String>> {
        let response = AsyncUser::view_account(self, account_id).map(|info| info.nonce);
        Box::new(response)
    }

    fn get_best_block_index(&self) -> Box<dyn Future<Item = u64, Error = String>> {
        let url = format!("{}{}", self.url, "/view_latest_beacon_block");
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };
        let response = client
            .post(url.as_str())
            .send()
            .and_then(|mut resp| resp.json::<SignedBeaconBlockResponse>())
            .map(|res| res.header.index)
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    fn get_transaction_result(
        &self,
        hash: &CryptoHash,
    ) -> Box<dyn Future<Item = TransactionResult, Error = String>> {
        let url = format!("{}{}", self.url, "/get_transaction_result");
        let body = GetTransactionRequest { hash: *hash };
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };
        let response = client
            .post(url.as_str())
            .body(serde_json::to_string(&body).unwrap())
            .send()
            .and_then(|mut resp| resp.json::<TransactionResultResponse>())
            .map(|res| res.result)
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    fn get_transaction_final_result(
        &self,
        hash: &CryptoHash,
    ) -> Box<Future<Item = FinalTransactionResult, Error = String>> {
        let url = format!("{}{}", self.url, "/get_transaction_final_result");
        let body = GetTransactionRequest { hash: *hash };
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };
        let response = client
            .post(url.as_str())
            .body(serde_json::to_string(&body).unwrap())
            .send()
            .and_then(|mut resp| resp.json::<TransactionFinalResultResponse>())
            .map(|res| res.result)
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    fn get_state_root(&self) -> Box<dyn Future<Item = CryptoHash, Error = String>> {
        let url = format!("{}{}", self.url, "/view_latest_shard_block");
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };
        let response = client
            .post(url.as_str())
            .send()
            .and_then(|mut resp| resp.json::<SignedShardBlockResponse>())
            .map(|resp| resp.body.header.merkle_root_state)
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    fn get_receipt_info(
        &self,
        hash: &CryptoHash,
    ) -> Box<dyn Future<Item = ReceiptInfo, Error = String>> {
        let url = format!("{}{}", self.url, "/get_transaction_result");
        let body = GetTransactionRequest { hash: *hash };
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };
        let response = client
            .post(url.as_str())
            .body(serde_json::to_string(&body).unwrap())
            .send()
            .and_then(|mut resp| resp.json::<ReceiptInfoResponse>())
            .map(|response| ReceiptInfo {
                receipt: response.receipt.body.try_into().unwrap(),
                block_index: response.block_index,
                result: response.result,
            })
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Box<dyn Future<Item = SignedShardBlocksResponse, Error = String>> {
        let url = format!("{}{}", self.url, "/get_shard_blocks_by_index");
        let client = match self.client() {
            Ok(c) => c,
            Err(err) => return Box::new(futures::future::done(Err(err))),
        };
        let response = client
            .post(url.as_str())
            .body(serde_json::to_string(&r).unwrap())
            .send()
            .and_then(|mut resp| resp.json::<SignedShardBlocksResponse>())
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let client = reqwest::Client::new();
        let body: ViewAccountRequest = ViewAccountRequest { account_id: account_id.clone() };
        let url = format!("{}{}", self.url, "/view_account");
        let mut response = client
            .post(url.as_str())
            .body(serde_json::to_string(&body).unwrap())
            .send()
            .map_err(|err| format!("{}", err))?;
        let response: ViewAccountResponse = response.json().map_err(|err| format!("{}", err))?;
        let result = AccountViewCallResult {
            account_id: response.account_id,
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
        let url = format!("{}{}", self.url, "/view_state");
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
        let client = reqwest::Client::new();
        let body = SubmitTransactionRequest { transaction: transaction.into() };
        let url = format!("{}{}", self.url, "/submit_transaction");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let _response: SubmitTransactionResponse = response.json().unwrap();
        Ok(())
    }

    fn add_receipt(&self, _receipt: ReceiptTransaction) -> Result<(), String> {
        unimplemented!("add receipt should not be implemented for RpcUser");
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        Some(User::view_account(self, account_id).ok()?.nonce)
    }

    fn get_best_block_index(&self) -> Option<u64> {
        let client = reqwest::Client::new();
        let url = format!("{}{}", self.url, "/view_latest_beacon_block");
        let mut response = client.post(url.as_str()).send().ok()?;
        let response: SignedBeaconBlockResponse = response.json().ok()?;
        Some(response.header.index)
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        let client = reqwest::Client::new();
        let body = GetTransactionRequest { hash: *hash };
        let url = format!("{}{}", self.url, "/get_transaction_result");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let response: TransactionResultResponse = response.json().unwrap();
        response.result
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        let client = reqwest::Client::new();
        let body = GetTransactionRequest { hash: *hash };
        let url = format!("{}{}", self.url, "/get_transaction_final_result");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let response: TransactionFinalResultResponse = response.json().unwrap();
        response.result
    }

    fn get_state_root(&self) -> CryptoHash {
        let client = reqwest::Client::new();
        let url = format!("{}{}", self.url, "/view_latest_shard_block");
        let mut response = client.post(url.as_str()).send().unwrap();
        let response: SignedShardBlockResponse = response.json().unwrap();
        response.body.header.merkle_root_state
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        let client = reqwest::Client::new();
        let body = GetTransactionRequest { hash: *hash };
        let url = format!("{}{}", self.url, "/get_transaction_result");
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
        let client = reqwest::Client::new();
        let url = format!("{}{}", self.url, "/get_shard_blocks_by_index");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&r).unwrap()).send().unwrap();
        let response: SignedShardBlocksResponse = response.json().unwrap();
        Ok(response)
    }
}
