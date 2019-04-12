use bs58;
use client::Client;
use node_http::types::{
    GetTransactionRequest, ReceiptInfoResponse, SignedBeaconBlockResponse,
    SignedShardBlockResponse, SubmitTransactionRequest, SubmitTransactionResponse,
    TransactionResultResponse, ViewAccountRequest, ViewAccountResponse, ViewStateRequest,
    ViewStateResponse,
};
use node_runtime::state_viewer::{AccountViewCallResult, TrieViewer, ViewStateResult};
use node_runtime::test_utils::to_receipt_block;
use node_runtime::{ApplyState, Runtime};
use primitives::block_traits::SignedBlock;
use primitives::chain::ReceiptBlock;
use primitives::hash::CryptoHash;
use primitives::transaction::{ReceiptTransaction, SignedTransaction, TransactionResult};
use primitives::types::{AccountId, Balance, MerkleHash, Nonce};
use shard::{ReceiptInfo, ShardClient};
use storage::storages::GenericStorage;
use storage::{Trie, TrieUpdate};

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::rc::Rc;
use std::sync::Arc;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub trait NodeUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String>;

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        Ok(self.view_account(account_id)?.amount)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String>;

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String>;

    // this should not be implemented for RpcUser
    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String>;

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64>;

    fn get_best_block_index(&self) -> u64;

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult;

    fn get_state_root(&self) -> MerkleHash;

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo>;
}

pub struct RpcNodeUser {
    pub url: String,
}

pub struct ThreadNodeUser {
    pub client: Arc<Client>,
}

pub struct ShardClientUser {
    pub client: Arc<ShardClient>,
}

/// Fake client without chain, used in RuntimeUser and RuntimeNode
pub struct FakeClient {
    pub runtime: Runtime,
    // Arc here because get_runtime_and_trie returns Arc<Trie> and
    // TrieUpdate takes Arc<Trie>.
    pub trie: Arc<Trie>,
    pub state_root: MerkleHash,
}

impl FakeClient {
    pub fn get_state_update(&self) -> TrieUpdate {
        TrieUpdate::new(self.trie.clone(), self.state_root)
    }
}

pub struct RuntimeUser {
    pub account_id: AccountId,
    pub nonce: RefCell<Nonce>,
    pub trie_viewer: TrieViewer,
    pub client: Rc<RefCell<FakeClient>>,
    // Store results of applying transactions/receipts
    pub transaction_results: RefCell<HashMap<CryptoHash, TransactionResult>>,
    // store receipts generated when applying transactions
    pub receipts: RefCell<HashSet<ReceiptTransaction>>,
}

impl RpcNodeUser {
    pub fn new(rpc_port: u16) -> RpcNodeUser {
        RpcNodeUser { url: format!("http://127.0.0.1:{}", rpc_port) }
    }
}

impl ThreadNodeUser {
    pub fn new(client: Arc<Client>) -> ThreadNodeUser {
        ThreadNodeUser { client }
    }
}

impl ShardClientUser {
    pub fn new(client: Arc<ShardClient>) -> ShardClientUser {
        ShardClientUser { client }
    }

    /// unified way of submitting transaction or receipt. Pool
    /// must not be empty.
    fn add_payload(&self) -> Result<(), String> {
        assert!(!self.client.pool.is_empty());
        let snapshot = self.client.pool.snapshot_payload();
        let payload = self.client.pool.pop_payload_snapshot(&snapshot).unwrap();
        let mut transactions = payload.transactions;
        let mut receipts = payload.receipts;

        loop {
            let last_block_hash = *self
                .client
                .storage
                .write()
                .expect(POISONED_LOCK_ERR)
                .blockchain_storage_mut()
                .best_block_hash()
                .map_err(|e| format!("{}", e))?
                .unwrap();
            let (block, block_extra) =
                self.client.prepare_new_block(last_block_hash, receipts, transactions.clone());
            let index = block.index();
            let has_new_receipts = block_extra.new_receipts.is_empty();
            self.client.insert_block(
                &block,
                block_extra.db_changes,
                block_extra.tx_results,
                block_extra.largest_tx_nonce,
                block_extra.new_receipts,
            );
            if has_new_receipts {
                break;
            }
            transactions.clear();
            receipts = vec![self
                .client
                .get_receipt_block(index, 0)
                .ok_or("Receipt does not exist".to_string())?];
        }
        Ok(())
    }
}

impl RuntimeUser {
    pub fn new(account_id: &str, client: Rc<RefCell<FakeClient>>) -> Self {
        RuntimeUser {
            trie_viewer: TrieViewer {},
            account_id: account_id.to_string(),
            nonce: Default::default(),
            client,
            transaction_results: Default::default(),
            receipts: Default::default(),
        }
    }

    pub fn apply_all(
        &self,
        apply_state: ApplyState,
        prev_receipts: Vec<ReceiptBlock>,
        transactions: Vec<SignedTransaction>,
    ) {
        let mut cur_apply_state = apply_state;
        let mut receipts = prev_receipts;
        let mut txs = transactions;
        loop {
            let mut client = self.client.borrow_mut();
            let state_update = TrieUpdate::new(client.trie.clone(), cur_apply_state.root);
            let mut apply_result =
                client.runtime.apply(state_update, &cur_apply_state, &receipts, &txs);
            let mut counter = 0;
            for (i, receipt) in receipts.iter().flat_map(|b| b.receipts.iter()).enumerate() {
                counter += 1;
                let transaction_result = apply_result.tx_result[i].clone();
                self.transaction_results.borrow_mut().insert(receipt.nonce, transaction_result);
            }
            for (i, tx) in txs.iter().enumerate() {
                let transaction_result = apply_result.tx_result[i + counter].clone();
                self.transaction_results.borrow_mut().insert(tx.get_hash(), transaction_result);
            }
            client.trie.apply_changes(apply_result.db_changes).unwrap();
            if apply_result.new_receipts.is_empty() {
                client.state_root = apply_result.root;
                return;
            }
            cur_apply_state = ApplyState {
                root: apply_result.root,
                shard_id: cur_apply_state.shard_id,
                block_index: cur_apply_state.block_index,
                parent_block_hash: cur_apply_state.parent_block_hash,
            };
            let new_receipts: Vec<_> =
                apply_result.new_receipts.drain().flat_map(|(_, v)| v).collect();
            for receipt in new_receipts.iter() {
                self.receipts.borrow_mut().insert(receipt.clone());
            }
            receipts = vec![to_receipt_block(new_receipts)];
            txs = vec![];
        }
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
            public_keys: response.public_keys,
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
        unreachable!("add receipt should not be implemented for RpcUser");
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

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        let client = reqwest::Client::new();
        let body = GetTransactionRequest { hash: *hash };
        let url = format!("{}{}", self.url, "/get_transaction_result");
        let mut response =
            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
        let response: TransactionResultResponse = response.json().unwrap();
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
}

impl NodeUser for ThreadNodeUser {
    fn view_account(&self, account_id: &String) -> Result<AccountViewCallResult, String> {
        let mut state_update = self.client.shard_client.get_state_update();
        self.client.shard_client.trie_viewer.view_account(&mut state_update, account_id)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        let mut state_update = self.client.shard_client.get_state_update();
        self.client.shard_client.trie_viewer.view_state(&mut state_update, account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.client.shard_client.pool.add_transaction(transaction)
    }

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String> {
        let receipt_block = to_receipt_block(vec![receipt]);
        self.client.shard_client.pool.add_receipt(receipt_block)
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        self.client.shard_client.get_account_nonce(account_id.clone())
    }

    fn get_best_block_index(&self) -> u64 {
        self.client.beacon_client.chain.best_index()
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.client.shard_client.get_transaction_result(hash)
    }

    fn get_state_root(&self) -> MerkleHash {
        self.client.shard_client.chain.best_header().body.merkle_root_state
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        self.client.shard_client.get_receipt_info(hash)
    }
}

impl NodeUser for ShardClientUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let mut state_update = self.client.get_state_update();
        self.client.trie_viewer.view_account(&mut state_update, account_id)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        let mut state_update = self.client.get_state_update();
        self.client.trie_viewer.view_state(&mut state_update, account_id)
    }

    /// First adding transaction to the mempool and then simulate how payload are extracted from
    /// mempool and executed in runtime.
    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.client.pool.add_transaction(transaction).unwrap();
        self.add_payload()
    }

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String> {
        let receipt_block = to_receipt_block(vec![receipt]);
        self.client.pool.add_receipt(receipt_block)?;
        self.add_payload()
    }

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64> {
        self.client.get_account_nonce(account_id.clone())
    }

    fn get_best_block_index(&self) -> u64 {
        self.client.chain.best_index()
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.client.get_transaction_result(hash)
    }

    fn get_state_root(&self) -> MerkleHash {
        self.client.chain.best_header().body.merkle_root_state
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        self.client.get_receipt_info(hash)
    }
}

impl NodeUser for RuntimeUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let mut state_update = self.client.borrow().get_state_update();
        self.trie_viewer.view_account(&mut state_update, account_id)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        let mut state_update = self.client.borrow().get_state_update();
        self.trie_viewer.view_state(&mut state_update, account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        let apply_state = ApplyState {
            root: self.client.borrow().state_root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0,
        };
        *self.nonce.borrow_mut() += 1;
        self.apply_all(apply_state, vec![], vec![transaction]);
        Ok(())
    }

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String> {
        let apply_state = ApplyState {
            root: self.client.borrow().state_root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0,
        };
        self.apply_all(apply_state, vec![to_receipt_block(vec![receipt])], vec![]);
        Ok(())
    }

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64> {
        self.view_account(account_id).ok().map(|account| account.nonce)
    }

    fn get_best_block_index(&self) -> u64 {
        unreachable!("get_best_block_index should not be implemented for RuntimeUser");
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.transaction_results.borrow().get(hash).cloned().unwrap()
    }

    fn get_state_root(&self) -> MerkleHash {
        self.client.borrow().state_root
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        let receipt = self.receipts.borrow().get(hash).cloned()?;
        let transaction_result = self.transaction_results.borrow().get(hash).cloned()?;
        Some(ReceiptInfo { receipt, result: transaction_result, block_index: Default::default() })
    }
}
