use std::sync::Arc;

use actix::Addr;
use near_client::{ClientActor, ViewClientActor, Query};
use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    FinalTransactionResult, ReceiptTransaction, SignedTransaction, TransactionResult,
};
use near_primitives::types::{AccountId, MerkleHash};
use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};

use crate::runtime_utils::to_receipt_block;
use crate::user::{User, POISONED_LOCK_ERR};
use near_primitives::receipt::ReceiptInfo;
use futures::future::Future;

pub struct ThreadUser {
    pub client_addr: Addr<ClientActor>,
    pub view_client_addr: Addr<ViewClientActor>,
}

impl ThreadUser {
    pub fn new(
        client_addr: Addr<ClientActor>,
        view_client_addr: Addr<ViewClientActor>,
    ) -> ThreadUser {
        ThreadUser { client_addr, view_client_addr }
    }
}

impl User for ThreadUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let result = self.view_client_addr.send(Query { path: format!("account/{}", account_id), data: vec![] }).wait();
        println!("{:?}", result);
        Err("".to_string())
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        Err("".to_string())
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        Err("".to_string())
    }

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String> {
        Err("".to_string())
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        let result = self.view_client_addr.send(Query { path: format!("account/{}", account_id), data: vec![] }).wait();
        println!("{:?}", result);
        None
    }

    fn get_best_block_index(&self) -> Option<u64> {
        None
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        TransactionResult::default()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        FinalTransactionResult::default()
    }

    fn get_state_root(&self) -> MerkleHash {
        MerkleHash::default()
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        None
    }

    fn get_access_key(&self, public_key: &PublicKey) -> Result<Option<AccessKey>, String> {
        Err("".to_string())
    }
}
