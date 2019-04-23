use crate::runtime_utils::to_receipt_block;
use crate::user::{User, POISONED_LOCK_ERR};
use client::Client;
use node_http::types::{GetBlocksByIndexRequest, SignedShardBlocksResponse};
use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};
use primitives::crypto::signer::InMemorySigner;
use primitives::hash::CryptoHash;
use primitives::transaction::{
    FinalTransactionResult, ReceiptTransaction, SignedTransaction, TransactionResult,
};
use primitives::types::{AccountId, MerkleHash};
use shard::ReceiptInfo;
use std::sync::Arc;

pub struct ThreadUser {
    pub client: Arc<Client<InMemorySigner>>,
}

impl ThreadUser {
    pub fn new(client: Arc<Client<InMemorySigner>>) -> ThreadUser {
        ThreadUser { client }
    }
}

impl User for ThreadUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let state_update = self.client.shard_client.get_state_update();
        self.client.shard_client.trie_viewer.view_account(&state_update, account_id)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        let state_update = self.client.shard_client.get_state_update();
        self.client.shard_client.trie_viewer.view_state(&state_update, account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.client
            .shard_client
            .pool
            .clone()
            .expect("Must have pool")
            .write()
            .expect(POISONED_LOCK_ERR)
            .add_transaction(transaction)
    }

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String> {
        let receipt_block = to_receipt_block(vec![receipt]);
        self.client
            .shard_client
            .pool
            .clone()
            .expect("Must have pool")
            .write()
            .expect(POISONED_LOCK_ERR)
            .add_receipt(receipt_block)
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        self.client.shard_client.get_account_nonce(account_id.clone())
    }

    fn get_best_block_index(&self) -> Option<u64> {
        Some(self.client.beacon_client.chain.best_index())
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.client.shard_client.get_transaction_result(hash)
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        self.client.shard_client.get_transaction_final_result(hash)
    }

    fn get_state_root(&self) -> MerkleHash {
        self.client.shard_client.chain.best_header().body.merkle_root_state
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        self.client.shard_client.get_receipt_info(hash)
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
