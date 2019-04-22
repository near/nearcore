use crate::user::{User, POISONED_LOCK_ERR};
use crate::runtime_utils::to_receipt_block;
use node_http::types::{GetBlocksByIndexRequest, SignedShardBlocksResponse};
use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};
use primitives::block_traits::SignedBlock;
use primitives::hash::CryptoHash;
use primitives::transaction::{
    FinalTransactionResult, ReceiptTransaction, SignedTransaction, TransactionResult,
};
use primitives::types::{AccountId, MerkleHash};
use shard::{ReceiptInfo, ShardClient};
use std::sync::Arc;
use storage::storages::GenericStorage;

pub struct ShardClientUser {
    pub client: Arc<ShardClient>,
}

impl ShardClientUser {
    pub fn new(client: Arc<ShardClient>) -> ShardClientUser {
        ShardClientUser { client }
    }

    /// unified way of submitting transaction or receipt. Pool
    /// must not be empty.
    fn add_payload(&self) -> Result<(), String> {
        let payload = {
            let mempool = self.client.pool.clone().unwrap();
            let mut pool = mempool.write().expect(POISONED_LOCK_ERR);
            assert!(!pool.is_empty());
            let snapshot = pool.snapshot_payload();
            pool.pop_payload_snapshot(&snapshot).unwrap()
        };
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
                .ok_or_else(|| "Receipt does not exist".to_string())?];
        }
        Ok(())
    }
}

impl User for ShardClientUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let mut state_update = self.client.get_state_update();
        self.client.trie_viewer.view_account(&mut state_update, account_id)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        let state_update = self.client.get_state_update();
        self.client.trie_viewer.view_state(&state_update, account_id)
    }

    /// First adding transaction to the mempool and then simulate how payload are extracted from
    /// mempool and executed in runtime.
    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.client
            .pool
            .clone()
            .unwrap()
            .write()
            .expect(POISONED_LOCK_ERR)
            .add_transaction(transaction)
            .unwrap();
        self.add_payload()
    }

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String> {
        let receipt_block = to_receipt_block(vec![receipt]);
        self.client
            .pool
            .clone()
            .unwrap()
            .write()
            .expect(POISONED_LOCK_ERR)
            .add_receipt(receipt_block)?;
        self.add_payload()
    }

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64> {
        self.client.get_account_nonce(account_id.clone())
    }

    fn get_best_block_index(&self) -> Option<u64> {
        Some(self.client.chain.best_index())
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.client.get_transaction_result(hash)
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        self.client.get_transaction_final_result(hash)
    }

    fn get_state_root(&self) -> MerkleHash {
        self.client.chain.best_header().body.merkle_root_state
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        self.client.get_receipt_info(hash)
    }

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        let start = r.start.unwrap_or_else(|| self.client.chain.best_index());
        let limit = r.limit.unwrap_or(25);
        let blocks = self.client.chain.get_blocks_by_indices(start, limit);
        Ok(SignedShardBlocksResponse {
            blocks: blocks.into_iter().map(std::convert::Into::into).collect(),
        })
    }
}
