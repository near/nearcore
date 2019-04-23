use crate::runtime_utils::to_receipt_block;
use crate::user::{User, POISONED_LOCK_ERR};
use node_http::types::{GetBlocksByIndexRequest, SignedShardBlocksResponse};
use node_runtime::state_viewer::{AccountViewCallResult, TrieViewer, ViewStateResult};
use node_runtime::{ApplyState, Runtime};
use primitives::chain::ReceiptBlock;
use primitives::hash::CryptoHash;
use primitives::transaction::{
    FinalTransactionResult, FinalTransactionStatus, ReceiptTransaction, SignedTransaction,
    TransactionLogs, TransactionResult, TransactionStatus,
};
use primitives::types::{AccountId, MerkleHash, Nonce};
use shard::ReceiptInfo;
use storage::{Trie, TrieUpdate};

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Mock client without chain, used in RuntimeUser and RuntimeNode
pub struct MockClient {
    pub runtime: Runtime,
    // Arc here because get_runtime_and_trie returns Arc<Trie> and
    // TrieUpdate takes Arc<Trie>.
    pub trie: Arc<Trie>,
    pub state_root: MerkleHash,
}

impl MockClient {
    pub fn get_state_update(&self) -> TrieUpdate {
        TrieUpdate::new(self.trie.clone(), self.state_root)
    }
}

pub struct RuntimeUser {
    pub account_id: AccountId,
    pub nonce: RefCell<Nonce>,
    pub trie_viewer: TrieViewer,
    pub client: Arc<RwLock<MockClient>>,
    // Store results of applying transactions/receipts
    pub transaction_results: RefCell<HashMap<CryptoHash, TransactionResult>>,
    // store receipts generated when applying transactions
    pub receipts: RefCell<HashMap<CryptoHash, ReceiptTransaction>>,
}

impl RuntimeUser {
    pub fn new(account_id: &str, client: Arc<RwLock<MockClient>>) -> Self {
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
            let mut client = self.client.write().expect(POISONED_LOCK_ERR);
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
                self.receipts.borrow_mut().insert(receipt.nonce, receipt.clone());
            }
            receipts = vec![to_receipt_block(new_receipts)];
            txs = vec![];
        }
    }

    fn collect_transaction_final_result(
        &self,
        transaction_result: &TransactionResult,
        logs: &mut Vec<TransactionLogs>,
    ) -> FinalTransactionStatus {
        match transaction_result.status {
            TransactionStatus::Unknown => FinalTransactionStatus::Unknown,
            TransactionStatus::Failed => FinalTransactionStatus::Failed,
            TransactionStatus::Completed => {
                for r in transaction_result.receipts.iter() {
                    let receipt_result = self.get_transaction_result(&r);
                    logs.push(TransactionLogs {
                        hash: *r,
                        lines: receipt_result.logs.clone(),
                        receipts: receipt_result.receipts.clone(),
                        result: receipt_result.result.clone(),
                    });
                    match self.collect_transaction_final_result(&receipt_result, logs) {
                        FinalTransactionStatus::Failed => return FinalTransactionStatus::Failed,
                        FinalTransactionStatus::Completed => {}
                        _ => return FinalTransactionStatus::Started,
                    };
                }
                FinalTransactionStatus::Completed
            }
        }
    }
}

impl User for RuntimeUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer.view_account(&state_update, account_id)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer.view_state(&state_update, account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        let apply_state = ApplyState {
            root: self.client.read().expect(POISONED_LOCK_ERR).state_root,
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
            root: self.client.read().expect(POISONED_LOCK_ERR).state_root,
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

    fn get_best_block_index(&self) -> Option<u64> {
        unimplemented!("get_best_block_index should not be implemented for RuntimeUser");
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.transaction_results.borrow().get(hash).cloned().unwrap()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        let transaction_result = self.get_transaction_result(hash);
        let mut result = FinalTransactionResult {
            status: FinalTransactionStatus::Unknown,
            logs: vec![TransactionLogs {
                hash: *hash,
                lines: transaction_result.logs.clone(),
                receipts: transaction_result.receipts.clone(),
                result: transaction_result.result.clone(),
            }],
        };
        result.status =
            self.collect_transaction_final_result(&transaction_result, &mut result.logs);
        result
    }

    fn get_state_root(&self) -> MerkleHash {
        self.client.read().expect(POISONED_LOCK_ERR).state_root
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        let receipt = self.receipts.borrow().get(hash).cloned()?;
        let transaction_result = self.transaction_results.borrow().get(hash).cloned()?;
        Some(ReceiptInfo { receipt, result: transaction_result, block_index: Default::default() })
    }

    fn get_shard_blocks_by_index(
        &self,
        _r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        unimplemented!("get_shard_blocks_by_index should not be implemented for RuntimeUser");
    }
}
