use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use tempdir::TempDir;

use lazy_static::lazy_static;
use near_chain::Block;
use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::crypto::signer::EDSigner;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptInfo};
use near_primitives::rpc::{AccountViewCallResult, ViewStateResult};
use near_primitives::transaction::{
    FinalTransactionResult, FinalTransactionStatus, SignedTransaction, TransactionLog,
    TransactionResult, TransactionStatus,
};
use near_primitives::types::{AccountId, BlockIndex, MerkleHash};
use near_store::{Trie, TrieUpdate};
use node_runtime::ethereum::EthashProvider;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{ApplyState, Runtime};

use crate::user::{User, POISONED_LOCK_ERR};

/// Mock client without chain, used in RuntimeUser and RuntimeNode
pub struct MockClient {
    pub runtime: Runtime,
    // Arc here because get_runtime_and_trie returns Arc<Trie> and
    // TrieUpdate takes Arc<Trie>.
    pub trie: Arc<Trie>,
    pub state_root: MerkleHash,
    pub epoch_length: BlockIndex,
}

impl MockClient {
    pub fn get_state_update(&self) -> TrieUpdate {
        TrieUpdate::new(self.trie.clone(), self.state_root)
    }
}

pub struct RuntimeUser {
    pub account_id: AccountId,
    pub signer: Arc<dyn EDSigner>,
    pub trie_viewer: TrieViewer,
    pub client: Arc<RwLock<MockClient>>,
    // Store results of applying transactions/receipts
    pub transaction_results: RefCell<HashMap<CryptoHash, TransactionResult>>,
    // store receipts generated when applying transactions
    pub receipts: RefCell<HashMap<CryptoHash, Receipt>>,
}

lazy_static! {
    static ref TEST_ETHASH_PROVIDER: Arc<Mutex<EthashProvider>> = Arc::new(Mutex::new(
        EthashProvider::new(TempDir::new("runtime_user_test_ethash").unwrap().path())
    ));
}

impl RuntimeUser {
    pub fn new(
        account_id: &str,
        signer: Arc<dyn EDSigner>,
        client: Arc<RwLock<MockClient>>,
    ) -> Self {
        let ethash_provider = TEST_ETHASH_PROVIDER.clone();
        RuntimeUser {
            signer,
            trie_viewer: TrieViewer::new(ethash_provider),
            account_id: account_id.to_string(),
            client,
            transaction_results: Default::default(),
            receipts: Default::default(),
        }
    }

    pub fn apply_all(
        &self,
        apply_state: ApplyState,
        prev_receipts: Vec<Vec<Receipt>>,
        transactions: Vec<SignedTransaction>,
    ) {
        let mut cur_apply_state = apply_state;
        let mut receipts = prev_receipts;
        let mut txs = transactions;
        loop {
            let mut client = self.client.write().expect(POISONED_LOCK_ERR);
            let state_update = TrieUpdate::new(client.trie.clone(), cur_apply_state.root);
            let mut apply_result =
                client.runtime.apply(state_update, &cur_apply_state, &receipts, &txs).unwrap();
            for transaction_result in apply_result.tx_result.into_iter() {
                self.transaction_results
                    .borrow_mut()
                    .insert(transaction_result.hash, transaction_result.result);
            }
            apply_result.trie_changes.into(client.trie.clone()).unwrap().0.commit().unwrap();
            if apply_result.new_receipts.is_empty() {
                client.state_root = apply_result.root;
                return;
            }
            cur_apply_state = ApplyState {
                root: apply_result.root,
                shard_id: cur_apply_state.shard_id,
                block_index: cur_apply_state.block_index,
                parent_block_hash: cur_apply_state.parent_block_hash,
                epoch_length: client.epoch_length,
            };
            let new_receipts: Vec<_> =
                apply_result.new_receipts.drain().flat_map(|(_, v)| v).collect();
            for receipt in new_receipts.iter() {
                self.receipts.borrow_mut().insert(receipt.receipt_id, receipt.clone());
            }
            receipts = vec![new_receipts];
            txs = vec![];
        }
    }

    fn apply_state(&self) -> ApplyState {
        let client = self.client.read().expect(POISONED_LOCK_ERR);
        ApplyState {
            root: client.state_root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0,
            epoch_length: client.epoch_length,
        }
    }

    fn get_recursive_transaction_results(&self, hash: &CryptoHash) -> Vec<TransactionLog> {
        let result = self.get_transaction_result(hash);
        let receipt_ids = result.receipts.clone();
        let mut transactions = vec![TransactionLog { hash: *hash, result }];
        for hash in &receipt_ids {
            transactions.extend(self.get_recursive_transaction_results(hash).into_iter());
        }
        transactions
    }

    fn get_final_transaction_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        let transactions = self.get_recursive_transaction_results(hash);
        let status = if transactions
            .iter()
            .find(|t| &t.result.status == &TransactionStatus::Failed)
            .is_some()
        {
            FinalTransactionStatus::Failed
        } else if transactions
            .iter()
            .find(|t| &t.result.status == &TransactionStatus::Unknown)
            .is_some()
        {
            FinalTransactionStatus::Started
        } else {
            FinalTransactionStatus::Completed
        };
        FinalTransactionResult { status, transactions }
    }
}

impl User for RuntimeUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer.view_account(&state_update, account_id).map_err(|err| err.to_string())
    }

    fn view_state(&self, account_id: &AccountId, prefix: &[u8]) -> Result<ViewStateResult, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer
            .view_state(&state_update, account_id, prefix)
            .map_err(|err| err.to_string())
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.apply_all(self.apply_state(), vec![], vec![transaction]);
        Ok(())
    }

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalTransactionResult, String> {
        self.apply_all(self.apply_state(), vec![], vec![transaction.clone()]);
        Ok(self.get_transaction_final_result(&transaction.get_hash()))
    }

    fn add_receipt(&self, receipt: Receipt) -> Result<(), String> {
        self.apply_all(self.apply_state(), vec![vec![receipt]], vec![]);
        Ok(())
    }

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64> {
        self.view_account(account_id).ok().map(|account| account.nonce)
    }

    fn get_best_block_index(&self) -> Option<u64> {
        unimplemented!("get_best_block_index should not be implemented for RuntimeUser");
    }

    fn get_block(&self, _index: u64) -> Option<Block> {
        unimplemented!("get_block should not be implemented for RuntimeUser");
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.transaction_results.borrow().get(hash).cloned().unwrap()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        self.get_final_transaction_result(hash)
    }

    fn get_state_root(&self) -> MerkleHash {
        self.client.read().expect(POISONED_LOCK_ERR).state_root
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        let receipt = self.receipts.borrow().get(hash).cloned()?;
        let transaction_result = self.transaction_results.borrow().get(hash).cloned()?;
        Some(ReceiptInfo { receipt, result: transaction_result, block_index: Default::default() })
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKey>, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer
            .view_access_key(&state_update, account_id, public_key)
            .map_err(|err| err.to_string())
    }

    fn signer(&self) -> Arc<dyn EDSigner> {
        self.signer.clone()
    }

    fn set_signer(&mut self, signer: Arc<dyn EDSigner>) {
        self.signer = signer;
    }
}
