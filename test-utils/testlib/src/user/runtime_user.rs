use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use near_crypto::{PublicKey, Signer};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::transaction::{ExecutionOutcomeWithProof, SignedTransaction};
use near_primitives::types::{AccountId, BlockIndex, MerkleHash};
use near_primitives::views::{
    AccessKeyView, AccountView, BlockView, ExecutionOutcomeView, ExecutionOutcomeWithIdView,
    ExecutionStatusView, ViewStateResult,
};
use near_primitives::views::{FinalExecutionOutcomeView, FinalExecutionStatus};
use near_store::{Trie, TrieUpdate};
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{ApplyState, Runtime};

use crate::user::{User, POISONED_LOCK_ERR};
use near::config::INITIAL_GAS_PRICE;
use near_chain::types::ApplyTransactionResult;
use near_primitives::errors::RuntimeError;

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
    pub signer: Arc<dyn Signer>,
    pub trie_viewer: TrieViewer,
    pub client: Arc<RwLock<MockClient>>,
    // Store results of applying transactions/receipts
    pub transaction_results: RefCell<HashMap<CryptoHash, ExecutionOutcomeView>>,
    // store receipts generated when applying transactions
    pub receipts: RefCell<HashMap<CryptoHash, Receipt>>,
}

impl RuntimeUser {
    pub fn new(account_id: &str, signer: Arc<dyn Signer>, client: Arc<RwLock<MockClient>>) -> Self {
        RuntimeUser {
            signer,
            trie_viewer: TrieViewer::new(),
            account_id: account_id.to_string(),
            client,
            transaction_results: Default::default(),
            receipts: Default::default(),
        }
    }

    pub fn apply_all(
        &self,
        apply_state: ApplyState,
        prev_receipts: Vec<Receipt>,
        transactions: Vec<SignedTransaction>,
    ) -> Result<(), String> {
        let mut receipts = prev_receipts;
        let mut txs = transactions;
        loop {
            let mut client = self.client.write().expect(POISONED_LOCK_ERR);
            let apply_result = client
                .runtime
                .apply(
                    client.trie.clone(),
                    client.state_root,
                    &None,
                    &apply_state,
                    &receipts,
                    &txs,
                    &HashSet::new(),
                )
                .map_err(|e| match e {
                    RuntimeError::InvalidTxError(e) => format!("{}", e),
                    RuntimeError::BalanceMismatch(e) => panic!("{}", e),
                    RuntimeError::StorageError(e) => panic!("Storage error {:?}", e),
                    RuntimeError::UnexpectedIntegerOverflow => {
                        panic!("UnexpectedIntegerOverflow error")
                    }
                })?;
            let (_, proofs) =
                ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);
            for (outcome_with_id, proof) in apply_result.outcomes.into_iter().zip(proofs) {
                self.transaction_results.borrow_mut().insert(
                    outcome_with_id.id,
                    ExecutionOutcomeWithProof { outcome: outcome_with_id.outcome, proof }.into(),
                );
            }
            apply_result.trie_changes.into(client.trie.clone()).unwrap().0.commit().unwrap();
            client.state_root = apply_result.state_root;
            if apply_result.new_receipts.is_empty() {
                return Ok(());
            }
            for receipt in apply_result.new_receipts.iter() {
                self.receipts.borrow_mut().insert(receipt.receipt_id, receipt.clone());
            }
            receipts = apply_result.new_receipts;
            txs = vec![];
        }
    }

    fn apply_state(&self) -> ApplyState {
        let client = self.client.read().expect(POISONED_LOCK_ERR);
        ApplyState {
            block_index: 0,
            block_timestamp: 0,
            epoch_length: client.epoch_length,
            gas_price: INITIAL_GAS_PRICE,
            gas_limit: None,
        }
    }

    fn get_recursive_transaction_results(
        &self,
        hash: &CryptoHash,
    ) -> Vec<ExecutionOutcomeWithIdView> {
        let outcome = self.get_transaction_result(hash);
        let receipt_ids = outcome.receipt_ids.clone();
        let mut transactions = vec![ExecutionOutcomeWithIdView { id: (*hash).into(), outcome }];
        for hash in &receipt_ids {
            transactions
                .extend(self.get_recursive_transaction_results(&hash.clone().into()).into_iter());
        }
        transactions
    }

    fn get_final_transaction_result(&self, hash: &CryptoHash) -> FinalExecutionOutcomeView {
        let mut outcomes = self.get_recursive_transaction_results(hash);
        let mut looking_for_id = (*hash).into();
        let num_outcomes = outcomes.len();
        let status = outcomes
            .iter()
            .find_map(|outcome_with_id| {
                if outcome_with_id.id == looking_for_id {
                    match &outcome_with_id.outcome.status {
                        ExecutionStatusView::Unknown if num_outcomes == 1 => {
                            Some(FinalExecutionStatus::NotStarted)
                        }
                        ExecutionStatusView::Unknown => Some(FinalExecutionStatus::Started),
                        ExecutionStatusView::Failure(e) => {
                            Some(FinalExecutionStatus::Failure(e.clone()))
                        }
                        ExecutionStatusView::SuccessValue(v) => {
                            Some(FinalExecutionStatus::SuccessValue(v.clone()))
                        }
                        ExecutionStatusView::SuccessReceiptId(id) => {
                            looking_for_id = id.clone();
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .expect("results should resolve to a final outcome");
        let receipts = outcomes.split_off(1);
        FinalExecutionOutcomeView { status, transaction: outcomes.pop().unwrap(), receipts }
    }
}

impl User for RuntimeUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountView, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer
            .view_account(&state_update, account_id)
            .map(|account| account.into())
            .map_err(|err| err.to_string())
    }

    fn view_state(&self, account_id: &AccountId, prefix: &[u8]) -> Result<ViewStateResult, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer
            .view_state(&state_update, account_id, prefix)
            .map_err(|err| err.to_string())
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.apply_all(self.apply_state(), vec![], vec![transaction])?;
        Ok(())
    }

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, String> {
        self.apply_all(self.apply_state(), vec![], vec![transaction.clone()])?;
        Ok(self.get_transaction_final_result(&transaction.get_hash()))
    }

    fn add_receipt(&self, receipt: Receipt) -> Result<(), String> {
        self.apply_all(self.apply_state(), vec![receipt], vec![])?;
        Ok(())
    }

    fn get_best_block_index(&self) -> Option<u64> {
        unimplemented!("get_best_block_index should not be implemented for RuntimeUser");
    }

    // This function is needed to sign transactions
    fn get_best_block_hash(&self) -> Option<CryptoHash> {
        Some(CryptoHash::default())
    }

    fn get_block(&self, _index: u64) -> Option<BlockView> {
        unimplemented!("get_block should not be implemented for RuntimeUser");
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> ExecutionOutcomeView {
        self.transaction_results.borrow().get(hash).cloned().unwrap()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalExecutionOutcomeView {
        self.get_final_transaction_result(hash)
    }

    fn get_state_root(&self) -> CryptoHash {
        self.client.read().expect(POISONED_LOCK_ERR).state_root.into()
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKeyView>, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer
            .view_access_key(&state_update, account_id, public_key)
            .map(|value| value.map(|access_key| access_key.into()))
            .map_err(|err| err.to_string())
    }

    fn signer(&self) -> Arc<dyn Signer> {
        self.signer.clone()
    }

    fn set_signer(&mut self, signer: Arc<dyn Signer>) {
        self.signer = signer;
    }
}
