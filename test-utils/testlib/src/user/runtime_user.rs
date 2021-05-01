use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use near_crypto::{PublicKey, Signer};
use near_jsonrpc_primitives::errors::ServerError;
use near_primitives::errors::{RuntimeError, TxExecutionError};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeightDelta, MerkleHash};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    AccessKeyView, AccountView, BlockView, CallResult, ChunkView, ContractCodeView,
    ExecutionOutcomeView, ExecutionOutcomeWithIdView, ExecutionStatusView,
    FinalExecutionOutcomeView, FinalExecutionStatus, ViewApplyState, ViewStateResult,
};
use near_store::{ShardTries, TrieUpdate};
use neard::config::MIN_GAS_PRICE;
#[cfg(feature = "protocol_feature_evm")]
use neard::config::TESTNET_EVM_CHAIN_ID;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{ApplyState, Runtime};

use crate::user::{User, POISONED_LOCK_ERR};

/// Mock client without chain, used in RuntimeUser and RuntimeNode
pub struct MockClient {
    pub runtime: Runtime,
    pub tries: ShardTries,
    pub state_root: MerkleHash,
    pub epoch_length: BlockHeightDelta,
    pub runtime_config: RuntimeConfig,
}

impl MockClient {
    pub fn get_state_update(&self) -> TrieUpdate {
        self.tries.new_trie_update(0, self.state_root)
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
    pub transactions: RefCell<HashSet<SignedTransaction>>,
    pub epoch_info_provider: MockEpochInfoProvider,
    pub runtime_config: Arc<RuntimeConfig>,
}

impl RuntimeUser {
    pub fn new(account_id: &str, signer: Arc<dyn Signer>, client: Arc<RwLock<MockClient>>) -> Self {
        let runtime_config = Arc::new(client.read().unwrap().runtime_config.clone());
        RuntimeUser {
            signer,
            trie_viewer: TrieViewer::new_with_state_size_limit(None),
            account_id: account_id.to_string(),
            client,
            transaction_results: Default::default(),
            receipts: Default::default(),
            transactions: RefCell::new(Default::default()),
            epoch_info_provider: MockEpochInfoProvider::default(),
            runtime_config,
        }
    }

    pub fn apply_all(
        &self,
        apply_state: ApplyState,
        prev_receipts: Vec<Receipt>,
        transactions: Vec<SignedTransaction>,
    ) -> Result<(), ServerError> {
        let mut receipts = prev_receipts;
        for transaction in transactions.iter() {
            self.transactions.borrow_mut().insert(transaction.clone());
        }
        let mut txs = transactions;
        loop {
            let mut client = self.client.write().expect(POISONED_LOCK_ERR);
            let apply_result = client
                .runtime
                .apply(
                    client.tries.get_trie_for_shard(0),
                    client.state_root,
                    &None,
                    &apply_state,
                    &receipts,
                    &txs,
                    &self.epoch_info_provider,
                )
                .map_err(|e| match e {
                    RuntimeError::InvalidTxError(e) => {
                        ServerError::TxExecutionError(TxExecutionError::InvalidTxError(e))
                    }
                    RuntimeError::BalanceMismatchError(e) => panic!("{}", e),
                    RuntimeError::StorageError(e) => panic!("Storage error {:?}", e),
                    RuntimeError::UnexpectedIntegerOverflow => {
                        panic!("UnexpectedIntegerOverflow error")
                    }
                    RuntimeError::ReceiptValidationError(e) => panic!("{}", e),
                    RuntimeError::ValidatorError(e) => panic!("{}", e),
                })?;
            for outcome_with_id in apply_result.outcomes {
                self.transaction_results
                    .borrow_mut()
                    .insert(outcome_with_id.id, outcome_with_id.outcome.into());
            }
            client.tries.apply_all(&apply_result.trie_changes, 0).unwrap().0.commit().unwrap();
            client.state_root = apply_result.state_root;
            if apply_result.outgoing_receipts.is_empty() {
                return Ok(());
            }
            for receipt in apply_result.outgoing_receipts.iter() {
                self.receipts.borrow_mut().insert(receipt.receipt_id, receipt.clone());
            }
            receipts = apply_result.outgoing_receipts;
            txs = vec![];
        }
    }

    fn apply_state(&self) -> ApplyState {
        ApplyState {
            block_index: 1,
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            block_timestamp: 0,
            epoch_height: 0,
            gas_price: MIN_GAS_PRICE,
            gas_limit: None,
            random_seed: Default::default(),
            epoch_id: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: self.runtime_config.clone(),
            cache: None,
            is_new_chunk: true,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: TESTNET_EVM_CHAIN_ID,
            profile: Default::default(),
        }
    }

    fn get_recursive_transaction_results(
        &self,
        hash: &CryptoHash,
    ) -> Vec<ExecutionOutcomeWithIdView> {
        let outcome = self.get_transaction_result(hash);
        let receipt_ids = outcome.receipt_ids.clone();
        let mut transactions = vec![ExecutionOutcomeWithIdView {
            id: *hash,
            outcome,
            proof: vec![],
            block_hash: Default::default(),
        }];
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
        let transaction = self.transactions.borrow().get(hash).unwrap().clone().into();
        FinalExecutionOutcomeView {
            status,
            transaction,
            transaction_outcome: outcomes.pop().unwrap(),
            receipts_outcome: receipts,
        }
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

    fn view_contract_code(&self, account_id: &AccountId) -> Result<ContractCodeView, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer
            .view_contract_code(&state_update, account_id)
            .map(|contract_code| contract_code.into())
            .map_err(|err| err.to_string())
    }

    fn view_state(&self, account_id: &AccountId, prefix: &[u8]) -> Result<ViewStateResult, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer
            .view_state(&state_update, account_id, prefix)
            .map_err(|err| err.to_string())
    }

    fn view_call(
        &self,
        account_id: &AccountId,
        method_name: &str,
        args: &[u8],
    ) -> Result<CallResult, String> {
        let apply_state = self.apply_state();
        let client = self.client.read().expect(POISONED_LOCK_ERR);
        let state_update = client.get_state_update();
        let mut result = CallResult::default();
        let view_state = ViewApplyState {
            block_height: apply_state.block_index,
            prev_block_hash: apply_state.prev_block_hash,
            block_hash: apply_state.block_hash,
            epoch_id: apply_state.epoch_id,
            epoch_height: apply_state.epoch_height,
            block_timestamp: apply_state.block_timestamp,
            current_protocol_version: PROTOCOL_VERSION,
            cache: apply_state.cache,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: TESTNET_EVM_CHAIN_ID,
        };
        result.result = self
            .trie_viewer
            .call_function(
                state_update,
                view_state,
                account_id,
                method_name,
                args,
                &mut result.logs,
                &self.epoch_info_provider,
            )
            .map_err(|err| err.to_string())?;
        Ok(result)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), ServerError> {
        self.apply_all(self.apply_state(), vec![], vec![transaction])?;
        Ok(())
    }

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, ServerError> {
        self.apply_all(self.apply_state(), vec![], vec![transaction.clone()])?;
        Ok(self.get_transaction_final_result(&transaction.get_hash()))
    }

    fn add_receipt(&self, receipt: Receipt) -> Result<(), ServerError> {
        self.apply_all(self.apply_state(), vec![receipt], vec![])?;
        Ok(())
    }

    fn get_best_height(&self) -> Option<u64> {
        unimplemented!("get_best_height should not be implemented for RuntimeUser");
    }

    // This function is needed to sign transactions
    fn get_best_block_hash(&self) -> Option<CryptoHash> {
        Some(CryptoHash::default())
    }

    fn get_block(&self, _height: u64) -> Option<BlockView> {
        unimplemented!("get_block should not be implemented for RuntimeUser");
    }

    fn get_block_by_hash(&self, _block_hash: CryptoHash) -> Option<BlockView> {
        None
    }

    fn get_chunk(&self, _height: u64, _shard_id: u64) -> Option<ChunkView> {
        unimplemented!("get_chunk should not be implemented for RuntimeUser");
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
    ) -> Result<AccessKeyView, String> {
        let state_update = self.client.read().expect(POISONED_LOCK_ERR).get_state_update();
        self.trie_viewer
            .view_access_key(&state_update, account_id, public_key)
            .map(|access_key| access_key.into())
            .map_err(|err| err.to_string())
    }

    fn signer(&self) -> Arc<dyn Signer> {
        self.signer.clone()
    }

    fn set_signer(&mut self, signer: Arc<dyn Signer>) {
        self.signer = signer;
    }
}
