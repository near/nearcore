use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use near_crypto::{PublicKey, Signer};
use near_jsonrpc_primitives::errors::ServerError;
use near_parameters::RuntimeConfig;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::congestion_info::{BlockCongestionInfo, ExtendedCongestionInfo};
use near_primitives::errors::{RuntimeError, TxExecutionError};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockHeightDelta, MerkleHash, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    AccessKeyView, AccountView, BlockView, CallResult, ChunkView, ContractCodeView,
    ExecutionOutcomeView, ExecutionOutcomeWithIdView, ExecutionStatusView,
    FinalExecutionOutcomeView, FinalExecutionStatus, ViewStateResult,
};
use near_store::adapter::StoreUpdateAdapter;
use near_store::{ShardTries, TrieUpdate};
use node_runtime::SignedValidPeriodTransactions;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{ApplyState, Runtime, state_viewer::ViewApplyState};
use parking_lot::RwLock;

use crate::user::User;
use near_primitives::shard_layout::{ShardLayout, ShardUId};

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
        self.tries.new_trie_update(ShardUId::single_shard(), self.state_root)
    }
}

pub struct RuntimeUser {
    pub account_id: AccountId,
    pub signer: Arc<Signer>,
    pub trie_viewer: TrieViewer,
    pub client: Arc<RwLock<MockClient>>,
    // Store results of applying transactions/receipts
    pub transaction_results: RefCell<HashMap<CryptoHash, ExecutionOutcomeView>>,
    // store receipts generated when applying transactions
    pub receipts: RefCell<HashMap<CryptoHash, Receipt>>,
    pub transactions: RefCell<HashSet<SignedTransaction>>,
    pub epoch_info_provider: MockEpochInfoProvider,
    pub runtime_config: Arc<RuntimeConfig>,
    pub gas_price: Balance,
}

impl RuntimeUser {
    pub fn new(
        account_id: AccountId,
        signer: Arc<Signer>,
        client: Arc<RwLock<MockClient>>,
        gas_price: Balance,
    ) -> Self {
        let runtime_config = Arc::new(client.read().runtime_config.clone());
        RuntimeUser {
            signer,
            trie_viewer: TrieViewer::default(),
            account_id,
            client,
            transaction_results: Default::default(),
            receipts: Default::default(),
            transactions: RefCell::new(Default::default()),
            epoch_info_provider: MockEpochInfoProvider::default(),
            runtime_config,
            gas_price,
        }
    }

    pub fn apply_all(
        &self,
        mut apply_state: ApplyState,
        prev_receipts: Vec<Receipt>,
        transactions: Vec<SignedTransaction>,
        use_flat_storage: bool,
    ) -> Result<(), ServerError> {
        let mut receipts = prev_receipts;
        for transaction in &transactions {
            self.transactions.borrow_mut().insert(transaction.clone());
        }
        let mut txs = transactions;
        loop {
            let mut client = self.client.write();
            let trie = if use_flat_storage {
                client.tries.get_trie_with_block_hash_for_shard(
                    ShardUId::single_shard(),
                    client.state_root,
                    &CryptoHash::default(),
                    false,
                )
            } else {
                let shard_uid = ShardUId::single_shard();
                let mut trie = client.tries.get_trie_for_shard(shard_uid, client.state_root);
                trie.set_use_trie_accounting_cache(true);
                trie
            };
            let validity_check_results = vec![true; txs.len()];
            let apply_result = client
                .runtime
                .apply(
                    trie,
                    &None,
                    &apply_state,
                    &receipts,
                    SignedValidPeriodTransactions::new(txs, validity_check_results),
                    &self.epoch_info_provider,
                    Default::default(),
                )
                .map_err(|e| match e {
                    RuntimeError::InvalidTxError(e) => {
                        ServerError::TxExecutionError(TxExecutionError::InvalidTxError(e))
                    }
                    RuntimeError::StorageError(e) => panic!("Storage error {:?}", e),
                    RuntimeError::UnexpectedIntegerOverflow(reason) => {
                        panic!("UnexpectedIntegerOverflow error {reason}")
                    }
                    RuntimeError::ReceiptValidationError(e) => panic!("{}", e),
                    RuntimeError::ValidatorError(e) => panic!("{}", e),
                })?;
            for outcome_with_id in apply_result.outcomes {
                self.transaction_results
                    .borrow_mut()
                    .insert(outcome_with_id.id, outcome_with_id.outcome.into());
            }
            let mut update = client.tries.store_update();
            client.tries.apply_all(
                &apply_result.trie_changes,
                ShardUId::single_shard(),
                &mut update,
            );
            if use_flat_storage {
                near_store::flat::FlatStateChanges::from_state_changes(&apply_result.state_changes)
                    .apply_to_flat_state(&mut update.flat_store_update(), ShardUId::single_shard());
            }
            update.commit().unwrap();
            client.state_root = apply_result.state_root;
            for receipt in &apply_result.outgoing_receipts {
                self.receipts.borrow_mut().insert(*receipt.receipt_id(), receipt.clone());
            }
            receipts = apply_result.outgoing_receipts;
            txs = vec![];

            apply_state.bandwidth_requests = BlockBandwidthRequests {
                shards_bandwidth_requests: [(
                    apply_state.shard_id,
                    apply_result.bandwidth_requests,
                )]
                .into_iter()
                .collect(),
            };
            let mut have_queued_receipts = false;
            if let Some(congestion_info) = apply_result.congestion_info {
                if congestion_info.receipt_bytes() > 0 {
                    have_queued_receipts = true;
                }
                apply_state.congestion_info.insert(
                    apply_state.shard_id,
                    ExtendedCongestionInfo { missed_chunks_count: 0, congestion_info },
                );
            }
            if receipts.is_empty() && !have_queued_receipts {
                return Ok(());
            }
        }
    }

    fn apply_state(&self) -> ApplyState {
        let shard_layout = ShardLayout::single_shard();
        let shard_ids = shard_layout.shard_ids().collect_vec();
        let shard_id = *shard_ids.first().unwrap();

        let congestion_info =
            shard_ids.into_iter().map(|id| (id, ExtendedCongestionInfo::default())).collect();
        let congestion_info = BlockCongestionInfo::new(congestion_info);

        ApplyState {
            apply_reason: ApplyChunkReason::UpdateTrackedShard,
            block_height: 1,
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            block_timestamp: 0,
            shard_id,
            epoch_height: 0,
            gas_price: self.gas_price,
            gas_limit: None,
            random_seed: Default::default(),
            epoch_id: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: self.runtime_config.clone(),
            cache: None,
            is_new_chunk: true,
            congestion_info,
            bandwidth_requests: BlockBandwidthRequests::empty(),
            trie_access_tracker_state: Default::default(),
        }
    }

    fn get_recursive_transaction_results(
        &self,
        hash: &CryptoHash,
    ) -> Vec<ExecutionOutcomeWithIdView> {
        let outcome = match self.get_transaction_result(hash) {
            Some(outcome) => outcome,
            None => {
                return vec![];
            }
        };

        let receipt_ids = outcome.receipt_ids.clone();
        let mut transactions = vec![ExecutionOutcomeWithIdView {
            id: *hash,
            outcome,
            proof: vec![],
            block_hash: Default::default(),
        }];
        for hash in &receipt_ids {
            transactions.extend(self.get_recursive_transaction_results(hash).into_iter());
        }
        transactions
    }

    // TODO(#10942) get rid of copy pasted code, it's outdated comparing to the original
    fn get_final_transaction_result(&self, hash: &CryptoHash) -> Option<FinalExecutionOutcomeView> {
        let mut outcomes = self.get_recursive_transaction_results(hash);
        let mut looking_for_id = *hash;
        let num_outcomes = outcomes.len();
        let status = outcomes.iter().find_map(|outcome_with_id| {
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
                        looking_for_id = *id;
                        None
                    }
                }
            } else {
                None
            }
        });
        // If we don't find the transaction at all, it must have been ignored due to having
        // been an invalid transaction (but due to relaxed validation we do not invalidate the
        // entire chunk.)
        let status = status?;
        let receipts = outcomes.split_off(1);
        let transaction = self.transactions.borrow().get(hash).unwrap().clone().into();
        Some(FinalExecutionOutcomeView {
            status,
            transaction,
            transaction_outcome: outcomes.pop().unwrap(),
            receipts_outcome: receipts,
        })
    }
}

impl User for RuntimeUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountView, String> {
        let state_update = self.client.read().get_state_update();
        self.trie_viewer
            .view_account(&state_update, account_id)
            .map(|account| account.into())
            .map_err(|err| err.to_string())
    }

    fn view_contract_code(&self, account_id: &AccountId) -> Result<ContractCodeView, String> {
        let state_update = self.client.read().get_state_update();
        self.trie_viewer
            .view_contract_code(&state_update, account_id)
            .map(|contract_code| {
                let hash = *contract_code.hash();
                ContractCodeView { hash, code: contract_code.into_code() }
            })
            .map_err(|err| err.to_string())
    }

    fn view_state(&self, account_id: &AccountId, prefix: &[u8]) -> Result<ViewStateResult, String> {
        let state_update = self.client.read().get_state_update();
        self.trie_viewer
            .view_state(&state_update, account_id, prefix, false)
            .map_err(|err| err.to_string())
    }

    fn is_locked(&self, account_id: &AccountId) -> Result<bool, String> {
        let state_update = self.client.read().get_state_update();
        self.trie_viewer
            .view_access_keys(&state_update, account_id)
            .map(|access_keys| access_keys.is_empty())
            .map_err(|err| err.to_string())
    }

    fn view_call(
        &self,
        account_id: &AccountId,
        method_name: &str,
        args: &[u8],
    ) -> Result<CallResult, String> {
        // TODO(congestion_control) - Set shard id somehow.
        let shard_id = ShardId::new(0);

        let apply_state = self.apply_state();
        let client = self.client.read();
        let state_update = client.get_state_update();
        let mut result = CallResult::default();
        let view_state = ViewApplyState {
            block_height: apply_state.block_height,
            prev_block_hash: apply_state.prev_block_hash,
            block_hash: apply_state.block_hash,
            shard_id,
            epoch_id: apply_state.epoch_id,
            epoch_height: apply_state.epoch_height,
            block_timestamp: apply_state.block_timestamp,
            current_protocol_version: PROTOCOL_VERSION,
            cache: apply_state.cache,
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
        self.apply_all(self.apply_state(), vec![], vec![transaction], true)?;
        Ok(())
    }

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, super::CommitError> {
        self.apply_all(self.apply_state(), vec![], vec![transaction.clone()], true)
            .map_err(super::CommitError::Server)?;
        self.get_transaction_final_result(&transaction.get_hash())
            .ok_or(super::CommitError::OutcomeNotFound)
    }

    fn add_receipts(
        &self,
        receipts: Vec<Receipt>,
        use_flat_storage: bool,
    ) -> Result<(), ServerError> {
        self.apply_all(self.apply_state(), receipts, vec![], use_flat_storage)?;
        Ok(())
    }

    fn get_best_height(&self) -> Option<u64> {
        unimplemented!("get_best_height should not be implemented for RuntimeUser");
    }

    // This function is needed to sign transactions
    fn get_best_block_hash(&self) -> Option<CryptoHash> {
        Some(CryptoHash::default())
    }

    fn get_block_by_height(&self, _height: u64) -> Option<BlockView> {
        unimplemented!("get_block should not be implemented for RuntimeUser");
    }

    fn get_block(&self, _block_hash: CryptoHash) -> Option<BlockView> {
        None
    }

    fn get_chunk_by_height(&self, _height: u64, _shard_id: ShardId) -> Option<ChunkView> {
        unimplemented!("get_chunk should not be implemented for RuntimeUser");
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> Option<ExecutionOutcomeView> {
        self.transaction_results.borrow().get(hash).cloned()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> Option<FinalExecutionOutcomeView> {
        self.get_final_transaction_result(hash)
    }

    fn get_state_root(&self) -> CryptoHash {
        self.client.read().state_root
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKeyView, String> {
        let state_update = self.client.read().get_state_update();
        self.trie_viewer
            .view_access_key(&state_update, account_id, public_key)
            .map(|access_key| access_key.into())
            .map_err(|err| err.to_string())
    }

    fn signer(&self) -> Arc<Signer> {
        self.signer.clone()
    }

    fn set_signer(&mut self, signer: Arc<Signer>) {
        self.signer = signer;
    }
}
