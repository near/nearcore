use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_pool::{types::PoolIterator, TransactionPool};
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::types::{AccountInfo, EpochId, EpochInfoProvider};
use near_primitives::{account::AccessKey, test_utils::account_new};
use near_primitives::{
    account::Account,
    errors::RuntimeError,
    hash::CryptoHash,
    receipt::Receipt,
    state_record::StateRecord,
    transaction::{ExecutionOutcome, ExecutionStatus, SignedTransaction},
    types::AccountId,
    types::{Balance, BlockHeight, EpochHeight, Gas, StateChangeCause},
};
use near_runtime_configs::RuntimeConfig;
use near_store::{
    get_access_key, get_account, set_account, test_utils::create_test_store, ShardTries, Store,
};
use node_runtime::{state_viewer::TrieViewer, ApplyState, Runtime};
use std::collections::HashMap;
use std::sync::Arc;

const DEFAULT_EPOCH_LENGTH: u64 = 3;

pub fn init_runtime_and_signer(root_account_id: &AccountId) -> (RuntimeStandalone, InMemorySigner) {
    let mut genesis = GenesisConfig::default();
    let signer = genesis.init_root_signer(root_account_id);
    (RuntimeStandalone::new_with_store(genesis), signer)
}

#[derive(Debug)]
pub struct GenesisConfig {
    pub genesis_time: u64,
    pub gas_price: Balance,
    pub gas_limit: Gas,
    pub genesis_height: u64,
    pub epoch_length: u64,
    pub runtime_config: RuntimeConfig,
    pub state_records: Vec<StateRecord>,
    pub validators: Vec<AccountInfo>,
}

impl Default for GenesisConfig {
    fn default() -> Self {
        Self {
            genesis_time: 0,
            gas_price: 0,
            gas_limit: std::u64::MAX,
            genesis_height: 0,
            epoch_length: DEFAULT_EPOCH_LENGTH,
            runtime_config: RuntimeConfig::default(),
            state_records: vec![],
            validators: vec![],
        }
    }
}

impl GenesisConfig {
    pub fn init_root_signer(&mut self, account_id: &AccountId) -> InMemorySigner {
        let signer = InMemorySigner::from_seed(account_id, KeyType::ED25519, "test");
        let root_account = account_new(std::u128::MAX, CryptoHash::default());

        self.state_records
            .push(StateRecord::Account { account_id: account_id.clone(), account: root_account });
        self.state_records.push(StateRecord::AccessKey {
            account_id: account_id.clone(),
            public_key: signer.public_key(),
            access_key: AccessKey::full_access(),
        });
        signer
    }
}

#[derive(Debug, Default, Clone)]
pub struct Block {
    prev_block: Option<Box<Block>>,
    state_root: CryptoHash,
    gas_burnt: Gas,
    pub epoch_height: EpochHeight,
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    pub gas_price: Balance,
    pub gas_limit: Gas,
}

impl Block {
    pub fn genesis(genesis_config: &GenesisConfig) -> Self {
        Self {
            prev_block: None,
            state_root: CryptoHash::default(),
            block_height: genesis_config.genesis_height,
            epoch_height: 0,
            block_timestamp: genesis_config.genesis_time,
            gas_price: genesis_config.gas_price,
            gas_limit: genesis_config.gas_limit,
            gas_burnt: 0,
        }
    }

    pub fn produce(&self, new_state_root: CryptoHash, epoch_length: u64) -> Block {
        Self {
            gas_price: self.gas_price,
            gas_limit: self.gas_limit,
            block_timestamp: self.block_timestamp + 1,
            prev_block: Some(Box::new(self.clone())),
            state_root: new_state_root,
            block_height: self.block_height + 1,
            epoch_height: (self.block_height + 1) / epoch_length,
            gas_burnt: 0,
        }
    }
}

pub struct RuntimeStandalone {
    genesis: GenesisConfig,
    tx_pool: TransactionPool,
    transactions: HashMap<CryptoHash, SignedTransaction>,
    outcomes: HashMap<CryptoHash, ExecutionOutcome>,
    cur_block: Block,
    runtime: Runtime,
    tries: ShardTries,
    pending_receipts: Vec<Receipt>,
    epoch_info_provider: Box<dyn EpochInfoProvider>,
}

impl RuntimeStandalone {
    pub fn new(genesis: GenesisConfig, store: Arc<Store>) -> Self {
        let mut genesis_block = Block::genesis(&genesis);
        let mut store_update = store.store_update();
        let runtime = Runtime::new(genesis.runtime_config.clone());
        let tries = ShardTries::new(store, 1);
        let (s_update, state_root) =
            runtime.apply_genesis_state(tries.clone(), 0, &[], &genesis.state_records);
        store_update.merge(s_update);
        store_update.commit().unwrap();
        genesis_block.state_root = state_root;
        let validators = genesis.validators.clone();
        Self {
            genesis,
            tries,
            runtime,
            transactions: HashMap::new(),
            outcomes: HashMap::new(),
            cur_block: genesis_block,
            tx_pool: TransactionPool::new(),
            pending_receipts: vec![],
            epoch_info_provider: Box::new(MockEpochInfoProvider::new(
                validators.into_iter().map(|info| (info.account_id, info.amount)),
            )),
        }
    }

    pub fn new_with_store(genesis: GenesisConfig) -> Self {
        RuntimeStandalone::new(genesis, create_test_store())
    }

    /// Processes blocks until the final value is produced
    pub fn resolve_tx(
        &mut self,
        mut tx: SignedTransaction,
    ) -> Result<ExecutionOutcome, RuntimeError> {
        tx.init();
        let mut outcome_hash = tx.get_hash();
        self.transactions.insert(outcome_hash, tx.clone());
        self.tx_pool.insert_transaction(tx);
        loop {
            self.produce_block()?;
            if let Some(outcome) = self.outcomes.get(&outcome_hash) {
                match outcome.status {
                    ExecutionStatus::Unknown => unreachable!(), // ExecutionStatus::Unknown is not relevant for a standalone runtime
                    ExecutionStatus::SuccessReceiptId(ref id) => outcome_hash = *id,
                    ExecutionStatus::SuccessValue(_) | ExecutionStatus::Failure(_) => {
                        return Ok(outcome.clone())
                    }
                };
            } else if self.pending_receipts.is_empty() {
                unreachable!("Lost an outcome for the receipt hash {}", outcome_hash);
            }
        }
    }

    /// Just puts tx into the transaction pool
    pub fn send_tx(&mut self, tx: SignedTransaction) -> CryptoHash {
        let tx_hash = tx.get_hash();
        self.transactions.insert(tx_hash, tx.clone());
        self.tx_pool.insert_transaction(tx);
        tx_hash
    }

    pub fn outcome(&self, hash: &CryptoHash) -> Option<ExecutionOutcome> {
        self.outcomes.get(hash).cloned()
    }

    /// Processes all transactions and pending receipts until there is no pending_receipts left
    pub fn process_all(&mut self) -> Result<(), RuntimeError> {
        loop {
            self.produce_block()?;
            if self.pending_receipts.len() == 0 {
                return Ok(());
            }
        }
    }

    /// Processes one block. Populates outcomes and producining new pending_receipts.
    pub fn produce_block(&mut self) -> Result<(), RuntimeError> {
        let apply_state = ApplyState {
            block_index: self.cur_block.block_height,
            epoch_height: self.cur_block.epoch_height,
            gas_price: self.cur_block.gas_price,
            block_timestamp: self.cur_block.block_timestamp,
            gas_limit: None,
            // not used
            last_block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
        };

        let apply_result = self.runtime.apply(
            self.tries.get_trie_for_shard(0),
            self.cur_block.state_root,
            &None,
            &apply_state,
            &self.pending_receipts,
            &Self::prepare_transactions(&mut self.tx_pool),
            self.epoch_info_provider.as_ref(),
        )?;
        self.pending_receipts = apply_result.outgoing_receipts;
        apply_result.outcomes.iter().for_each(|outcome| {
            self.outcomes.insert(outcome.id, outcome.outcome.clone());
        });
        let (update, _) =
            self.tries.apply_all(&apply_result.trie_changes, 0).expect("Unexpected Storage error");
        update.commit().expect("Unexpected io error");
        self.cur_block = self.cur_block.produce(apply_result.state_root, self.genesis.epoch_length);

        Ok(())
    }

    /// Produce num_of_blocks blocks.
    /// # Examples
    ///
    /// ```
    /// use near_runtime_standalone::init_runtime_and_signer;
    /// let (mut runtime, _) = init_runtime_and_signer(&"root".into());
    /// runtime.produce_blocks(5);
    /// assert_eq!(runtime.current_block().block_height, 5);
    /// assert_eq!(runtime.current_block().epoch_height, 1);
    ///```

    pub fn produce_blocks(&mut self, num_of_blocks: u64) -> Result<(), RuntimeError> {
        for _ in 0..num_of_blocks {
            self.produce_block()?;
        }
        Ok(())
    }

    /// Force alter account and change state_root.
    pub fn force_account_update(&mut self, account_id: AccountId, account: &Account) {
        let mut trie_update = self.tries.new_trie_update(0, self.cur_block.state_root);
        set_account(&mut trie_update, account_id, account);
        trie_update.commit(StateChangeCause::ValidatorAccountsUpdate);
        let (trie_changes, _) = trie_update.finalize().expect("Unexpected Storage error");
        let (store_update, new_root) = self.tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().expect("No io errors expected");
        self.cur_block.state_root = new_root;
    }

    pub fn view_account(&self, account_id: &AccountId) -> Option<Account> {
        let trie_update = self.tries.new_trie_update(0, self.cur_block.state_root);
        get_account(&trie_update, account_id).expect("Unexpected Storage error")
    }

    pub fn view_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Option<AccessKey> {
        let trie_update = self.tries.new_trie_update(0, self.cur_block.state_root);
        get_access_key(&trie_update, account_id, public_key).expect("Unexpected Storage error")
    }

    /// Outputs return_value and logs
    pub fn view_method_call(
        &self,
        account_id: &AccountId,
        method_name: &str,
        args: &[u8],
    ) -> Result<(Vec<u8>, Vec<String>), Box<dyn std::error::Error>> {
        let trie_update = self.tries.new_trie_update(0, self.cur_block.state_root);
        let viewer = TrieViewer {};
        let mut logs = vec![];
        let result = viewer.call_function(
            trie_update,
            self.cur_block.block_height,
            self.cur_block.block_timestamp,
            &CryptoHash::default(),
            self.cur_block.epoch_height,
            &EpochId::default(),
            account_id,
            method_name,
            args,
            &mut logs,
            self.epoch_info_provider.as_ref(),
        )?;
        Ok((result, logs))
    }

    pub fn current_block(&mut self) -> &mut Block {
        &mut self.cur_block
    }

    pub fn pending_receipts(&self) -> &[Receipt] {
        &self.pending_receipts
    }

    fn prepare_transactions(tx_pool: &mut TransactionPool) -> Vec<SignedTransaction> {
        let mut res = vec![];
        let mut pool_iter = tx_pool.pool_iterator();
        loop {
            if let Some(iter) = pool_iter.next() {
                if let Some(tx) = iter.next() {
                    res.push(tx);
                }
            } else {
                break;
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_block() {
        let (mut runtime, signer) = init_runtime_and_signer(&"root".into());
        let hash = runtime.send_tx(SignedTransaction::create_account(
            1,
            signer.account_id.clone(),
            "alice".into(),
            100,
            signer.public_key(),
            &signer,
            CryptoHash::default(),
        ));
        runtime.produce_block().unwrap();
        assert!(matches!(
            runtime.outcome(&hash),
            Some(ExecutionOutcome { status: ExecutionStatus::SuccessReceiptId(_), .. })
        ));
    }

    #[test]
    fn process_all() {
        let (mut runtime, signer) = init_runtime_and_signer(&"root".into());
        assert_eq!(runtime.view_account(&"alice".into()), None);
        let outcome = runtime.resolve_tx(SignedTransaction::create_account(
            1,
            signer.account_id.clone(),
            "alice".into(),
            165437999999999999999000,
            signer.public_key(),
            &signer,
            CryptoHash::default(),
        ));
        assert!(matches!(
            outcome,
            Ok(ExecutionOutcome { status: ExecutionStatus::SuccessValue(_), .. })
        ));
        assert_eq!(
            runtime.view_account(&"alice".into()),
            Some(Account {
                amount: 165437999999999999999000,
                code_hash: CryptoHash::default(),
                locked: 0,
                storage_usage: 182,
            })
        );
    }

    #[test]
    fn test_cross_contract_call() {
        let (mut runtime, signer) = init_runtime_and_signer(&"root".into());

        assert!(matches!(
            runtime.resolve_tx(SignedTransaction::create_contract(
                1,
                signer.account_id.clone(),
                "status".into(),
                include_bytes!("../contracts/status-message/res/status_message.wasm")
                    .as_ref()
                    .into(),
                23082408900000000000001000,
                signer.public_key(),
                &signer,
                CryptoHash::default(),
            )),
            Ok(ExecutionOutcome { status: ExecutionStatus::SuccessValue(_), .. })
        ));

        assert!(matches!(
            runtime.resolve_tx(SignedTransaction::create_contract(
                2,
                signer.account_id.clone(),
                "caller".into(),
                include_bytes!(
                    "../contracts/cross-contract-high-level/res/cross_contract_high_level.wasm"
                )
                .as_ref()
                .into(),
                23082408900000000000001000,
                signer.public_key(),
                &signer,
                CryptoHash::default(),
            )),
            Ok(ExecutionOutcome { status: ExecutionStatus::SuccessValue(_), .. })
        ));

        assert!(matches!(
            runtime.resolve_tx(SignedTransaction::call(
                3,
                signer.account_id.clone(),
                "caller".into(),
                &signer,
                0,
                "simple_call".into(),
                "{\"account_id\": \"status\", \"message\": \"caller status is ok!\"}"
                    .as_bytes()
                    .to_vec(),
                300_000_000_000_000,
                CryptoHash::default(),
            )),
            Ok(ExecutionOutcome { status: ExecutionStatus::SuccessValue(_), .. })
        ));

        runtime.process_all().unwrap();

        let caller_status = String::from_utf8(
            runtime
                .view_method_call(
                    &"status".into(),
                    "get_status",
                    "{\"account_id\": \"caller\"}".as_bytes(),
                )
                .unwrap()
                .0,
        )
        .unwrap();
        assert_eq!("\"caller status is ok!\"", caller_status);
    }

    #[test]
    fn test_force_update_account() {
        let (mut runtime, _) = init_runtime_and_signer(&"root".into());
        let mut bob_account = runtime.view_account(&"root".into()).unwrap();
        bob_account.locked = 10000;
        runtime.force_account_update("root".into(), &bob_account);
        assert_eq!(runtime.view_account(&"root".into()).unwrap().locked, 10000);
    }
}
