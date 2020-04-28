use near_chain::types::ApplyTransactionResult;
use near_primitives::{
    errors::RuntimeError,
    hash::CryptoHash,
    receipt::Receipt,
    transaction::SignedTransaction,
    types::{Balance, BlockHeight, Gas, StateChangeCause},
};
use near_runtime_configs::RuntimeConfig;
use near_store::{Store, Trie, TrieUpdate, WrappedTrieChanges};
use node_runtime::{verify_and_charge_transaction, ApplyState, Runtime};
use std::collections::HashMap;
use std::sync::Arc;

struct GenesisConfig {
    pub genesis_time: u64,
    pub gas_price: Balance,
    pub gas_limit: Gas,
    pub runtime_config: RuntimeConfig,
}
struct Block {
    prev_block: Option<Box<Block>>,
    state_root: CryptoHash,
    transactions: Vec<SignedTransaction>,
    receipts: Vec<Receipt>,
    block_height: BlockHeight,
    block_timestamp: u64,
    gas_price: Balance,
    gas_limit: Gas,
    gas_burnt: Gas,
}

impl Block {
    pub fn genesis(genesis_config: GenesisConfig) -> Self {
        Self {
            prev_block: None,
            state_root: CryptoHash::default(),
            transactions: vec![],
            receipts: vec![],
            block_height: 1,
            block_timestamp: genesis_config.genesis_time,
            gas_price: genesis_config.gas_price,
            gas_limit: genesis_config.gas_limit,
            gas_burnt: 0,
        }
    }

    pub fn process(&mut self, runtime: &RuntimeStandalone) -> Result<Block, RuntimeError> {
        let apply_state = ApplyState {
            block_index: self.block_height,
            epoch_length: 0, // TODO: support for epochs
            epoch_height: self.block_height,
            gas_price: self.gas_price,
            block_timestamp: self.block_timestamp,
            gas_limit: Some(self.gas_limit),
        };

        let apply_result = runtime.runtime().apply(
            runtime.trie(),
            self.state_root,
            &None,
            &apply_state,
            &self.receipts,
            &self.transactions,
        )?;

        let total_gas_burnt =
            apply_result.outcomes.iter().map(|tx_result| tx_result.outcome.gas_burnt).sum();

        let result = ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(
                runtime.trie(),
                apply_result.trie_changes,
                apply_result.state_changes,
                Default::default(),
            ),
            new_root: apply_result.state_root,
            outcomes: apply_result.outcomes,
            receipt_result: HashMap::new(), // no shard support
            validator_proposals: apply_result.validator_proposals,
            total_gas_burnt,
            total_validator_reward: apply_result.stats.total_validator_reward,
            total_balance_burnt: apply_result.stats.total_balance_burnt
                + apply_result.stats.total_balance_slashed,
            proof: None,
        };
        Ok(Block {})
    }

    pub fn add_tx(
        &mut self,
        runtime: &RuntimeStandalone,
        tx: SignedTransaction,
    ) -> Result<(), RuntimeError> {
        let mut state_update = TrieUpdate::new(runtime.trie(), self.state_root);
        let transactions_gas_limit = self.gas_limit / 2;

        match verify_and_charge_transaction(
            &runtime.runtime_config(),
            &mut state_update,
            self.gas_price,
            &tx,
        ) {
            Ok(verification_result) => {
                state_update.commit(StateChangeCause::NotWritableToDisk);
                self.transactions.push(tx);
                self.gas_burnt += verification_result.gas_burnt;
                Ok(())
            }
            Err(RuntimeError::InvalidTxError(err)) => {
                state_update.rollback();
                Err(RuntimeError::InvalidTxError(err))
            }
            Err(RuntimeError::StorageError(err)) => {
                panic!("Storage error: {:?}", err);
            }
            Err(err) => unreachable!("Unexpected RuntimeError error {:?}", err),
        }
    }
}

struct RuntimeStandalone {
    genesis: GenesisConfig,
    // TODO: use the actual TransactionPool impl
    tx_pool: Vec<SignedTransaction>,
    cur_block: Arc<Block>,
    runtime: Runtime,
    trie: Arc<Trie>,
    tx_db: HashMap<CryptoHash, SignedTransaction>,
}

impl RuntimeStandalone {
    fn new(genesis: GenesisConfig, store: Arc<Store>) -> Self {
        let genesis_block = Block::genesis(genesis);
        Self {
            trie: Arc::new(Trie::new(store)),
            runtime: Runtime::new(genesis.runtime_config.clone()),
            genesis,
            cur_block: genesis_block,
            blocks: vec![genesis_block],
            tx_pool: vec![],
        }
    }

    pub fn send_tx(&self, tx: SignedTransaction) {
        self.tx_pool.push(tx);
        self.tx_db.insert
    }

    pub fn runtime_config(&self) -> RuntimeConfig {
        self.genesis.runtime_config
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn trie(&self) -> Arc<Trie> {
        self.trie.clone()
    }

    fn charge() {}

    fn create_block(&mut self) {
        // self.blocks.
    }

    fn add_tx(&mut self, tx: SignedTransaction) {
        self.tx_pool.push(tx);
    }
}
