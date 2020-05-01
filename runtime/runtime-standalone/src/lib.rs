use near_pool::{types::PoolIterator, TransactionPool};
use near_primitives::{
    errors::RuntimeError,
    hash::CryptoHash,
    receipt::Receipt,
    state_record::StateRecord,
    transaction::{ExecutionOutcome, SignedTransaction},
    types::{Balance, BlockHeight, Gas},
};
use near_runtime_configs::RuntimeConfig;
use near_store::{Store, Trie, TrieUpdate};
use node_runtime::{ApplyState, Runtime};

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct GenesisConfig {
    pub genesis_time: u64,
    pub gas_price: Balance,
    pub gas_limit: Gas,
    pub runtime_config: RuntimeConfig,
    pub state_records: Vec<StateRecord>,
}

#[derive(Debug, Default, Clone)]
struct Block {
    prev_block: Option<Box<Block>>,
    pub state_root: CryptoHash,
    transactions: Vec<SignedTransaction>,
    receipts: Vec<Receipt>,
    block_height: BlockHeight,
    block_timestamp: u64,
    gas_price: Balance,
    gas_limit: Gas,
    gas_burnt: Gas,
}

impl Block {
    pub fn genesis(genesis_config: &GenesisConfig) -> Self {
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

    pub fn produce(&self, new_state_root: CryptoHash) -> Block {
        Self {
            gas_price: self.gas_price,
            gas_limit: self.gas_limit,
            block_timestamp: self.block_timestamp + 1,
            prev_block: Some(Box::new(self.clone())),
            state_root: new_state_root,
            transactions: vec![],
            receipts: vec![],
            block_height: 1,
            gas_burnt: 0,
        }
    }
}

pub struct RuntimeStandalone {
    tx_pool: TransactionPool,
    transactions: HashMap<CryptoHash, SignedTransaction>,
    outcomes: HashMap<CryptoHash, ExecutionOutcome>,
    cur_block: Block,
    runtime: Runtime,
    trie: Arc<Trie>,
    receipts_pool: Vec<Receipt>,
}

impl RuntimeStandalone {
    pub fn new(genesis: GenesisConfig, store: Arc<Store>) -> Self {
        let mut genesis_block = Block::genesis(&genesis);
        let mut store_update = store.store_update();
        let runtime = Runtime::new(genesis.runtime_config.clone());
        let trie = Arc::new(Trie::new(store));
        let trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        let (s_update, state_root) =
            runtime.apply_genesis_state(trie_update, &[], &genesis.state_records);
        store_update.merge(s_update);
        store_update.commit().unwrap();
        genesis_block.state_root = state_root;
        Self {
            trie,
            runtime,
            transactions: HashMap::new(),
            outcomes: HashMap::new(),
            cur_block: genesis_block,
            tx_pool: TransactionPool::new(),
            receipts_pool: vec![],
        }
    }

    pub fn run_tx(&mut self, mut tx: SignedTransaction) -> Result<ExecutionOutcome, RuntimeError> {
        tx.init();
        let tx_hash = tx.get_hash();
        self.transactions.insert(tx_hash, tx.clone());
        self.tx_pool.insert_transaction(tx);
        self.process()?;
        Ok(self
            .outcomes
            .get(&tx_hash)
            .expect("successful self.process() guaranies to have outcome for a tx")
            .clone())
    }

    pub fn process(&mut self) -> Result<(), RuntimeError> {
        let apply_state = ApplyState {
            block_index: self.cur_block.block_height,
            epoch_length: 0, // TODO: support for epochs
            epoch_height: self.cur_block.block_height,
            gas_price: self.cur_block.gas_price,
            block_timestamp: self.cur_block.block_timestamp,
            gas_limit: Some(self.cur_block.gas_limit),
        };

        let apply_result = self.runtime.apply(
            self.trie.clone(),
            self.cur_block.state_root,
            &None,
            &apply_state,
            &self.receipts_pool,
            &Self::prepare_transactions(&mut self.tx_pool),
        )?;
        self.receipts_pool = vec![];
        self.outcomes = apply_result
            .outcomes
            .iter()
            .map(|outcome| (outcome.id, outcome.outcome.clone()))
            .collect();

        self.cur_block = self.cur_block.produce(apply_result.state_root);

        Ok(())
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
    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::{account::AccessKey, test_utils::account_new};
    use near_store::test_utils::create_test_store;
    #[test]
    fn should_work() {
        let mut genesis = GenesisConfig::default();
        let root_account = account_new(164438000000000000001000, CryptoHash::default());
        let signer = InMemorySigner::from_seed("bob".into(), KeyType::ED25519, "test");

        genesis
            .state_records
            .push(StateRecord::Account { account_id: "bob".into(), account: root_account });
        genesis.state_records.push(StateRecord::AccessKey {
            account_id: "bob".into(),
            public_key: signer.public_key(),
            access_key: AccessKey::full_access(),
        });

        let mut runtime = RuntimeStandalone::new(genesis, create_test_store());
        let outcome = runtime.run_tx(SignedTransaction::create_account(
            1,
            "bob".into(),
            "alice".into(),
            100,
            signer.public_key(),
            &signer,
            CryptoHash::default(),
        ));
        println!("{:?}", outcome);
    }
}
