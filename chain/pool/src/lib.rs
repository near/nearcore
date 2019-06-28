use std::collections::btree_map::BTreeMap;
use std::collections::HashMap;

use near_chain::ValidTransaction;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Nonce};

pub use crate::types::Error;

pub mod types;

/// Transaction pool: keeps track of transactions that were not yet accepted into the block chain.
pub struct TransactionPool {
    num_transactions: usize,
    /// Transactions grouped by account and ordered by nonce.
    pub transactions: HashMap<AccountId, BTreeMap<Nonce, SignedTransaction>>,
}

impl TransactionPool {
    pub fn new() -> Self {
        TransactionPool { num_transactions: 0, transactions: HashMap::default() }
    }

    /// Insert a valid transaction into the pool that passed validation.
    pub fn insert_transaction(&mut self, valid_transaction: ValidTransaction) {
        let account = valid_transaction.transaction.body.get_originator();
        let nonce = valid_transaction.transaction.body.get_nonce();
        self.num_transactions += 1;
        self.transactions
            .entry(account)
            .or_insert_with(BTreeMap::new)
            .insert(nonce, valid_transaction.transaction);
    }

    /// Take transactions from the pool, in the appropriate order to be put in a new block.
    /// Ensure that on average they will fit into expected weight.
    pub fn prepare_transactions(
        &mut self,
        expected_weight: u32,
    ) -> Result<Vec<SignedTransaction>, Error> {
        // TODO: pack transactions better.
        let result = self
            .transactions
            .values()
            .flat_map(BTreeMap::values)
            .take(expected_weight as usize)
            .cloned()
            .collect();
        Ok(result)
    }

    /// Quick reconciliation step - evict all transactions that already in the block
    /// or became invalid after it.
    pub fn remove_transactions(&mut self, transactions: &Vec<SignedTransaction>) {
        for transaction in transactions.iter() {
            let account = transaction.body.get_originator();
            let nonce = transaction.body.get_nonce();
            let mut remove_map = false;
            if let Some(map) = self.transactions.get_mut(&account) {
                map.remove(&nonce);
                remove_map = map.is_empty();
            }
            if remove_map {
                self.num_transactions -= 1;
                self.transactions.remove(&account);
            }
        }
    }

    /// Reintroduce transactions back during the reorg
    pub fn reintroduce_transactions(&mut self, transactions: &Vec<SignedTransaction>) {
        for transaction in transactions.iter() {
            let transaction = transaction.clone();
            self.insert_transaction(ValidTransaction { transaction });
        }
    }

    pub fn len(&self) -> usize {
        self.num_transactions
    }
}

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use near_chain::ValidTransaction;
    use near_primitives::crypto::signer::InMemorySigner;
    use near_primitives::transaction::TransactionBody;

    use crate::TransactionPool;
    use near_primitives::types::Balance;

    /// Add transactions of nonce from 1..10 in random order. Check that mempool
    /// orders them correctly.
    #[test]
    fn test_order_nonce() {
        let signer = InMemorySigner::from_seed("alice.near", "alice.near");
        let mut transactions: Vec<_> = (1..10)
            .map(|i| {
                TransactionBody::send_money(i, "alice.near", "bob.near", i as Balance).sign(&signer)
            })
            .collect();
        let mut pool = TransactionPool::new();
        let mut rng = thread_rng();
        transactions.shuffle(&mut rng);
        for tx in transactions {
            pool.insert_transaction(ValidTransaction { transaction: tx });
        }
        let transactions = pool.prepare_transactions(10).unwrap();
        let nonces: Vec<u64> = transactions.iter().map(|tx| tx.body.get_nonce()).collect();
        assert_eq!(nonces, (1..10).collect::<Vec<u64>>())
    }

}
