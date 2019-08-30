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
        let signer_id = valid_transaction.transaction.transaction.signer_id.clone();
        let nonce = valid_transaction.transaction.transaction.nonce;
        self.num_transactions += 1;
        self.transactions
            .entry(signer_id)
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
            let signer_id = &transaction.transaction.signer_id;
            let nonce = transaction.transaction.nonce;
            let mut remove_map = false;
            if let Some(map) = self.transactions.get_mut(signer_id) {
                map.remove(&nonce);
                remove_map = map.is_empty();
            }
            if remove_map {
                self.num_transactions -= 1;
                self.transactions.remove(signer_id);
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
    use std::sync::Arc;

    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use near_chain::ValidTransaction;
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::transaction::SignedTransaction;

    use crate::TransactionPool;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::Balance;

    /// Add transactions of nonce from 1..10 in random order. Check that mempool
    /// orders them correctly.
    #[test]
    fn test_order_nonce() {
        let signer =
            Arc::new(InMemorySigner::from_seed("alice.near", KeyType::ED25519, "alice.near"));
        let mut transactions: Vec<_> = (1..10)
            .map(|i| {
                SignedTransaction::send_money(
                    i,
                    "alice.near".to_string(),
                    "bob.near".to_string(),
                    signer.clone(),
                    i as Balance,
                    CryptoHash::default(),
                )
            })
            .collect();
        let mut pool = TransactionPool::new();
        let mut rng = thread_rng();
        transactions.shuffle(&mut rng);
        for tx in transactions {
            pool.insert_transaction(ValidTransaction { transaction: tx });
        }
        let transactions = pool.prepare_transactions(10).unwrap();
        let nonces: Vec<u64> = transactions.iter().map(|tx| tx.transaction.nonce).collect();
        assert_eq!(nonces, (1..10).collect::<Vec<u64>>())
    }

}
