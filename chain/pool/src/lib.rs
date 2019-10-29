use std::collections::{HashMap, HashSet};

use near_crypto::PublicKey;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;

pub use crate::types::Error;
use near_primitives::hash::CryptoHash;

pub mod types;

/// Transaction pool: keeps track of transactions that were not yet accepted into the block chain.
#[derive(Default)]
pub struct TransactionPool {
    /// Transactions grouped by a pair of (account ID, signer public key).
    /// It's more efficient to keep transactions unsorted and with potentially conflicting nonce
    /// than create a BTreeMap for every transaction on average.
    pub transactions: HashMap<(AccountId, PublicKey), Vec<SignedTransaction>>,
    /// Set of all hashes to quickly check if the given transaction is in the pool.
    pub unique_transactions: HashSet<CryptoHash>,
}

impl TransactionPool {
    /// Insert a signed transaction into the pool that passed validation.
    pub fn insert_transaction(&mut self, signed_transaction: SignedTransaction) {
        if self.unique_transactions.contains(&signed_transaction.get_hash()) {
            return;
        }
        let signer_id = signed_transaction.transaction.signer_id.clone();
        let signer_public_key = signed_transaction.transaction.public_key.clone();
        self.transactions
            .entry((signer_id, signer_public_key))
            .or_insert_with(Vec::new)
            .push(signed_transaction);
    }

    /// Take transactions from the pool, in the appropriate order to be put in a new block.
    /// We first take one transaction per key of (AccountId, PublicKey) with the lowest nonce,
    /// then we take the next transaction per key with the lowest nonce.
    pub fn prepare_transactions(
        &mut self,
        expected_weight: u32,
    ) -> Result<Vec<SignedTransaction>, Error> {
        let mut sorted = false;
        let mut result = vec![];
        while result.len() < expected_weight as usize && !self.transactions.is_empty() {
            let mut keys_to_remove = vec![];
            for (key, txs) in self.transactions.iter_mut() {
                if !sorted {
                    // Reverse sort by nonce (non-increasing)
                    txs.sort_by(|a, b| b.transaction.nonce.cmp(&a.transaction.nonce));
                }
                let tx = txs.pop().expect("transaction groups shouldn't be empty");
                if txs.is_empty() {
                    keys_to_remove.push(key.clone());
                }
                result.push(tx);
                if result.len() >= expected_weight as usize {
                    break;
                }
            }
            sorted = true;
            // Removing empty keys
            keys_to_remove.into_iter().for_each(|key| {
                self.transactions.remove(&key);
            });
        }
        result.iter().for_each(|tx| {
            self.unique_transactions.remove(&tx.get_hash());
        });
        Ok(result)
    }

    /// Quick reconciliation step - evict all transactions that already in the block
    /// or became invalid after it.
    pub fn remove_transactions(&mut self, transactions: &Vec<SignedTransaction>) {
        transactions
            .iter()
            .filter(|tx| self.unique_transactions.contains(&tx.get_hash()))
            .fold(HashMap::<_, HashSet<_>>::new(), |mut acc, tx| {
                let signer_id = &tx.transaction.signer_id;
                let signer_public_key = &tx.transaction.public_key;
                acc.entry((signer_id, signer_public_key))
                    .or_insert_with(HashSet::new)
                    .insert(tx.get_hash());
                acc
            })
            .into_iter()
            .for_each(|(key, hashes)| {
                let key = (key.0.clone(), key.1.clone());
                let mut remove_entry = false;
                if let Some(v) = self.transactions.get_mut(&key) {
                    v.retain(|tx| !hashes.contains(&tx.get_hash()));
                    remove_entry = v.is_empty();
                }
                if remove_entry {
                    self.transactions.remove(&key);
                }
                hashes.iter().for_each(|hash| {
                    self.unique_transactions.remove(hash);
                });
            });
    }

    /// Reintroduce transactions back during the reorg
    pub fn reintroduce_transactions(&mut self, transactions: Vec<SignedTransaction>) {
        transactions.into_iter().for_each(|tx| {
            self.insert_transaction(tx);
        });
    }

    pub fn len(&self) -> usize {
        self.unique_transactions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.unique_transactions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::seq::SliceRandom;
    use rand::thread_rng;

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
                    &*signer,
                    i as Balance,
                    CryptoHash::default(),
                )
            })
            .collect();
        let mut pool = TransactionPool::default();
        let mut rng = thread_rng();
        transactions.shuffle(&mut rng);
        for tx in transactions {
            pool.insert_transaction(tx);
        }
        let transactions = pool.prepare_transactions(10).unwrap();
        let nonces: Vec<u64> = transactions.iter().map(|tx| tx.transaction.nonce).collect();
        assert_eq!(nonces, (1..10).collect::<Vec<u64>>())
    }
}
