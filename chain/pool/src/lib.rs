use std::collections::{HashMap, HashSet};

use near_crypto::PublicKey;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;

pub use crate::types::Error;
use near_primitives::hash::CryptoHash;
use std::cell::Cell;

pub mod types;

type TxMap = HashMap<(AccountId, PublicKey), Vec<SignedTransaction>>;

/// Transaction pool: keeps track of transactions that were not yet accepted into the block chain.
#[derive(Default)]
pub struct TransactionPool {
    /// Transactions grouped by a pair of (account ID, signer public key).
    /// It's more efficient to keep transactions unsorted and with potentially conflicting nonce
    /// than create a BTreeMap for every transaction on average.
    pub transactions: Cell<TxMap>,
    /// Set of all hashes to quickly check if the given transaction is in the pool.
    pub unique_transactions: HashSet<CryptoHash>,
}

impl TransactionPool {
    /// Insert a signed transaction into the pool that passed validation.
    pub fn insert_transaction(&mut self, signed_transaction: SignedTransaction) {
        if self.unique_transactions.contains(&signed_transaction.get_hash()) {
            return;
        }
        self.unique_transactions.insert(signed_transaction.get_hash());
        let signer_id = signed_transaction.transaction.signer_id.clone();
        let signer_public_key = signed_transaction.transaction.public_key.clone();
        self.transactions
            .get_mut()
            .entry((signer_id, signer_public_key))
            .or_insert_with(Vec::new)
            .push(signed_transaction);
    }

    /// Returns a draining structure that pulls transactions from the pool in the proper order.
    /// It has an option to take transactions with the same key as the last one or with the new key.
    /// When the iterator is dropped, the rest of the transactions remains in the pool.
    pub fn draining_iterator(&mut self) -> DrainingIterator {
        DrainingIterator::new(self)
    }

    /// Take `min(self.len(), max_number_of_transactions)` transactions from the pool, in the
    /// appropriate order. We first take one transaction per key of (AccountId, PublicKey) with
    /// the lowest nonce, then we take the next transaction per key with the lowest nonce.
    pub fn prepare_transactions(
        &mut self,
        max_number_of_transactions: u32,
    ) -> Result<Vec<SignedTransaction>, Error> {
        let mut res = vec![];
        let mut iter = self.draining_iterator();
        for _ in 0..max_number_of_transactions {
            if let Some(tx) = iter.next(false) {
                res.push(tx);
            } else {
                break;
            }
        }
        Ok(res)
    }

    /// Quick reconciliation step - evict all transactions that already in the block
    /// or became invalid after it.
    pub fn remove_transactions(&mut self, transactions: &[SignedTransaction]) {
        let mut grouped_transactions = HashMap::new();
        for tx in transactions {
            if self.unique_transactions.contains(&tx.get_hash()) {
                let signer_id = &tx.transaction.signer_id;
                let signer_public_key = &tx.transaction.public_key;
                grouped_transactions
                    .entry((signer_id, signer_public_key))
                    .or_insert_with(HashSet::new)
                    .insert(tx.get_hash());
            }
        }
        for (key, hashes) in grouped_transactions {
            let key = (key.0.clone(), key.1.clone());
            let mut remove_entry = false;
            if let Some(v) = self.transactions.get_mut().get_mut(&key) {
                v.retain(|tx| !hashes.contains(&tx.get_hash()));
                remove_entry = v.is_empty();
            }
            if remove_entry {
                self.transactions.get_mut().remove(&key);
            }
            for hash in hashes {
                self.unique_transactions.remove(&hash);
            }
        }
    }

    /// Reintroduce transactions back during the chain reorg
    pub fn reintroduce_transactions(&mut self, transactions: Vec<SignedTransaction>) {
        for tx in transactions {
            self.insert_transaction(tx);
        }
    }

    pub fn len(&self) -> usize {
        self.unique_transactions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.unique_transactions.is_empty()
    }
}

pub struct DrainingIterator<'a> {
    pool: &'a mut TransactionPool,
    sorted: bool,
    current_map: Cell<TxMap>,
    next_map: Cell<TxMap>,
    last_entry: Option<((AccountId, PublicKey), Vec<SignedTransaction>)>,
}

impl<'a> DrainingIterator<'a> {
    pub fn new(pool: &'a mut TransactionPool) -> Self {
        let current_map = Default::default();
        pool.transactions.swap(&current_map);
        Self { pool, sorted: false, current_map, next_map: Default::default(), last_entry: None }
    }

    /// Returns the next transaction in the proper order. `same_key` defines whether the
    /// transaction should be from the same group (with the same key) as the previous transaction.
    /// If the previous transaction was invalid, the next transaction has to be with the same key to
    /// maintain the proper order. Otherwise the invalid transaction skips the given key.
    fn next(&mut self, same_key: bool) -> Option<SignedTransaction> {
        // If we need the new/next key, or the current transaction group is fully used.
        if !same_key || self.last_entry.is_none() {
            if let Some((key, txs)) = self.last_entry.take() {
                self.next_map.get_mut().insert(key, txs);
            }
            if self.current_map.get_mut().is_empty() {
                self.sorted = true;
                self.current_map.swap(&self.next_map);
            }
            let key = if let Some(key) = self.current_map.get_mut().keys().next() {
                key.clone()
            } else {
                return None;
            };
            self.last_entry = self.current_map.get_mut().remove_entry(&key);
            if !self.sorted {
                // Sort by nonce in non-increasing order to pop from the end
                self.last_entry
                    .as_mut()
                    .unwrap()
                    .1
                    .sort_by_key(|a| std::cmp::Reverse(a.transaction.nonce));
            }
        }

        if self.last_entry.is_some() {
            let tx = self
                .last_entry
                .as_mut()
                .unwrap()
                .1
                .pop()
                .expect("transaction groups shouldn't be empty");
            if self.last_entry.as_ref().unwrap().1.is_empty() {
                self.last_entry = None;
            }
            self.pool.unique_transactions.remove(&tx.get_hash());
            Some(tx)
        } else {
            None
        }
    }
}

impl<'a> Drop for DrainingIterator<'a> {
    fn drop(&mut self) {
        if let Some((key, txs)) = self.last_entry.take() {
            self.current_map.get_mut().insert(key, txs);
        }
        self.pool.transactions.swap(&self.current_map);
        self.pool.transactions.get_mut().extend(self.next_map.get_mut().drain());
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

    fn generate_transactions(
        signer_id: &str,
        signer_seed: &str,
        starting_nonce: u64,
        end_nonce: u64,
    ) -> Vec<SignedTransaction> {
        let signer =
            Arc::new(InMemorySigner::from_seed(signer_seed, KeyType::ED25519, signer_seed));
        (starting_nonce..=end_nonce)
            .map(|i| {
                SignedTransaction::send_money(
                    i,
                    signer_id.to_string(),
                    "bob.near".to_string(),
                    &*signer,
                    i as Balance,
                    CryptoHash::default(),
                )
            })
            .collect()
    }

    fn process_txs_to_nonces(
        mut transactions: Vec<SignedTransaction>,
        expected_weight: u32,
    ) -> (Vec<u64>, TransactionPool) {
        let mut pool = TransactionPool::default();
        let mut rng = thread_rng();
        transactions.shuffle(&mut rng);
        for tx in transactions {
            pool.insert_transaction(tx);
        }
        (
            pool.prepare_transactions(expected_weight)
                .unwrap()
                .iter()
                .map(|tx| tx.transaction.nonce)
                .collect(),
            pool,
        )
    }

    fn sort_pairs(a: &mut [u64]) {
        for c in a.chunks_exact_mut(2) {
            if c[0] > c[1] {
                c.swap(0, 1);
            }
        }
    }

    /// Add transactions of nonce from 1..10 in random order. Check that mempool
    /// orders them correctly.
    #[test]
    fn test_order_nonce() {
        let transactions = generate_transactions("alice.near", "alice.near", 1, 10);
        let (nonces, _) = process_txs_to_nonces(transactions, 10);
        assert_eq!(nonces, (1..=10).collect::<Vec<u64>>());
    }

    /// Add transactions of nonce from 1..10 in random order from 2 signers. Check that mempool
    /// orders them correctly.
    #[test]
    fn test_order_nonce_two_signers() {
        let mut transactions = generate_transactions("alice.near", "alice.near", 1, 10);
        transactions.extend(generate_transactions("bob.near", "bob.near", 1, 10));

        let (nonces, _) = process_txs_to_nonces(transactions, 10);
        assert_eq!(nonces, (1..=5).map(|a| vec![a; 2]).flatten().collect::<Vec<u64>>());
    }

    /// Add transactions of nonce from 1..10 in random order from the same account but with
    /// different public keys.
    #[test]
    fn test_order_nonce_same_account_two_access_keys_variable_nonces() {
        let mut transactions = generate_transactions("alice.near", "alice.near", 1, 10);
        transactions.extend(generate_transactions("alice.near", "bob.near", 21, 30));

        let (mut nonces, _) = process_txs_to_nonces(transactions, 10);
        sort_pairs(&mut nonces[..]);
        assert_eq!(nonces, (1..=5).map(|a| vec![a, a + 20]).flatten().collect::<Vec<u64>>());
    }

    /// Add transactions of nonce from 1..=3 and transactions with nonce 21..=31. Pull 10.
    /// Then try to get another 10.
    #[test]
    fn test_retain() {
        let mut transactions = generate_transactions("alice.near", "alice.near", 1, 3);
        transactions.extend(generate_transactions("alice.near", "bob.near", 21, 31));

        let (mut nonces, mut pool) = process_txs_to_nonces(transactions, 10);
        sort_pairs(&mut nonces[..6]);
        assert_eq!(nonces, vec![1, 21, 2, 22, 3, 23, 24, 25, 26, 27]);
        let nonces: Vec<u64> =
            pool.prepare_transactions(10).unwrap().iter().map(|tx| tx.transaction.nonce).collect();
        assert_eq!(nonces, vec![28, 29, 30, 31]);
    }

    #[test]
    fn test_remove_transactions() {
        let n = 100;
        let mut transactions = (1..=n)
            .map(|i| {
                let signer_seed = format!("user_{}", i % 3);
                let signer = Arc::new(InMemorySigner::from_seed(
                    &signer_seed,
                    KeyType::ED25519,
                    &signer_seed,
                ));
                let signer_id = format!("user_{}", i % 5);
                SignedTransaction::send_money(
                    i,
                    signer_id.to_string(),
                    "bob.near".to_string(),
                    &*signer,
                    i as Balance,
                    CryptoHash::default(),
                )
            })
            .collect::<Vec<_>>();

        let mut pool = TransactionPool::default();
        let mut rng = thread_rng();
        transactions.shuffle(&mut rng);
        for tx in transactions.clone() {
            println!("{:?}", tx);
            pool.insert_transaction(tx);
        }
        assert_eq!(pool.len(), n as usize);

        transactions.shuffle(&mut rng);
        let (txs_to_remove, txs_to_check) = transactions.split_at(transactions.len() / 2);
        pool.remove_transactions(txs_to_remove);

        assert_eq!(pool.len(), txs_to_check.len());

        let mut pool_txs = pool.prepare_transactions(txs_to_check.len() as u32).unwrap();
        pool_txs.sort_by_key(|tx| tx.transaction.nonce);
        let mut expected_txs = txs_to_check.to_vec();
        expected_txs.sort_by_key(|tx| tx.transaction.nonce);

        assert_eq!(pool_txs, expected_txs);
    }

    /// Add transactions of nonce from 1..=3 and transactions with nonce 21..=31. Pull 10.
    /// Then try to get another 10.
    #[test]
    fn test_draining_iterator() {
        let mut transactions = generate_transactions("alice.near", "alice.near", 1, 3);
        transactions.extend(generate_transactions("alice.near", "bob.near", 21, 31));

        let (nonces, mut pool) = process_txs_to_nonces(transactions, 0);
        assert!(nonces.is_empty());
        let mut res = vec![];
        let mut iter = pool.draining_iterator();
        loop {
            let mut same_key = false;
            let tx = loop {
                if let Some(tx) = iter.next(same_key) {
                    if tx.transaction.nonce & 1 == 1 {
                        break Some(tx);
                    } else {
                        same_key = true;
                    }
                } else {
                    break None;
                }
            };
            if let Some(tx) = tx {
                res.push(tx);
            } else {
                break;
            }
        }
        let mut nonces: Vec<_> = res.into_iter().map(|tx| tx.transaction.nonce).collect();
        sort_pairs(&mut nonces[..4]);
        assert_eq!(nonces, vec![1, 21, 3, 23, 25, 27, 29, 31]);
    }
}
