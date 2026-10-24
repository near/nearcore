use crate::grouped_pool::{GroupedPool, GroupedPoolIterator, PoolMetrics};
use crate::item::GroupId;
use crate::types::{TransactionGroup, TransactionGroupIterator};
use near_primitives::epoch_info::RngSeed;
use near_primitives::transaction::{SignedTransaction, ValidatedTransaction};

mod grouped_pool;
mod item;
mod metrics;
pub mod types;

#[derive(Debug, PartialEq)]
pub enum InsertTransactionResult {
    /// Transaction was successfully inserted.
    Success,
    /// Transaction is already in the pool.
    Duplicate,
    /// Not enough space to fit the transaction.
    NoSpaceLeft,
}

/// Transaction pool: keeps track of transactions that were not yet accepted into the block chain.
pub struct TransactionPool {
    /// Signed transactions are grouped by a triple of (account ID, signer public key, nonce
    /// index).
    signed: GroupedPool<ValidatedTransaction>,
    /// If set, new transactions that bring the size of the pool over this limit will be rejected.
    total_transaction_size_limit: Option<u64>,
}

impl TransactionPool {
    pub fn new(
        key_seed: RngSeed,
        total_transaction_size_limit: Option<u64>,
        metrics_label: &str,
    ) -> Self {
        let metrics = PoolMetrics::new(
            &metrics::TRANSACTION_POOL_COUNT,
            &metrics::TRANSACTION_POOL_SIZE,
            metrics_label,
        );
        Self { signed: GroupedPool::new(key_seed, metrics), total_transaction_size_limit }
    }

    /// Inserts a signed transaction that passed validation into the pool.
    pub fn insert_transaction(
        &mut self,
        validated_tx: ValidatedTransaction,
    ) -> InsertTransactionResult {
        self.signed.insert(validated_tx, self.total_transaction_size_limit)
    }

    /// Returns a pool iterator wrapper that implements an iterator-like trait to iterate over
    /// transaction groups in the proper order defined by the protocol.
    /// When the iterator is dropped, all remaining groups are inserted back into the pool.
    pub fn pool_iterator(&mut self) -> PoolIteratorWrapper<'_> {
        PoolIteratorWrapper::new(self)
    }

    /// Removes given transactions from the pool.
    ///
    /// In practice, used to evict transactions that have already been included into the block or
    /// became invalid.
    pub fn remove_transactions<'a>(
        &mut self,
        signed_txs: impl IntoIterator<Item = &'a SignedTransaction>,
    ) {
        self.signed.remove_items(signed_txs.into_iter().map(|signed_tx| {
            let tx = &signed_tx.transaction;
            let group = GroupId::AccessKey {
                account_id: tx.signer_id(),
                public_key: tx.public_key(),
                nonce_index: tx.nonce().nonce_index(),
            };
            (signed_tx.get_hash(), group)
        }));
    }

    /// Returns the number of unique transactions in the pool.
    pub fn len(&self) -> usize {
        self.signed.len()
    }

    /// Returns the total size of transactions in the pool in bytes.
    pub fn transaction_size(&self) -> u64 {
        self.signed.size()
    }

    #[cfg(test)]
    fn group_count(&self) -> usize {
        self.signed.group_count()
    }
}

/// PoolIterator is a structure to pull transactions from the pool.
/// It implements `TransactionGroupIterator` trait that iterates over transaction groups one by one.
/// When the wrapper is dropped the remaining transactions are returned back to the pool.
pub struct PoolIteratorWrapper<'a> {
    signed: GroupedPoolIterator<'a, ValidatedTransaction>,
}

impl<'a> PoolIteratorWrapper<'a> {
    pub fn new(pool: &'a mut TransactionPool) -> Self {
        Self { signed: GroupedPoolIterator::new(&mut pool.signed) }
    }
}

impl<'a> TransactionGroupIterator for PoolIteratorWrapper<'a> {
    fn next(&mut self) -> Option<&mut TransactionGroup> {
        self.signed.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::hash::CryptoHash;
    use near_primitives::transaction::{SignedTransaction, TransactionNonce};
    use near_primitives::types::{AccountId, Balance, NonceIndex};
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use std::collections::HashMap;
    use std::sync::Arc;
    const TEST_SEED: RngSeed = [3; 32];

    fn generate_transactions(
        signer_id: &str,
        signer_seed: &str,
        starting_nonce: u64,
        end_nonce: u64,
    ) -> Vec<ValidatedTransaction> {
        let signer_id: AccountId = signer_id.parse().unwrap();
        let signer =
            Arc::new(InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, signer_seed));
        (starting_nonce..=end_nonce)
            .map(|i| {
                let signed_tx = SignedTransaction::send_money(
                    i,
                    signer_id.clone(),
                    "bob.near".parse().unwrap(),
                    &*signer,
                    Balance::from_yoctonear(u128::from(i)),
                    CryptoHash::default(),
                );
                ValidatedTransaction::new_for_test(signed_tx)
            })
            .collect()
    }

    fn generate_transactions_v1(
        signer_id: &str,
        signer_seed: &str,
        starting_nonce: u64,
        end_nonce: u64,
        nonce_index: Option<NonceIndex>,
    ) -> Vec<ValidatedTransaction> {
        let signer_id: AccountId = signer_id.parse().unwrap();
        let signer =
            Arc::new(InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, signer_seed));
        (starting_nonce..=end_nonce)
            .map(|i| {
                let signed_tx = SignedTransaction::send_money_v1(
                    match nonce_index {
                        Some(index) => TransactionNonce::from_nonce_and_index(i, index),
                        None => TransactionNonce::from_nonce(i),
                    },
                    signer_id.clone(),
                    "bob.near".parse().unwrap(),
                    &*signer,
                    Balance::from_yoctonear(u128::from(i)),
                    CryptoHash::default(),
                );
                ValidatedTransaction::new_for_test(signed_tx)
            })
            .collect()
    }

    fn process_txs_to_nonces(
        mut validated_txs: Vec<ValidatedTransaction>,
        expected_weight: u32,
    ) -> (Vec<u64>, TransactionPool) {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let mut rng = StdRng::seed_from_u64(0);
        validated_txs.shuffle(&mut rng);
        for validated_tx in validated_txs {
            assert_eq!(pool.insert_transaction(validated_tx), InsertTransactionResult::Success);
        }
        (
            prepare_transactions(&mut pool, expected_weight)
                .iter()
                .map(|tx| tx.transaction.nonce().nonce())
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

    fn prepare_transactions(
        pool: &mut TransactionPool,
        max_number_of_transactions: u32,
    ) -> Vec<SignedTransaction> {
        let mut res = vec![];
        let mut pool_iter = pool.pool_iterator();
        while res.len() < max_number_of_transactions as usize {
            if let Some(iter) = pool_iter.next() {
                if let Some(tx) = iter.next() {
                    res.push(tx.into_signed_tx());
                }
            } else {
                break;
            }
        }
        res
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
        let nonces: Vec<u64> = prepare_transactions(&mut pool, 10)
            .iter()
            .map(|tx| tx.transaction.nonce().nonce())
            .collect();
        assert_eq!(nonces, vec![28, 29, 30, 31]);
    }

    #[test]
    fn test_remove_transactions() {
        let n = 100;
        let mut transactions = (1..=n)
            .map(|i| {
                let signer_id = AccountId::try_from(format!("user_{}", i % 5)).unwrap();
                let signer_seed = format!("user_{}", i % 3);
                let signer = Arc::new(InMemorySigner::from_seed(
                    signer_id.clone(),
                    KeyType::ED25519,
                    &signer_seed,
                ));
                let signed_tx = SignedTransaction::send_money(
                    i,
                    signer_id,
                    "bob.near".parse().unwrap(),
                    &*signer,
                    Balance::from_yoctonear(u128::from(i)),
                    CryptoHash::default(),
                );
                ValidatedTransaction::new_for_test(signed_tx)
            })
            .collect::<Vec<_>>();

        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let mut rng = StdRng::seed_from_u64(0);
        transactions.shuffle(&mut rng);
        for tx in transactions.clone() {
            println!("{:?}", tx);
            assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
        }
        assert_eq!(pool.len(), n as usize);

        transactions.shuffle(&mut rng);
        let (txs_to_remove, txs_to_check) = transactions.split_at(transactions.len() / 2);
        let txs_to_remove =
            txs_to_remove.into_iter().cloned().map(|vt| vt.into_signed_tx()).collect::<Vec<_>>();
        pool.remove_transactions(&txs_to_remove);

        assert_eq!(pool.len(), txs_to_check.len());

        let mut pool_txs = prepare_transactions(&mut pool, txs_to_check.len() as u32);
        pool_txs.sort_by_key(|tx| tx.transaction.nonce().nonce());
        let mut expected_txs = txs_to_check.to_vec();
        expected_txs.sort_by_key(|tx| tx.nonce().nonce());
        let expected_txs =
            expected_txs.into_iter().map(|vt| vt.into_signed_tx()).collect::<Vec<_>>();

        assert_eq!(pool_txs, expected_txs);
    }

    /// Add transactions of nonce from 1..=3 and transactions with nonce 21..=31. Pull 10.
    /// Then try to get another 10.
    #[test]
    fn test_pool_iterator() {
        let mut transactions = generate_transactions("alice.near", "alice.near", 1, 3);
        transactions.extend(generate_transactions("alice.near", "bob.near", 21, 31));

        let (nonces, mut pool) = process_txs_to_nonces(transactions, 0);
        assert!(nonces.is_empty());
        let mut res = vec![];
        let mut pool_iter = pool.pool_iterator();
        while let Some(iter) = pool_iter.next() {
            while let Some(tx) = iter.next() {
                if tx.nonce().nonce() & 1 == 1 {
                    res.push(tx);
                    break;
                }
            }
        }
        drop(pool_iter);
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.transaction_size(), 0);
        let mut nonces: Vec<_> = res.into_iter().map(|tx| tx.nonce().nonce()).collect();
        sort_pairs(&mut nonces[..4]);
        assert_eq!(nonces, vec![1, 21, 3, 23, 25, 27, 29, 31]);
    }

    /// Test pool iterator updates unique transactions.
    #[test]
    fn test_pool_iterator_removes_unique() {
        let transactions = generate_transactions("alice.near", "alice.near", 1, 10);

        let (nonces, mut pool) = process_txs_to_nonces(transactions.clone(), 5);
        assert_eq!(nonces.len(), 5);
        assert_eq!(pool.len(), 5);

        for tx in transactions {
            assert!(matches!(
                pool.insert_transaction(tx),
                InsertTransactionResult::Success | InsertTransactionResult::Duplicate
            ));
        }
        assert_eq!(pool.len(), 10);
        let txs = prepare_transactions(&mut pool, 10);
        assert_eq!(txs.len(), 10);
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.transaction_size(), 0);
    }

    /// Test pool iterator remembers the last key.
    #[test]
    fn test_pool_iterator_remembers_the_last_key() {
        let transactions = (1..=10)
            .map(|i| {
                let signer_id = AccountId::try_from(format!("user_{}", i)).unwrap();
                let signer = Arc::new(InMemorySigner::test_signer(&signer_id));
                let signed_tx = SignedTransaction::send_money(
                    i,
                    signer_id,
                    "bob.near".parse().unwrap(),
                    &*signer,
                    Balance::from_yoctonear(u128::from(i)),
                    CryptoHash::default(),
                );
                ValidatedTransaction::new_for_test(signed_tx)
            })
            .collect::<Vec<_>>();
        let (mut nonces, mut pool) = process_txs_to_nonces(transactions.clone(), 5);
        assert_eq!(nonces.len(), 5);
        assert_eq!(pool.len(), 5);

        for tx in transactions {
            assert!(matches!(
                pool.insert_transaction(tx),
                InsertTransactionResult::Success | InsertTransactionResult::Duplicate
            ));
        }
        assert_eq!(pool.len(), 10);
        let txs = prepare_transactions(&mut pool, 5);
        assert_eq!(txs.len(), 5);
        nonces.sort();
        let mut new_nonces =
            txs.iter().map(|tx| tx.transaction.nonce().nonce()).collect::<Vec<_>>();
        new_nonces.sort();
        assert_ne!(nonces, new_nonces);
    }

    #[test]
    fn test_transaction_pool_size() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let transactions = generate_transactions("alice.near", "alice.near", 1, 100);
        let mut total_transaction_size = 0;
        // Adding transactions increases the size.
        for tx in transactions.clone() {
            total_transaction_size += tx.wire_size();
            assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
            assert_eq!(pool.transaction_size(), total_transaction_size);
        }
        // Removing transactions decreases the size.
        for tx in transactions {
            total_transaction_size -= tx.wire_size();
            pool.remove_transactions(&[tx.into_signed_tx()]);
            assert_eq!(pool.transaction_size(), total_transaction_size);
        }
        assert_eq!(pool.transaction_size(), 0);
    }

    #[test]
    fn test_transaction_pool_size_limit() {
        let transactions = generate_transactions("alice.near", "alice.near", 1, 100);
        // Each transaction is at least 1 byte in size, so the last transaction will not fit.
        let pool_size_limit =
            transactions.iter().map(|tx| tx.wire_size()).sum::<u64>().strict_sub(1);
        let mut pool = TransactionPool::new(TEST_SEED, Some(pool_size_limit), "");
        for (i, tx) in transactions.iter().cloned().enumerate() {
            if i + 1 < transactions.len() {
                assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
            } else {
                assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::NoSpaceLeft);
            }
        }
    }

    #[test]
    fn test_order_nonce_with_nonce_index() {
        // Create transactions from the same signer with two different nonce indices.
        // Each nonce index should form a separate group.
        let mut transactions = generate_transactions_v1("alice.near", "alice.near", 1, 5, Some(0));
        transactions.extend(generate_transactions_v1("alice.near", "alice.near", 1, 5, Some(1)));

        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let mut rng = StdRng::seed_from_u64(0);
        transactions.shuffle(&mut rng);
        for tx in transactions {
            assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
        }

        // There should be 2 transaction groups (one per nonce index).
        assert_eq!(pool.group_count(), 2);
        assert_eq!(pool.len(), 10);

        // Pull transactions using the pool iterator. Since there are 2 groups,
        // we should get transactions interleaved from each group.
        let mut nonces_by_index: HashMap<NonceIndex, Vec<u64>> = HashMap::new();
        let mut pool_iter = pool.pool_iterator();
        while let Some(group) = pool_iter.next() {
            while let Some(tx) = group.next() {
                let nonce_index = tx.nonce().nonce_index().unwrap();
                let nonce = tx.nonce().nonce();
                nonces_by_index.entry(nonce_index).or_default().push(nonce);
            }
        }
        drop(pool_iter);

        // Each nonce index group should have nonces 1..=5 in order.
        assert_eq!(nonces_by_index.get(&0), Some(&vec![1, 2, 3, 4, 5]));
        assert_eq!(nonces_by_index.get(&1), Some(&vec![1, 2, 3, 4, 5]));
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.transaction_size(), 0);
    }

    #[test]
    fn test_remove_transactions_with_nonce_index() {
        let n = 50;
        let mut transactions: Vec<ValidatedTransaction> = Vec::new();

        // Create transactions with different nonce indices from the same signer.
        for nonce_index in 0..3 {
            transactions.extend(generate_transactions_v1(
                "alice.near",
                "alice.near",
                1,
                n,
                Some(nonce_index),
            ));
        }

        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let mut rng = StdRng::seed_from_u64(0);
        transactions.shuffle(&mut rng);
        for tx in transactions.clone() {
            assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
        }
        assert_eq!(pool.len(), (n * 3) as usize);
        // Should have 3 groups (one per nonce index).
        assert_eq!(pool.group_count(), 3);

        // Remove half of the transactions.
        transactions.shuffle(&mut rng);
        let (txs_to_remove, txs_to_check) = transactions.split_at(transactions.len() / 2);
        let txs_to_remove =
            txs_to_remove.iter().cloned().map(|vt| vt.into_signed_tx()).collect::<Vec<_>>();
        pool.remove_transactions(&txs_to_remove);

        assert_eq!(pool.len(), txs_to_check.len());

        // Verify remaining transactions can be retrieved and match expected.
        let mut remaining_txs = vec![];
        let mut pool_iter = pool.pool_iterator();
        while let Some(group) = pool_iter.next() {
            while let Some(tx) = group.next() {
                remaining_txs.push(tx.clone());
            }
        }
        drop(pool_iter);

        remaining_txs.sort_by_key(|tx| (tx.nonce().nonce_index(), tx.nonce().nonce()));
        let mut expected_txs: Vec<_> = txs_to_check.to_vec();
        expected_txs.sort_by_key(|tx| (tx.nonce().nonce_index(), tx.nonce().nonce()));

        assert_eq!(remaining_txs.len(), expected_txs.len());
        for (actual, expected) in remaining_txs.iter().zip(expected_txs.iter()) {
            assert_eq!(actual.get_hash(), expected.get_hash());
        }
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.transaction_size(), 0);
    }

    /// Groups where the caller never pops a transaction (simulating a
    /// strict-nonce gap) must not starve groups that make progress. The
    /// iterator should park stalled groups and terminate.
    #[test]
    fn test_stalled_groups_do_not_starve_valid_groups() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");

        // 3 groups: "alice" (5 txs), "bob" (3 txs), "stalled" (2 txs).
        let (tx_count_alice, tx_count_bob, tx_count_stalled) = (5, 3, 2);
        for tx in generate_transactions("alice.near", "alice.near", 1, tx_count_alice) {
            pool.insert_transaction(tx);
        }
        for tx in generate_transactions("bob.near", "bob.near", 1, tx_count_bob) {
            pool.insert_transaction(tx);
        }
        for tx in generate_transactions("stalled.near", "stalled.near", 1, tx_count_stalled) {
            pool.insert_transaction(tx);
        }
        assert_eq!(pool.len() as u64, tx_count_alice + tx_count_bob + tx_count_stalled);

        // Pop from "alice" and "bob" groups (making progress) but not from "stalled".
        let stalled_id: AccountId = "stalled.near".parse().unwrap();
        let mut consumed = vec![];
        let mut iter = pool.pool_iterator();
        while let Some(group) = iter.next() {
            let Some(peek) = group.peek_next() else { continue };
            if *peek.signer_id() == stalled_id {
                // Simulate stall: don't pop any transaction.
                continue;
            }
            if let Some(tx) = group.next() {
                consumed.push(tx.into_signed_tx());
            }
        }
        drop(iter);

        assert_eq!(consumed.len() as u64, tx_count_alice + tx_count_bob);
        assert_eq!(pool.len() as u64, tx_count_stalled);
    }

    /// A signed transaction of `alice.near` with the given nonce. Different deposits make
    /// transactions with the same nonce different transactions.
    fn alice_tx(nonce: u64, deposit: u128) -> ValidatedTransaction {
        let signer_id: AccountId = "alice.near".parse().unwrap();
        let signer = InMemorySigner::test_signer(&signer_id);
        let signed_tx = SignedTransaction::send_money(
            nonce,
            signer_id,
            "bob.near".parse().unwrap(),
            &signer,
            Balance::from_yoctonear(deposit),
            CryptoHash::default(),
        );
        ValidatedTransaction::new_for_test(signed_tx)
    }

    /// Takes everything out of the pool. Returns the hashes in the order they were taken.
    fn drain_hashes(pool: &mut TransactionPool) -> Vec<CryptoHash> {
        let mut hashes = vec![];
        let mut iter = pool.pool_iterator();
        while let Some(group) = iter.next() {
            while let Some(tx) = group.next() {
                hashes.push(tx.get_hash());
            }
        }
        hashes
    }

    /// Of the transactions with the same nonce in a group, the one added last is taken first.
    #[test]
    fn test_equal_nonces_the_last_added_goes_first() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let txs = [alice_tx(1, 1), alice_tx(1, 2), alice_tx(1, 3), alice_tx(2, 1)];
        for tx in txs.clone() {
            assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
        }

        let expected = [&txs[2], &txs[1], &txs[0], &txs[3]].map(|tx| tx.get_hash());
        assert_eq!(drain_hashes(&mut pool), expected);
    }

    /// The same holds when the group was taken out of the pool and returned in between, and new
    /// transactions with the same nonce were added meanwhile.
    #[test]
    fn test_equal_nonces_the_last_added_goes_first_after_the_group_was_returned() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let (a, b, c, d) = (alice_tx(1, 1), alice_tx(1, 2), alice_tx(1, 3), alice_tx(1, 4));
        let later = alice_tx(2, 1);
        for tx in [a.clone(), b.clone(), later.clone()] {
            assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
        }
        // Take one transaction and return the rest.
        let first = {
            let mut iter = pool.pool_iterator();
            iter.next().unwrap().next().unwrap()
        };
        assert_eq!(first.get_hash(), b.get_hash());
        for tx in [c.clone(), d.clone()] {
            assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
        }

        let expected = [&d, &c, &a, &later].map(|tx| tx.get_hash());
        assert_eq!(drain_hashes(&mut pool), expected);
    }

    /// A duplicate is reported as a duplicate, not as a lack of space, even if the pool is full.
    #[test]
    fn test_duplicate_is_reported_before_no_space() {
        let tx = alice_tx(1, 1);
        let mut pool = TransactionPool::new(TEST_SEED, Some(tx.wire_size()), "");
        assert_eq!(pool.insert_transaction(tx.clone()), InsertTransactionResult::Success);

        assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Duplicate);
        assert_eq!(pool.insert_transaction(alice_tx(2, 1)), InsertTransactionResult::NoSpaceLeft);
        assert_eq!(pool.len(), 1);
    }

    /// The gauges follow the pool through inserts, removals and iteration.
    #[test]
    fn test_metrics_follow_the_pool() {
        let label = "test_metrics_follow_the_pool";
        let count = metrics::TRANSACTION_POOL_COUNT.with_label_values(&[label]);
        let size = metrics::TRANSACTION_POOL_SIZE.with_label_values(&[label]);
        let mut pool = TransactionPool::new(TEST_SEED, None, label);
        assert_eq!((count.get(), size.get()), (0, 0));

        let txs = (1..=4).map(|nonce| alice_tx(nonce, 1)).collect::<Vec<_>>();
        for tx in txs.clone() {
            pool.insert_transaction(tx);
        }
        let tx_size = txs[0].wire_size() as i64;
        assert_eq!((count.get(), size.get()), (4, 4 * tx_size));

        // Take one transaction out for good, the rest returns to the pool.
        {
            let mut iter = pool.pool_iterator();
            iter.next().unwrap().next().unwrap();
        }
        assert_eq!((count.get(), size.get()), (3, 3 * tx_size));

        pool.remove_transactions(&[txs[3].clone().into_signed_tx()]);
        assert_eq!((count.get(), size.get()), (2, 2 * tx_size));

        // Taking everything and exhausting the groups updates them too.
        drain_hashes(&mut pool);
        assert_eq!((count.get(), size.get()), (0, 0));
    }

    /// Removing transactions that are not in the pool changes nothing.
    #[test]
    fn test_removing_unknown_transactions_changes_nothing() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let txs = [alice_tx(1, 1), alice_tx(2, 1)];
        for tx in txs.clone() {
            pool.insert_transaction(tx);
        }
        let size = pool.transaction_size();

        pool.remove_transactions(&[
            alice_tx(3, 1).into_signed_tx(),
            alice_tx(1, 2).into_signed_tx(),
        ]);

        assert_eq!(pool.len(), 2);
        assert_eq!(pool.transaction_size(), size);
        assert_eq!(drain_hashes(&mut pool), [&txs[0], &txs[1]].map(|tx| tx.get_hash()));
    }

    /// A group is gone from the pool once all of its transactions were removed.
    #[test]
    fn test_group_is_gone_after_all_its_transactions_were_removed() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let alice = generate_transactions("alice.near", "alice.near", 1, 2);
        let bob = generate_transactions("bob.near", "bob.near", 1, 2);
        for tx in alice.iter().chain(&bob).cloned() {
            pool.insert_transaction(tx);
        }

        let alice_txs = alice.into_iter().map(|tx| tx.into_signed_tx()).collect::<Vec<_>>();
        pool.remove_transactions(&alice_txs);

        let mut groups = 0;
        let mut iter = pool.pool_iterator();
        while let Some(group) = iter.next() {
            groups += 1;
            assert_eq!(group.peek_next().unwrap().signer_id().as_str(), "bob.near");
            while group.next().is_some() {}
        }
        assert_eq!(groups, 1);
    }

    /// What is taken out and not put back leaves the pool, and what is left stays, in order.
    #[test]
    fn test_partially_taken_group_goes_back_to_the_pool() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let txs = (1..=3).map(|nonce| alice_tx(nonce, 1)).collect::<Vec<_>>();
        for tx in txs.clone() {
            pool.insert_transaction(tx);
        }

        let taken = {
            let mut iter = pool.pool_iterator();
            iter.next().unwrap().next().unwrap()
        };

        assert_eq!(taken.get_hash(), txs[0].get_hash());
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.transaction_size(), txs[1].wire_size() + txs[2].wire_size());
        assert_eq!(drain_hashes(&mut pool), [&txs[1], &txs[2]].map(|tx| tx.get_hash()));
    }

    /// The gauges are updated as soon as the iterator releases a group that was taken empty, not
    /// only when it is dropped.
    #[test]
    fn test_metrics_are_updated_while_iterating() {
        let label = "test_metrics_are_updated_while_iterating";
        let count = metrics::TRANSACTION_POOL_COUNT.with_label_values(&[label]);
        let size = metrics::TRANSACTION_POOL_SIZE.with_label_values(&[label]);
        let mut pool = TransactionPool::new(TEST_SEED, None, label);
        let txs = (1..=2).map(|nonce| alice_tx(nonce, 1)).collect::<Vec<_>>();
        for tx in txs.clone() {
            pool.insert_transaction(tx);
        }
        assert_eq!((count.get(), size.get()), (2, 2 * txs[0].wire_size() as i64));

        let mut iter = pool.pool_iterator();
        let group = iter.next().unwrap();
        while group.next().is_some() {}
        // The pool has no more groups, so this takes the emptied group out of the queue.
        assert!(iter.next().is_none());

        assert_eq!((count.get(), size.get()), (0, 0));
    }

    /// The iterator goes through the groups in the order of their keys, and after the last key it
    /// wraps around to the first one, in every pass.
    #[test]
    fn test_iteration_wraps_around_to_the_first_group() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        for i in 0..6 {
            let signer_id = format!("user{i}.near");
            for tx in generate_transactions(&signer_id, &signer_id, 1, 1) {
                pool.insert_transaction(tx);
            }
        }
        // Looks at every group without taking anything from it.
        let visit_all = |pool: &mut TransactionPool| {
            let mut order = vec![];
            let mut iter = pool.pool_iterator();
            while let Some(group) = iter.next() {
                order.push(group.peek_next().unwrap().get_hash());
            }
            order
        };

        let first_pass = visit_all(&mut pool);
        assert_eq!(first_pass.len(), 6);
        assert_eq!(visit_all(&mut pool), first_pass);
        assert_eq!(visit_all(&mut pool), first_pass);
    }

    /// A group that was yielded and not touched is not lost when the iterator is dropped.
    #[test]
    fn test_untouched_group_goes_back_to_the_pool() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let txs = [alice_tx(1, 1), alice_tx(2, 1)];
        for tx in txs.clone() {
            pool.insert_transaction(tx);
        }
        {
            let mut iter = pool.pool_iterator();
            assert!(iter.next().is_some());
            assert!(iter.next().is_none());
        }

        assert_eq!(drain_hashes(&mut pool), txs.map(|tx| tx.get_hash()));
    }

    /// The order of the groups depends on the seed of the pool.
    #[test]
    fn test_group_order_depends_on_the_seed() {
        let group_order = |seed: RngSeed| {
            let mut pool = TransactionPool::new(seed, None, "");
            for i in 0..8 {
                let signer_id = format!("user{i}.near");
                for tx in generate_transactions(&signer_id, &signer_id, 1, 1) {
                    pool.insert_transaction(tx);
                }
            }
            let mut order = vec![];
            let mut iter = pool.pool_iterator();
            while let Some(group) = iter.next() {
                order.push(group.peek_next().unwrap().signer_id().clone());
                while group.next().is_some() {}
            }
            order
        };

        assert_eq!(group_order([3; 32]).len(), 8);
        assert_eq!(group_order([3; 32]), group_order([3; 32]));
        assert_ne!(group_order([3; 32]), group_order([4; 32]));
    }
}
