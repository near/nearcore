use crate::grouped_pool::{GroupedPool, GroupedPoolIterator, PoolMetrics};
use crate::types::{PooledGroup, TransactionGroupIterator};
use near_primitives::epoch_info::RngSeed;
use near_primitives::transaction::{
    TransactionEnvelope, ValidatedEcc, ValidatedTransaction, ValidatedTransactionEnvelope,
};

mod ecc;
mod grouped_pool;
mod item;
mod metrics;
mod signed;
pub mod types;

/// ECCs may fill at most `1 / ECC_SIZE_SHARE_DIVISOR` of the pool size limit. It is a placeholder
/// until the real share is picked.
// TODO(ecc): consider making this configurable
const ECC_SIZE_SHARE_DIVISOR: u64 = 2;

#[derive(Debug, PartialEq)]
pub enum InsertTransactionResult {
    /// Transaction was successfully inserted.
    Success,
    /// Transaction is already in the pool.
    Duplicate,
    /// Not enough space to fit the transaction, or the ECCs' share of the pool is full.
    NoSpaceLeft,
}

/// Size limits of the pool in bytes. `None` means no limit.
struct PoolLimits {
    /// New transactions that bring the size of the pool over this limit are rejected.
    total_transaction_size_limit: Option<u64>,
    /// New ECCs that bring the size of all ECCs in the pool over this limit
    /// are rejected. It is the ECCs' share of the total.
    ecc_share_limit: Option<u64>,
}

impl PoolLimits {
    fn new(total_transaction_size_limit: Option<u64>) -> Self {
        let ecc_share_limit =
            total_transaction_size_limit.map(|limit| limit / ECC_SIZE_SHARE_DIVISOR);
        Self { total_transaction_size_limit, ecc_share_limit }
    }
}

/// The smaller of two limits, where `None` is no limit.
fn smaller_limit(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Transaction pool: keeps track of transactions that were not yet accepted into the block chain.
///
/// It looks like one pool from the outside, but it is made of two: one for signed transactions and
/// one for ECCs. The two pools share the size limit, and the iterator takes groups from
/// each of them in turn.
pub struct TransactionPool {
    /// Signed transactions are grouped by a triple of (account ID, signer public key, nonce
    /// index).
    signed: GroupedPool<ValidatedTransaction>,
    /// ECCs are grouped by the contract they call.
    eccs: GroupedPool<ValidatedEcc>,
    limits: PoolLimits,
}

impl TransactionPool {
    pub fn new(
        key_seed: RngSeed,
        total_transaction_size_limit: Option<u64>,
        metrics_label: &str,
    ) -> Self {
        let signed_metrics = PoolMetrics::new(
            &metrics::TRANSACTION_POOL_COUNT,
            &metrics::TRANSACTION_POOL_SIZE,
            metrics_label,
        );
        let ecc_metrics =
            PoolMetrics::new(&metrics::ECC_POOL_COUNT, &metrics::ECC_POOL_SIZE, metrics_label);
        Self {
            signed: GroupedPool::new(key_seed, signed_metrics),
            eccs: GroupedPool::new(key_seed, ecc_metrics),
            limits: PoolLimits::new(total_transaction_size_limit),
        }
    }

    /// The most the signed transactions may take: what the ECCs leave of the total.
    fn signed_size_limit(&self) -> Option<u64> {
        let ecc_size = self.eccs.size();
        self.limits.total_transaction_size_limit.map(|limit| limit.saturating_sub(ecc_size))
    }

    /// The most the ECCs may take: their share, and what the signed transactions leave
    /// of the total.
    fn ecc_size_limit(&self) -> Option<u64> {
        let signed_size = self.signed.size();
        let room_in_total =
            self.limits.total_transaction_size_limit.map(|limit| limit.saturating_sub(signed_size));
        smaller_limit(room_in_total, self.limits.ecc_share_limit)
    }

    /// Inserts a transaction that passed validation into the pool. An ECC waits behind
    /// the ECCs already waiting for its contract.
    pub fn insert_transaction(
        &mut self,
        validated_tx: impl Into<ValidatedTransactionEnvelope>,
    ) -> InsertTransactionResult {
        match validated_tx.into() {
            ValidatedTransactionEnvelope::Signed(tx) => {
                let size_limit = self.signed_size_limit();
                self.signed.insert(tx, size_limit)
            }
            ValidatedTransactionEnvelope::Unsigned(ecc) => {
                let size_limit = self.ecc_size_limit();
                self.eccs.insert(ecc, size_limit)
            }
        }
    }

    /// Puts back transactions that were taken out of the pool, e.g. because their chunk missed a
    /// block. They are expected in the order of arrival. ECCs go ahead of everything
    /// waiting for their contract, including ECCs put back earlier, and keep their order among
    /// themselves. So when several batches are put back, the newest one has to go first.
    ///
    /// Each transaction is admitted in this order if it fits, and signed transactions get the
    /// space before the ECCs. Returns the number of transactions that were added or are already
    /// present in the pool.
    pub fn reintroduce_transactions(
        &mut self,
        validated_txs: impl IntoIterator<Item = impl Into<ValidatedTransactionEnvelope>>,
    ) -> usize {
        let mut signed_txs = vec![];
        let mut eccs = vec![];
        for validated_tx in validated_txs {
            match validated_tx.into() {
                ValidatedTransactionEnvelope::Signed(tx) => signed_txs.push(tx),
                ValidatedTransactionEnvelope::Unsigned(ecc) => eccs.push(ecc),
            }
        }

        let signed_size_limit = self.signed_size_limit();
        let reintroduced_signed_txs = signed_txs
            .into_iter()
            .map(|tx| self.signed.insert(tx, signed_size_limit))
            .filter(|result| *result != InsertTransactionResult::NoSpaceLeft)
            .count();
        let ecc_size_limit = self.ecc_size_limit();
        let reintroduced_eccs = self.eccs.reintroduce_items(eccs, ecc_size_limit);
        reintroduced_signed_txs + reintroduced_eccs
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
        txs: impl IntoIterator<Item = &'a TransactionEnvelope>,
    ) {
        let txs = txs.into_iter().collect::<Vec<_>>();
        self.signed.remove_items(
            txs.iter()
                .filter_map(|tx| tx.as_signed())
                .map(|tx| (tx.get_hash(), signed::group_id(tx))),
        );
        self.eccs.remove_items(
            txs.iter()
                .filter_map(|tx| tx.as_unsigned())
                .map(|tx| (tx.get_hash(), ecc::group_id(tx))),
        );
    }

    /// Returns the number of unique transactions in the pool.
    pub fn len(&self) -> usize {
        self.signed.len() + self.eccs.len()
    }

    /// Returns the total size of transactions in the pool in bytes.
    pub fn transaction_size(&self) -> u64 {
        self.signed.size() + self.eccs.size()
    }

    #[cfg(test)]
    fn ecc_size(&self) -> u64 {
        self.eccs.size()
    }

    #[cfg(test)]
    fn group_count(&self) -> usize {
        self.signed.group_count() + self.eccs.group_count()
    }
}

/// PoolIterator is a structure to pull transactions from the pool.
/// It implements `TransactionGroupIterator` trait that iterates over transaction groups one by one.
/// When the wrapper is dropped the remaining transactions are returned back to the pool.
///
/// Groups of signed transactions and groups of ECCs alternate. If one kind runs out,
/// the other one goes on.
pub struct PoolIteratorWrapper<'a> {
    signed: GroupedPoolIterator<'a, ValidatedTransaction>,
    eccs: GroupedPoolIterator<'a, ValidatedEcc>,
    /// Whether the next group is taken from the signed transactions first.
    signed_turn: bool,
}

impl<'a> PoolIteratorWrapper<'a> {
    pub fn new(pool: &'a mut TransactionPool) -> Self {
        Self {
            signed: GroupedPoolIterator::new(&mut pool.signed),
            eccs: GroupedPoolIterator::new(&mut pool.eccs),
            signed_turn: true,
        }
    }

    /// Takes all the remaining transactions out of the pool, group by group.
    pub fn drain(mut self) -> Vec<ValidatedTransactionEnvelope> {
        let mut txs = vec![];
        while let Some(group) = self.next() {
            match group {
                PooledGroup::Signed(group) => {
                    while let Some(tx) = group.next() {
                        txs.push(tx.into());
                    }
                }
                PooledGroup::Ecc(group) => {
                    while let Some(ecc) = group.next() {
                        txs.push(ecc.into());
                    }
                }
            }
        }
        txs
    }
}

impl<'a> TransactionGroupIterator for PoolIteratorWrapper<'a> {
    fn next(&mut self) -> Option<PooledGroup<'_>> {
        let signed_turn = self.signed_turn;
        self.signed_turn = !signed_turn;
        if signed_turn {
            let signed = self.signed.next().map(PooledGroup::Signed);
            signed.or_else(|| self.eccs.next().map(PooledGroup::Ecc))
        } else {
            let ecc = self.eccs.next().map(PooledGroup::Ecc);
            ecc.or_else(|| self.signed.next().map(PooledGroup::Signed))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TransactionGroup;
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::hash::CryptoHash;
    use near_primitives::transaction::{EccTransaction, SignedTransaction, TransactionNonce};
    use near_primitives::types::{AccountId, Balance, NonceIndex};
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use std::collections::HashMap;
    use std::sync::Arc;
    const TEST_SEED: RngSeed = [3; 32];

    fn to_envelopes(signed_txs: &[SignedTransaction]) -> Vec<TransactionEnvelope> {
        signed_txs.iter().cloned().map(TransactionEnvelope::from).collect()
    }

    /// The next group of the pool iterator, in a pool that has no ECCs.
    fn next_signed<'a>(iter: &'a mut PoolIteratorWrapper<'_>) -> Option<&'a mut TransactionGroup> {
        iter.next().map(|group| {
            let PooledGroup::Signed(group) = group else { panic!("there are no ECCs in the pool") };
            group
        })
    }

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
            if let Some(iter) = next_signed(&mut pool_iter) {
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
        pool.remove_transactions(&to_envelopes(&txs_to_remove));

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
        while let Some(iter) = next_signed(&mut pool_iter) {
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
            pool.remove_transactions(&to_envelopes(&[tx.into_signed_tx()]));
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
        while let Some(group) = next_signed(&mut pool_iter) {
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
        pool.remove_transactions(&to_envelopes(&txs_to_remove));

        assert_eq!(pool.len(), txs_to_check.len());

        // Verify remaining transactions can be retrieved and match expected.
        let mut remaining_txs = vec![];
        let mut pool_iter = pool.pool_iterator();
        while let Some(group) = next_signed(&mut pool_iter) {
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
        while let Some(group) = next_signed(&mut iter) {
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
        pool.pool_iterator().drain().iter().map(|tx| tx.get_hash()).collect()
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
            next_signed(&mut iter).unwrap().next().unwrap()
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
            next_signed(&mut iter).unwrap().next().unwrap();
        }
        assert_eq!((count.get(), size.get()), (3, 3 * tx_size));

        pool.remove_transactions(&to_envelopes(&[txs[3].clone().into_signed_tx()]));
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

        pool.remove_transactions(&to_envelopes(&[
            alice_tx(3, 1).into_signed_tx(),
            alice_tx(1, 2).into_signed_tx(),
        ]));

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
        pool.remove_transactions(&to_envelopes(&alice_txs));

        let mut groups = 0;
        let mut iter = pool.pool_iterator();
        while let Some(group) = next_signed(&mut iter) {
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
            next_signed(&mut iter).unwrap().next().unwrap()
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
        let group = next_signed(&mut iter).unwrap();
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
            while let Some(group) = next_signed(&mut iter) {
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

    /// The order of the groups of ECCs depends on the seed of the pool too.
    #[test]
    fn test_ecc_group_order_depends_on_the_seed() {
        let group_order = |seed: RngSeed| {
            let mut pool = TransactionPool::new(seed, None, "");
            for i in 0..8 {
                insert_all(&mut pool, [ecc(&format!("contract{i}.near"), 1, 8)]);
            }
            let mut order = vec![];
            let mut iter = pool.pool_iterator();
            while let Some(group) = iter.next() {
                let PooledGroup::Ecc(group) = group else {
                    panic!("there are no signed transactions")
                };
                order.push(group.peek_next().unwrap().contract_id().clone());
                while group.next().is_some() {}
            }
            order
        };

        assert_eq!(group_order([3; 32]).len(), 8);
        assert_eq!(group_order([3; 32]), group_order([3; 32]));
        assert_ne!(group_order([3; 32]), group_order([4; 32]));
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
            while let Some(group) = next_signed(&mut iter) {
                order.push(group.peek_next().unwrap().signer_id().clone());
                while group.next().is_some() {}
            }
            order
        };

        assert_eq!(group_order([3; 32]).len(), 8);
        assert_eq!(group_order([3; 32]), group_order([3; 32]));
        assert_ne!(group_order([3; 32]), group_order([4; 32]));
    }

    /// Signed transactions of `signer_id` with the nonces from `from` to `to`.
    fn signed_txs(signer_id: &str, from: u64, to: u64) -> Vec<ValidatedTransaction> {
        generate_transactions(signer_id, signer_id, from, to)
    }

    /// An ECC for `contract_id` with a payload of `payload_len` bytes, all equal to `tag`.
    fn ecc(contract_id: &str, tag: u8, payload_len: usize) -> ValidatedEcc {
        let contract_id = contract_id.parse().unwrap();
        ValidatedEcc::new_for_test(EccTransaction::new(contract_id, vec![tag; payload_len]))
    }

    fn to_envelope(ecc: &ValidatedEcc) -> TransactionEnvelope {
        ecc.clone().into_ecc().into()
    }

    /// What was in a group that was taken out of the pool, in the order it was taken.
    #[derive(Clone, Debug, PartialEq)]
    enum Drained {
        /// The nonces of signed transactions.
        Signed(Vec<u64>),
        /// The tags of ECCs.
        Eccs(Vec<u8>),
    }

    /// Takes everything out of the pool, group by group.
    fn drain(pool: &mut TransactionPool) -> Vec<Drained> {
        let mut groups = vec![];
        let mut iter = pool.pool_iterator();
        while let Some(group) = iter.next() {
            groups.push(match group {
                PooledGroup::Signed(group) => {
                    let mut nonces = vec![];
                    while let Some(tx) = group.next() {
                        nonces.push(tx.nonce().nonce());
                    }
                    Drained::Signed(nonces)
                }
                PooledGroup::Ecc(group) => {
                    let mut tags = vec![];
                    while let Some(ecc) = group.next() {
                        tags.push(ecc.payload()[0]);
                    }
                    Drained::Eccs(tags)
                }
            });
        }
        groups
    }

    fn is_signed(group: &Drained) -> bool {
        matches!(group, Drained::Signed(_))
    }

    fn insert_all(
        pool: &mut TransactionPool,
        txs: impl IntoIterator<Item = impl Into<ValidatedTransactionEnvelope>>,
    ) {
        txs.into_iter().for_each(|tx| {
            assert_eq!(pool.insert_transaction(tx), InsertTransactionResult::Success);
        });
    }

    /// Groups of the two kinds alternate, and when one kind runs out the other one goes on.
    #[test]
    fn test_groups_of_the_two_kinds_alternate() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        for signer_id in ["alice.near", "bob.near"] {
            insert_all(&mut pool, signed_txs(signer_id, 1, 2));
        }
        for contract_id in ["a.near", "b.near", "c.near", "d.near"] {
            insert_all(&mut pool, (1..=2).map(|tag| ecc(contract_id, tag, 8)));
        }

        let groups = drain(&mut pool);

        let kinds = groups.iter().map(is_signed).collect::<Vec<_>>();
        assert_eq!(kinds, vec![true, false, true, false, false, false]);
        assert!(groups.iter().all(|group| match group {
            Drained::Signed(nonces) => *nonces == [1, 2],
            Drained::Eccs(tags) => *tags == [1, 2],
        }));
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.transaction_size(), 0);
    }

    /// `drain` takes the transactions of both kinds out of the pool, and nothing is left.
    #[test]
    fn test_drain_takes_everything_out_of_the_pool() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let transactions = signed_txs("alice.near", 1, 3);
        let eccs = [ecc("a.near", 1, 8), ecc("a.near", 2, 8), ecc("b.near", 1, 8)];
        insert_all(&mut pool, transactions.clone());
        insert_all(&mut pool, eccs.clone());

        let mut drained =
            pool.pool_iterator().drain().iter().map(|tx| tx.get_hash()).collect::<Vec<_>>();

        drained.sort();
        let mut expected = transactions.iter().map(|tx| tx.get_hash()).collect::<Vec<_>>();
        expected.extend(eccs.iter().map(|ecc| ecc.get_hash()));
        expected.sort();
        assert_eq!(drained, expected);
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.transaction_size(), 0);
    }

    #[test]
    fn test_iteration_with_one_kind_only() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        insert_all(&mut pool, signed_txs("alice.near", 1, 3));
        assert_eq!(drain(&mut pool), vec![Drained::Signed(vec![1, 2, 3])]);

        insert_all(&mut pool, (1..=3).map(|tag| ecc("a.near", tag, 8)));
        assert_eq!(drain(&mut pool), vec![Drained::Eccs(vec![1, 2, 3])]);
    }

    /// An ECC past the share of the pool is refused and the pool stays as it was, while a signed
    /// transaction is still accepted. Removing an ECC makes room for the next one.
    #[test]
    fn test_ecc_size_limit() {
        let ecc_size = ecc("a.near", 0, 1000).get_size();
        // The share of ECCs is half of the limit, so 3 ECCs fit.
        let mut pool = TransactionPool::new(TEST_SEED, Some(6 * ecc_size), "");
        let eccs = (1..=3).map(|tag| ecc("a.near", tag, 1000)).collect::<Vec<_>>();
        insert_all(&mut pool, eccs.clone());
        let size = pool.transaction_size();
        assert_eq!(pool.ecc_size(), 3 * ecc_size);

        let refused = ecc("b.near", 4, 1000);
        assert_eq!(pool.insert_transaction(refused.clone()), InsertTransactionResult::NoSpaceLeft);
        assert_eq!(pool.reintroduce_transactions([refused.clone()]), 0);

        assert_eq!(pool.len(), 3);
        assert_eq!(pool.transaction_size(), size);
        assert_eq!(pool.ecc_size(), 3 * ecc_size);

        insert_all(&mut pool, signed_txs("alice.near", 1, 1));

        pool.remove_transactions(&[to_envelope(&eccs[0])]);
        assert_eq!(pool.insert_transaction(refused), InsertTransactionResult::Success);
    }

    /// The share is a cap, not a reservation: a pool full of signed transactions refuses ECCs
    /// even if they would fit in the share.
    #[test]
    fn test_ecc_refused_when_the_pool_is_full_of_signed_transactions() {
        let transactions = signed_txs("alice.near", 1, 10);
        let total_size = transactions.iter().map(|tx| tx.wire_size()).sum::<u64>();
        let mut pool = TransactionPool::new(TEST_SEED, Some(total_size), "");
        insert_all(&mut pool, transactions);

        let small_ecc = ecc("a.near", 1, 8);
        assert!(small_ecc.get_size() < total_size / 2);
        assert_eq!(pool.insert_transaction(small_ecc), InsertTransactionResult::NoSpaceLeft);
        assert_eq!(pool.ecc_size(), 0);
    }

    /// The ECCs count against the limit of the signed transactions.
    #[test]
    fn test_signed_transactions_leave_room_for_eccs() {
        let transactions = signed_txs("alice.near", 1, 10);
        let tx_size = transactions[0].wire_size();
        let ecc_size = ecc("a.near", 0, 8).get_size();
        let mut pool = TransactionPool::new(TEST_SEED, Some(10 * tx_size), "");
        insert_all(&mut pool, [ecc("a.near", 1, 8)]);

        // Everything but the room taken by the ECC.
        let (fitting, rest) = transactions.split_at(9);
        insert_all(&mut pool, fitting.to_vec());
        assert_eq!(pool.transaction_size(), 9 * tx_size + ecc_size);
        let result = pool.insert_transaction(rest[0].clone());
        assert_eq!(result, InsertTransactionResult::NoSpaceLeft);
    }

    /// A duplicate is reported as such even if the pool is full.
    #[test]
    fn test_duplicate_is_reported_before_no_space_for_both_kinds() {
        let transactions = signed_txs("alice.near", 1, 1);
        let first_ecc = ecc("a.near", 1, 8);
        let total_size = transactions[0].wire_size() + first_ecc.get_size();
        let mut pool = TransactionPool::new(TEST_SEED, Some(total_size), "");
        insert_all(&mut pool, transactions.clone());
        insert_all(&mut pool, [first_ecc.clone()]);

        assert_eq!(
            pool.insert_transaction(transactions[0].clone()),
            InsertTransactionResult::Duplicate
        );
        assert_eq!(pool.insert_transaction(first_ecc.clone()), InsertTransactionResult::Duplicate);
        assert_eq!(pool.reintroduce_transactions([first_ecc]), 1);
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn test_remove_transactions_of_both_kinds() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        let transactions = signed_txs("alice.near", 1, 3);
        let (a1, a2, b1) = (ecc("a.near", 1, 8), ecc("a.near", 2, 8), ecc("b.near", 1, 8));
        insert_all(&mut pool, transactions.clone());
        insert_all(&mut pool, [a1.clone(), a2.clone(), b1.clone()]);
        assert_eq!(pool.len(), 6);

        // Some of what was included in a block, and an ECC that was not in the pool at all.
        let included = [
            to_envelope(&a1),
            to_envelope(&b1),
            transactions[0].clone().into_signed_tx().into(),
            to_envelope(&ecc("c.near", 1, 8)),
        ];
        pool.remove_transactions(&included);

        assert_eq!(pool.len(), 3);
        assert_eq!(pool.ecc_size(), a2.get_size());
        let remaining_size =
            a2.get_size() + transactions[1].wire_size() + transactions[2].wire_size();
        assert_eq!(pool.transaction_size(), remaining_size);
        let mut groups = drain(&mut pool);
        groups.sort_by_key(|group| !is_signed(group));
        assert_eq!(groups, vec![Drained::Signed(vec![2, 3]), Drained::Eccs(vec![2])]);
    }

    /// In a batch of both kinds, the ECCs keep their order among themselves and go ahead of the
    /// ones that were already waiting, and no signed transaction is lost.
    #[test]
    fn test_reintroduce_mixed_batch() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        insert_all(&mut pool, [ecc("a.near", 4, 8)]);
        let transactions = signed_txs("alice.near", 1, 2);

        let batch: Vec<ValidatedTransactionEnvelope> = vec![
            ecc("a.near", 1, 8).into(),
            transactions[0].clone().into(),
            ecc("a.near", 2, 8).into(),
            transactions[1].clone().into(),
            ecc("a.near", 3, 8).into(),
        ];
        assert_eq!(pool.reintroduce_transactions(batch), 5);

        let mut groups = drain(&mut pool);
        groups.sort_by_key(|group| !is_signed(group));
        assert_eq!(groups, vec![Drained::Signed(vec![1, 2]), Drained::Eccs(vec![1, 2, 3, 4])]);
    }

    /// Reintroducing the batches of several blocks, the newest block first, like a reorg does,
    /// restores the order of arrival of the ECCs.
    #[test]
    fn test_reintroduce_batches_newest_first_restores_arrival_order() {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        insert_all(&mut pool, [ecc("a.near", 5, 8)]);

        pool.reintroduce_transactions([ecc("a.near", 3, 8), ecc("a.near", 4, 8)]);
        pool.reintroduce_transactions([ecc("a.near", 1, 8), ecc("a.near", 2, 8)]);

        assert_eq!(drain(&mut pool), vec![Drained::Eccs(vec![1, 2, 3, 4, 5])]);
    }

    /// A reintroduced batch of signed transactions that does not fit loses its tail, not its head.
    #[test]
    fn test_reintroduce_signed_transactions_refuses_the_tail_when_the_pool_is_full() {
        let transactions = signed_txs("alice.near", 1, 3);
        let limit = transactions[0].wire_size() + transactions[1].wire_size();
        let mut pool = TransactionPool::new(TEST_SEED, Some(limit), "");

        assert_eq!(pool.reintroduce_transactions(transactions), 2);

        assert_eq!(drain(&mut pool), vec![Drained::Signed(vec![1, 2])]);
    }
}
