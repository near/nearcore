use near_primitives::hash::CryptoHash;
use near_primitives::transaction::ValidatedTransaction;

/// Trait acts like an iterator. It iterates over transactions groups by returning mutable
/// references to them. Each transaction group implements a draining iterator to pull transactions.
/// The order of the transaction groups is round robin scheduling.
pub trait TransactionGroupIterator {
    fn next(&mut self) -> Option<&mut TransactionGroup>;
}

/// A hash of (an AccountId, a PublicKey and a seed).
/// Used to randomize the order of the keys.
pub(crate) type PoolKey = CryptoHash;

/// Represents a group of transactions with the same key.
pub struct TransactionGroup {
    /// The key of the group.
    pub(crate) key: PoolKey,
    /// Ordered transactions by nonce in non-increasing order (e.g. 3, 2, 2).
    pub(crate) transactions: Vec<ValidatedTransaction>,
    /// Hashes of the transactions that were pulled from the group using `.next()`.
    pub(crate) removed_transaction_hashes: Vec<CryptoHash>,
    /// Total size of transactions that were pulled from the group using `.next()`.
    pub(crate) removed_transaction_size: u64,
}

impl TransactionGroup {
    /// Returns the next transaction with the smallest nonce and removes it from the group.
    /// It also stores all hashes of returned transactions.
    pub fn next(&mut self) -> Option<ValidatedTransaction> {
        if let Some(validated_tx) = self.transactions.pop() {
            self.removed_transaction_hashes.push(validated_tx.get_hash());
            self.removed_transaction_size += validated_tx.get_size();
            Some(validated_tx)
        } else {
            None
        }
    }

    pub fn peek_next(&self) -> Option<&ValidatedTransaction> {
        self.transactions.last()
    }
}
