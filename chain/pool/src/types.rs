use near_crypto::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;

/// Trait acts as an iterator, but removes the returned item form the pool.
/// This iterator returns transaction groups one by one from the pool in the round robin scheduling.
/// When a draining iterator is dropped the remaining transactions are returned back to the pool.
pub trait DrainingIterator {
    fn next(&mut self) -> Option<&mut TransactionGroup>;
}

/// A key that is used for grouping transactions together.
/// It consists of the signer's account ID and signer's public key.
pub(crate) type AccountPK = (AccountId, PublicKey);

/// Represents a group of transactions with the same key.
pub struct TransactionGroup {
    /// The key of the group.
    pub(crate) key: AccountPK,
    /// Ordered transactions by nonce in non-increasing order (e.g. 3, 2, 2).
    pub(crate) transactions: Vec<SignedTransaction>,
    /// Hashes of the transactions that were pulled from the group using `.next()`.
    pub(crate) removed_transaction_hashes: Vec<CryptoHash>,
}

impl TransactionGroup {
    /// Returns the next transaction with the smallest nonce and removes it from the group.
    /// It also stores all hashes of returned transactions.
    pub fn next(&mut self) -> Option<SignedTransaction> {
        if let Some(tx) = self.transactions.pop() {
            self.removed_transaction_hashes.push(tx.get_hash());
            Some(tx)
        } else {
            None
        }
    }
}
