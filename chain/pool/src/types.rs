use near_crypto::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;

pub trait DrainingIterator {
    fn next(&mut self) -> Option<&mut TransactionGroup>;
}

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
    pub fn next(&mut self) -> Option<SignedTransaction> {
        if let Some(tx) = self.transactions.pop() {
            self.removed_transaction_hashes.push(tx.get_hash());
            Some(tx)
        } else {
            None
        }
    }
}
