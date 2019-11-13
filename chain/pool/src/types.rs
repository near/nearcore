use near_crypto::PublicKey;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;

pub trait DrainingIterator {
    fn next(&mut self) -> Option<&mut TransactionGroup>;
}

pub(crate) type AccountPK = (AccountId, PublicKey);

/// Represents a group of transactions with the same key.
pub struct TransactionGroup {
    pub(crate) key: AccountPK,
    pub(crate) transactions: Vec<SignedTransaction>,
}

impl TransactionGroup {
    pub fn next(&mut self) -> Option<SignedTransaction> {
        self.transactions.pop()
    }
}
