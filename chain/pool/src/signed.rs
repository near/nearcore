//! What is specific to signed transactions in the pool.

use crate::item::{GroupId, PoolItem};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{SignedTransaction, ValidatedTransaction};
use std::collections::VecDeque;

/// The group of a signed transaction: the transactions of one access key are in one group.
pub(crate) fn group_id(signed_tx: &SignedTransaction) -> GroupId<'_> {
    let tx = &signed_tx.transaction;
    GroupId::AccessKey {
        account_id: tx.signer_id(),
        public_key: tx.public_key(),
        nonce_index: tx.nonce().nonce_index(),
    }
}

impl PoolItem for ValidatedTransaction {
    fn hash(&self) -> CryptoHash {
        self.get_hash()
    }

    fn size(&self) -> u64 {
        self.wire_size()
    }

    fn group(&self) -> GroupId<'_> {
        group_id(self.to_signed_tx())
    }

    /// The transactions of a group are taken in the order of nonces, and are only sorted when
    /// the group is taken out of the pool. It is more efficient on average to keep them
    /// unsorted and with potentially conflicting nonce than to keep them sorted all the time.
    ///
    /// The one that was added last goes to the front, so that among transactions with the same
    /// nonce it is the one taken first. Sorting is stable, so this order survives it.
    fn add_to_group(group: &mut VecDeque<Self>, item: Self) {
        group.push_front(item);
    }

    fn sort_group(group: &mut VecDeque<Self>) {
        group.make_contiguous().sort_by_key(|tx| tx.nonce().nonce());
    }
}
