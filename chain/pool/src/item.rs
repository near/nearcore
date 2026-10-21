//! What the pool needs to know about the kind of transactions it holds.

use crate::types::PoolKey;
use near_crypto::PublicKey;
use near_primitives::epoch_info::RngSeed;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::transaction::ValidatedTransaction;
use near_primitives::types::{AccountId, NonceIndex};
use std::collections::VecDeque;

/// Identifies a group of items in the pool. A kind of items that is grouped differently adds a
/// variant.
pub enum GroupId<'a> {
    /// Signed transactions of one access key. A gas key has a group for each of its nonces.
    AccessKey {
        account_id: &'a AccountId,
        public_key: &'a PublicKey,
        nonce_index: Option<NonceIndex>,
    },
}

impl GroupId<'_> {
    /// The key of the group in the pool. The seed randomizes the order of the keys.
    pub(crate) fn key(&self, key_seed: &RngSeed) -> PoolKey {
        match self {
            GroupId::AccessKey { account_id, public_key, nonce_index } => {
                let mut v = borsh::to_vec(public_key).unwrap();
                v.extend_from_slice(key_seed);
                v.extend_from_slice(account_id.as_bytes());
                if let Some(idx) = nonce_index {
                    v.extend_from_slice(&borsh::to_vec(idx).unwrap());
                }
                hash(&v)
            }
        }
    }
}

/// A kind of items that the pool can hold. The pool groups its items, and the items of a group
/// wait in a queue with the next one to take at the front. How the items of a group are sorted is
/// up to the kind of items.
pub trait PoolItem: Sized {
    fn hash(&self) -> CryptoHash;

    /// The size that counts against the size limit of the pool.
    fn size(&self) -> u64;

    fn group(&self) -> GroupId<'_>;

    /// Adds a new item to its group.
    fn add_to_group(group: &mut VecDeque<Self>, item: Self);

    /// Sorts the items of a group, so that the one to take next is at the front. The pool
    /// iterator calls it each time it takes a group out of the pool.
    fn sort_group(group: &mut VecDeque<Self>);
}

impl PoolItem for ValidatedTransaction {
    fn hash(&self) -> CryptoHash {
        self.get_hash()
    }

    fn size(&self) -> u64 {
        self.wire_size()
    }

    fn group(&self) -> GroupId<'_> {
        GroupId::AccessKey {
            account_id: self.signer_id(),
            public_key: self.public_key(),
            nonce_index: self.nonce().nonce_index(),
        }
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
