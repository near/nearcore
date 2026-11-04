use crate::item::PoolItem;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{ValidatedEcc, ValidatedTransaction};
use std::collections::VecDeque;

/// Trait acts like an iterator. It iterates over transactions groups by returning mutable
/// references to them. Each transaction group implements a draining iterator to pull transactions.
/// The order of the transaction groups is round robin scheduling.
pub trait TransactionGroupIterator {
    fn next(&mut self) -> Option<PooledGroup<'_>>;
}

/// A hash of the group's identity (e.g. an AccountId and a PublicKey, or the AccountId of a
/// contract) and a seed.
/// Used to randomize the order of the keys.
pub(crate) type PoolKey = CryptoHash;

/// Represents a group of items with the same key.
pub struct Group<T> {
    /// The key of the group.
    pub(crate) key: PoolKey,
    /// The items in the order they are taken in, the next one at the front.
    pub(crate) items: VecDeque<T>,
    /// Hashes of the items that were pulled from the group using `.next()`.
    pub(crate) removed_hashes: Vec<CryptoHash>,
    /// Total size of items that were pulled from the group using `.next()`.
    pub(crate) removed_size: u64,
    /// Set to `true` when `next()` pops an item, reset to `false` when
    /// the iterator yields this group. Used to detect stalled groups.
    pub(crate) removed_any_since_yield: bool,
}

/// A group of signed transactions with the same key, taken in the order of nonces.
pub type TransactionGroup = Group<ValidatedTransaction>;

/// A group of ECCs to the same contract, taken in the order of arrival.
pub type EccGroup = Group<ValidatedEcc>;

/// A group of transactions that the pool iterator hands out.
pub enum PooledGroup<'a> {
    Signed(&'a mut TransactionGroup),
    Ecc(&'a mut EccGroup),
}

impl<T: PoolItem> Group<T> {
    /// Returns the next item and removes it from the group. For transactions that is the one
    /// with the smallest nonce, and for ECCs the oldest one.
    /// It also stores all hashes of returned items.
    pub fn next(&mut self) -> Option<T> {
        if let Some(item) = self.items.pop_front() {
            self.removed_hashes.push(item.hash());
            self.removed_size += item.size();
            self.removed_any_since_yield = true;
            Some(item)
        } else {
            None
        }
    }

    pub fn peek_next(&self) -> Option<&T> {
        self.items.front()
    }
}
