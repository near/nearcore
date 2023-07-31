use near_primitives::hash::CryptoHash;

/// Trait acts like an iterator. It iterates over transactions groups by returning mutable
/// references to them. Each transaction group implements a draining iterator to pull transactions.
/// The order of the transaction groups is round robin scheduling.
/// When this iterator is dropped the remaining transactions are returned back to the pool.
// pub type PoolIterator<'a> = dyn Iterator<Item = &'a SignedTransaction>;

/// A hash of (an AccountId, a PublicKey and a seed).
/// Used to randomize the order of the keys.
pub type PoolKey = CryptoHash;
