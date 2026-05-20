//! Best-effort `tx_status` feedback. The RPC handler writes whether a tx is pending or dropped,
//! the view client reads it back, and nothing is persisted.
//!
//! Pending and dropped get separate caches so heavy pending traffic can't evict drops. A hash
//! lives in at most one.

use lru::LruCache;
use near_primitives::hash::CryptoHash;
use std::num::NonZeroUsize;

// Sized to cover ~one validity window of submissions; older entries are silently evicted.
pub const DEFAULT_PENDING_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100_000).unwrap();
// Drops are rare, so a smaller cache retains them far longer than the pending cache.
pub const DEFAULT_DROPPED_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(10_000).unwrap();

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatus {
    Pending(CryptoHash),
    DroppedMempoolFull,
    Unknown,
}

#[derive(Debug)]
pub struct RecentTransactionTracker {
    pending: LruCache<CryptoHash, CryptoHash>,
    dropped: LruCache<CryptoHash, ()>,
}

impl RecentTransactionTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacities(DEFAULT_PENDING_CACHE_SIZE, DEFAULT_DROPPED_CACHE_SIZE)
    }

    #[must_use]
    pub fn with_capacities(pending_cap: NonZeroUsize, dropped_cap: NonZeroUsize) -> Self {
        Self { pending: LruCache::new(pending_cap), dropped: LruCache::new(dropped_cap) }
    }

    pub fn record_pending(&mut self, tx_hash: CryptoHash, base_block_hash: CryptoHash) {
        self.dropped.pop(&tx_hash);
        self.pending.put(tx_hash, base_block_hash);
    }

    pub fn record_dropped_mempool_full(&mut self, tx_hash: CryptoHash) {
        self.pending.pop(&tx_hash);
        self.dropped.put(tx_hash, ());
    }

    /// Check `dropped` first so a violated invariant still returns the terminal `Dropped*` status.
    pub fn status(&mut self, tx_hash: &CryptoHash) -> TransactionStatus {
        if self.dropped.get(tx_hash).is_some() {
            return TransactionStatus::DroppedMempoolFull;
        }
        if let Some(&base_block_hash) = self.pending.get(tx_hash) {
            return TransactionStatus::Pending(base_block_hash);
        }
        TransactionStatus::Unknown
    }

    #[cfg(test)]
    fn force_both_for_test(&mut self, tx_hash: CryptoHash, base_block_hash: CryptoHash) {
        self.pending.put(tx_hash, base_block_hash);
        self.dropped.put(tx_hash, ());
    }
}

impl Default for RecentTransactionTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::CryptoHash;
    use std::num::NonZeroUsize;

    use super::{RecentTransactionTracker, TransactionStatus};

    fn hash(n: u8) -> CryptoHash {
        let mut bytes = [0u8; 32];
        bytes[0] = n;
        CryptoHash(bytes)
    }

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[test]
    fn pending_lookup_returns_base_block_hash() {
        let mut tracker = RecentTransactionTracker::new();
        let tx = hash(1);
        let base = hash(2);
        tracker.record_pending(tx, base);
        assert_eq!(tracker.status(&tx), TransactionStatus::Pending(base));
    }

    #[test]
    fn dropped_lookup_returns_dropped() {
        let mut tracker = RecentTransactionTracker::new();
        let tx = hash(1);
        tracker.record_dropped_mempool_full(tx);
        assert_eq!(tracker.status(&tx), TransactionStatus::DroppedMempoolFull);
    }

    #[test]
    fn unknown_tx_returns_unknown() {
        let mut tracker = RecentTransactionTracker::new();
        assert_eq!(tracker.status(&hash(42)), TransactionStatus::Unknown);
    }

    #[test]
    fn lru_eviction_pending() {
        let cap = 3;
        let mut tracker = RecentTransactionTracker::with_capacities(nz(cap), nz(1));
        let base = hash(0);
        for i in 1..=(cap + 1) as u8 {
            tracker.record_pending(hash(i), base);
        }
        assert_eq!(tracker.status(&hash(1)), TransactionStatus::Unknown);
        assert_eq!(tracker.status(&hash(4)), TransactionStatus::Pending(base));
    }

    #[test]
    fn lru_eviction_dropped() {
        let cap = 3;
        let mut tracker = RecentTransactionTracker::with_capacities(nz(1), nz(cap));
        for i in 1..=(cap + 1) as u8 {
            tracker.record_dropped_mempool_full(hash(i));
        }
        assert_eq!(tracker.status(&hash(1)), TransactionStatus::Unknown);
        assert_eq!(tracker.status(&hash(4)), TransactionStatus::DroppedMempoolFull);
    }

    #[test]
    fn pending_then_dropped_ends_in_dropped_only() {
        let mut tracker = RecentTransactionTracker::new();
        let tx = hash(1);
        let base = hash(2);
        tracker.record_pending(tx, base);
        tracker.record_dropped_mempool_full(tx);
        assert_eq!(tracker.status(&tx), TransactionStatus::DroppedMempoolFull);
    }

    #[test]
    fn dropped_then_pending_ends_in_pending_only() {
        let mut tracker = RecentTransactionTracker::new();
        let tx = hash(1);
        let base = hash(2);
        tracker.record_dropped_mempool_full(tx);
        tracker.record_pending(tx, base);
        assert_eq!(tracker.status(&tx), TransactionStatus::Pending(base));
    }

    #[test]
    fn record_absent_from_other_cache_is_noop() {
        let mut tracker = RecentTransactionTracker::new();
        tracker.record_pending(hash(1), hash(2));
        tracker.record_dropped_mempool_full(hash(99));
        tracker.record_dropped_mempool_full(hash(3));
        tracker.record_pending(hash(98), hash(0));
    }

    #[test]
    fn dropped_checked_before_pending() {
        let mut tracker = RecentTransactionTracker::new();
        let tx = hash(1);
        tracker.force_both_for_test(tx, hash(2));
        assert_eq!(tracker.status(&tx), TransactionStatus::DroppedMempoolFull);
    }
}
