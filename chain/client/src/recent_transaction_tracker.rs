//! Best-effort `tx_status` feedback. The RPC handler writes whether a tx is pending or dropped,
//! the view client reads it back, and nothing is persisted.
//!
//! Pending and dropped get separate caches so heavy pending traffic can't evict drops. A hash
//! lives in at most one.

use lru::LruCache;
use near_primitives::hash::CryptoHash;
use std::num::NonZeroUsize;

// Sized to cover ~one validity window of submissions; older entries are silently evicted.
const PENDING_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100_000).unwrap();
// Drops are rare, so a smaller cache retains them far longer than the pending cache.
const DROPPED_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(10_000).unwrap();

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatus {
    Pending(CryptoHash),
    Dropped,
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
        Self {
            pending: LruCache::new(PENDING_CACHE_SIZE),
            dropped: LruCache::new(DROPPED_CACHE_SIZE),
        }
    }

    #[cfg(test)]
    fn with_capacities(pending_cap: usize, dropped_cap: usize) -> Self {
        Self {
            pending: LruCache::new(NonZeroUsize::new(pending_cap).unwrap()),
            dropped: LruCache::new(NonZeroUsize::new(dropped_cap).unwrap()),
        }
    }

    pub fn record_pending(&mut self, tx_hash: CryptoHash, base_block_hash: CryptoHash) {
        self.dropped.pop(&tx_hash);
        self.pending.put(tx_hash, base_block_hash);
    }

    pub fn record_dropped(&mut self, tx_hash: CryptoHash) {
        self.pending.pop(&tx_hash);
        self.dropped.put(tx_hash, ());
    }

    /// Check `dropped` first so a violated invariant still returns the terminal `Dropped`.
    pub fn status(&mut self, tx_hash: &CryptoHash) -> TransactionStatus {
        if self.dropped.get(tx_hash).is_some() {
            return TransactionStatus::Dropped;
        }
        if let Some(&base_block_hash) = self.pending.get(tx_hash) {
            return TransactionStatus::Pending(base_block_hash);
        }
        TransactionStatus::Unknown
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

    use super::{RecentTransactionTracker, TransactionStatus};

    fn hash(n: u8) -> CryptoHash {
        let mut bytes = [0u8; 32];
        bytes[0] = n;
        CryptoHash(bytes)
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
        tracker.record_dropped(tx);
        assert_eq!(tracker.status(&tx), TransactionStatus::Dropped);
    }

    #[test]
    fn unknown_tx_returns_unknown() {
        let mut tracker = RecentTransactionTracker::new();
        assert_eq!(tracker.status(&hash(42)), TransactionStatus::Unknown);
    }

    #[test]
    fn lru_eviction_pending() {
        let cap = 3;
        let mut tracker = RecentTransactionTracker::with_capacities(cap, 1);
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
        let mut tracker = RecentTransactionTracker::with_capacities(1, cap);
        for i in 1..=(cap + 1) as u8 {
            tracker.record_dropped(hash(i));
        }
        assert_eq!(tracker.status(&hash(1)), TransactionStatus::Unknown);
        assert_eq!(tracker.status(&hash(4)), TransactionStatus::Dropped);
    }

    #[test]
    fn pending_then_dropped_ends_in_dropped_only() {
        let mut tracker = RecentTransactionTracker::new();
        let tx = hash(1);
        let base = hash(2);
        tracker.record_pending(tx, base);
        tracker.record_dropped(tx);
        assert_eq!(tracker.status(&tx), TransactionStatus::Dropped);
    }

    #[test]
    fn dropped_then_pending_ends_in_pending_only() {
        let mut tracker = RecentTransactionTracker::new();
        let tx = hash(1);
        let base = hash(2);
        tracker.record_dropped(tx);
        tracker.record_pending(tx, base);
        assert_eq!(tracker.status(&tx), TransactionStatus::Pending(base));
    }

    #[test]
    fn record_absent_from_other_cache_is_noop() {
        let mut tracker = RecentTransactionTracker::new();
        tracker.record_pending(hash(1), hash(2));
        tracker.record_dropped(hash(99));
        tracker.record_dropped(hash(3));
        tracker.record_pending(hash(98), hash(0));
    }

    #[test]
    fn dropped_checked_before_pending() {
        let mut tracker = RecentTransactionTracker::new();
        let tx = hash(1);
        // Bypass the mutual-exclusion API so both caches hold the same hash.
        tracker.pending.put(tx, hash(2));
        tracker.dropped.put(tx, ());
        assert_eq!(tracker.status(&tx), TransactionStatus::Dropped);
    }
}
