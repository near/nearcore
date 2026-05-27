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
pub enum TransactionFate {
    Pending { base_block_hash: CryptoHash },
    DroppedMempoolFull,
    Unknown,
}

#[derive(Debug)]
pub struct RecentTxFateCache {
    pending: LruCache<CryptoHash, CryptoHash>,
    dropped: LruCache<CryptoHash, ()>,
}

impl RecentTxFateCache {
    pub fn new() -> Self {
        Self::with_capacities(DEFAULT_PENDING_CACHE_SIZE, DEFAULT_DROPPED_CACHE_SIZE)
    }

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

    pub fn fate(&mut self, tx_hash: &CryptoHash) -> TransactionFate {
        if self.dropped.get(tx_hash).is_some() {
            return TransactionFate::DroppedMempoolFull;
        }
        if let Some(&base_block_hash) = self.pending.get(tx_hash) {
            return TransactionFate::Pending { base_block_hash };
        }
        TransactionFate::Unknown
    }

    #[cfg(test)]
    fn force_both_for_test(&mut self, tx_hash: CryptoHash, base_block_hash: CryptoHash) {
        self.pending.put(tx_hash, base_block_hash);
        self.dropped.put(tx_hash, ());
    }
}

impl Default for RecentTxFateCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{RecentTxFateCache, TransactionFate};
    use near_primitives::hash::CryptoHash;
    use std::num::NonZeroUsize;

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
        let mut cache = RecentTxFateCache::new();
        let tx = hash(1);
        let base = hash(2);
        cache.record_pending(tx, base);
        assert_eq!(cache.fate(&tx), TransactionFate::Pending { base_block_hash: base });
    }

    #[test]
    fn dropped_lookup_returns_dropped() {
        let mut cache = RecentTxFateCache::new();
        let tx = hash(1);
        cache.record_dropped_mempool_full(tx);
        assert_eq!(cache.fate(&tx), TransactionFate::DroppedMempoolFull);
    }

    #[test]
    fn unknown_tx_returns_unknown() {
        let mut cache = RecentTxFateCache::new();
        assert_eq!(cache.fate(&hash(42)), TransactionFate::Unknown);
    }

    #[test]
    fn lru_eviction_pending() {
        let cap = 3;
        let mut cache = RecentTxFateCache::with_capacities(nz(cap), nz(1));
        let base = hash(0);
        for i in 1..=(cap + 1) as u8 {
            cache.record_pending(hash(i), base);
        }
        assert_eq!(cache.fate(&hash(1)), TransactionFate::Unknown);
        assert_eq!(cache.fate(&hash(4)), TransactionFate::Pending { base_block_hash: base });
    }

    #[test]
    fn lru_eviction_dropped() {
        let cap = 3;
        let mut cache = RecentTxFateCache::with_capacities(nz(1), nz(cap));
        for i in 1..=(cap + 1) as u8 {
            cache.record_dropped_mempool_full(hash(i));
        }
        assert_eq!(cache.fate(&hash(1)), TransactionFate::Unknown);
        assert_eq!(cache.fate(&hash(4)), TransactionFate::DroppedMempoolFull);
    }

    #[test]
    fn pending_then_dropped_ends_in_dropped_only() {
        let mut cache = RecentTxFateCache::new();
        let tx = hash(1);
        let base = hash(2);
        cache.record_pending(tx, base);
        cache.record_dropped_mempool_full(tx);
        assert_eq!(cache.fate(&tx), TransactionFate::DroppedMempoolFull);
    }

    #[test]
    fn dropped_then_pending_ends_in_pending_only() {
        let mut cache = RecentTxFateCache::new();
        let tx = hash(1);
        let base = hash(2);
        cache.record_dropped_mempool_full(tx);
        cache.record_pending(tx, base);
        assert_eq!(cache.fate(&tx), TransactionFate::Pending { base_block_hash: base });
    }

    #[test]
    fn dropped_checked_before_pending() {
        let mut cache = RecentTxFateCache::new();
        let tx = hash(1);
        cache.force_both_for_test(tx, hash(2));
        assert_eq!(cache.fate(&tx), TransactionFate::DroppedMempoolFull);
    }
}
