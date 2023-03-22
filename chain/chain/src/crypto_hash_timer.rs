use lru::LruCache;
use near_primitives::{hash::CryptoHash, static_clock::StaticClock};
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use once_cell::sync::Lazy;

// Cache with the mapping from CryptoHash (blocks, chunks) to the number milliseconds that it took to process them.
// Used only for debugging purposes.
static CRYPTO_HASH_TIMER_RESULTS: Lazy<Mutex<LruCache<CryptoHash, Duration>>> =
    Lazy::new(|| Mutex::new(LruCache::new(10000)));

/// Struct to measure computation times related to different CryptoHashes (for example chunk or block computations).
/// It stores the data in the global LRU cache, which allows it to be read afterwards.
///
/// Example use:
/// ```rust, ignore
/// {
///     let _timer = CryptoHashTime(block.hash);
///     // Do stuff with the block
///     // timer gets released at the end of this block
/// }
/// ```
#[must_use]
pub struct CryptoHashTimer {
    key: CryptoHash,
    start: Instant,
}

impl CryptoHashTimer {
    pub fn new(key: CryptoHash) -> Self {
        Self::new_with_start(key, StaticClock::instant())
    }
    pub fn new_with_start(key: CryptoHash, start: Instant) -> Self {
        CryptoHashTimer { key, start }
    }
    pub fn get_timer_value(key: CryptoHash) -> Option<Duration> {
        CRYPTO_HASH_TIMER_RESULTS.lock().unwrap().get(&key).cloned()
    }
}

impl Drop for CryptoHashTimer {
    fn drop(&mut self) {
        let time_passed = StaticClock::instant() - self.start;
        let mut guard_ = CRYPTO_HASH_TIMER_RESULTS.lock().unwrap();
        let previous = guard_.get(&self.key).copied().unwrap_or_default();
        guard_.put(self.key, time_passed + previous);
    }
}

#[test]
fn test_crypto_hash_timer() {
    let crypto_hash: CryptoHash = "s3N6V7CNAN2Eg6vfivMVHR4hbMZeh72fTmYbrC6dXBT".parse().unwrap();
    // Timer should be missing.
    assert_eq!(CryptoHashTimer::get_timer_value(crypto_hash), None);
    let mock_clock_guard = near_primitives::static_clock::MockClockGuard::default();
    let start_time = Instant::now();
    mock_clock_guard.add_instant(start_time);
    mock_clock_guard.add_instant(start_time + std::time::Duration::from_secs(1));
    {
        let _timer = CryptoHashTimer::new(crypto_hash);
    }
    // Timer should show exactly 1 second (1000 ms).
    assert_eq!(CryptoHashTimer::get_timer_value(crypto_hash), Some(Duration::from_millis(1000)));
    let start_time = Instant::now();
    mock_clock_guard.add_instant(start_time);
    mock_clock_guard.add_instant(start_time + std::time::Duration::from_secs(2));
    {
        let _timer = CryptoHashTimer::new(crypto_hash);
    }
    // Now we run the second time (with the same hash) - so the counter should show 3 seconds.
    assert_eq!(CryptoHashTimer::get_timer_value(crypto_hash), Some(Duration::from_millis(3000)));
}
