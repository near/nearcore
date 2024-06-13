use lru::LruCache;
use near_async::time::{Clock, Duration, Instant};
use near_primitives::hash::CryptoHash;
use once_cell::sync::Lazy;
use std::num::NonZeroUsize;
use std::sync::Mutex;

// Cache with the mapping from CryptoHash (blocks, chunks) to the number milliseconds that it took to process them.
// Used only for debugging purposes.
static CRYPTO_HASH_TIMER_RESULTS: Lazy<Mutex<LruCache<CryptoHash, Duration>>> =
    Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(10000).unwrap())));

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
    clock: Clock,
    key: CryptoHash,
    start: Instant,
}

impl CryptoHashTimer {
    pub fn new(clock: Clock, key: CryptoHash) -> Self {
        Self::new_with_start(clock.clone(), key, clock.now())
    }
    pub fn new_with_start(clock: Clock, key: CryptoHash, start: Instant) -> Self {
        CryptoHashTimer { clock, key, start }
    }
    pub fn get_timer_value(key: CryptoHash) -> Option<Duration> {
        CRYPTO_HASH_TIMER_RESULTS.lock().unwrap().get(&key).cloned()
    }
}

impl Drop for CryptoHashTimer {
    fn drop(&mut self) {
        let time_passed = self.clock.now() - self.start;
        let mut guard_ = CRYPTO_HASH_TIMER_RESULTS.lock().unwrap();
        let previous = guard_.get(&self.key).copied().unwrap_or_default();
        guard_.put(self.key, time_passed + previous);
    }
}

#[cfg(test)]
mod tests {
    use crate::crypto_hash_timer::CryptoHashTimer;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_primitives::hash::CryptoHash;

    #[test]
    fn test_crypto_hash_timer() {
        let crypto_hash: CryptoHash =
            "s3N6V7CNAN2Eg6vfivMVHR4hbMZeh72fTmYbrC6dXBT".parse().unwrap();
        // Timer should be missing.
        let clock = FakeClock::new(Utc::UNIX_EPOCH);
        assert_eq!(CryptoHashTimer::get_timer_value(crypto_hash), None);
        {
            let _timer = CryptoHashTimer::new(clock.clock(), crypto_hash);
            clock.advance(Duration::seconds(1));
        }
        // Timer should show exactly 1 second (1000 ms).
        assert_eq!(
            CryptoHashTimer::get_timer_value(crypto_hash),
            Some(Duration::milliseconds(1000))
        );

        {
            let _timer = CryptoHashTimer::new(clock.clock(), crypto_hash);
            clock.advance(Duration::seconds(2));
        }
        // Now we run the second time (with the same hash) - so the counter should show 3 seconds.
        assert_eq!(
            CryptoHashTimer::get_timer_value(crypto_hash),
            Some(Duration::milliseconds(3000))
        );
    }
}
