//! Implementation of the token bucket algorithm, used to put limits on
//! bandwidth and burstiness of network traffic.
//!
//! The algorithm depicts an imaginary bucket into which tokens are added
//! at regular intervals of time. The bucket has a well defined maximum size
//! and overflowing tokens are simply discarded.
//! Network traffic (packets, messages, etc) 'consume' a given amount of tokens
//! in order to be allowed to pass.
//! If there aren't enough tokens in the bucket, the traffic might be stopped
//! or delayed. However, this module responsibility stops at telling
//! whether or not the incoming messages are allowed.

use std::sync::atomic::{AtomicU64, Ordering};

use near_async::time::Duration;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum TokenBucketError {
    #[error("invalid value for refill rate ({0})")]
    InvalidRefillRate(f32),
}

/// Into how many parts a token can be divided.
const TOKEN_PARTS_NUMBER: u64 = 1 << 31;

/// Struct to hold the state for the token bucket algorithm. Thread-safe.
///
/// The precision guarantee is, at least, such that a bucket having `refill_rate` = 0.001s
/// update at regular intervals every 10ms will successfully generate a token after 1000±1s.
pub struct TokenBucket {
    maximum_size: u32,
    /// Tokens in the bucket. They are stored as `tokens * TOKEN_PARTS_NUMBER`.
    /// In this way we can refill the bucket at shorter intervals.  
    size: AtomicU64,
    /// Refill rate in token per second.
    refill_rate: f32,
}

impl TokenBucket {
    /// Creates a new token bucket.
    ///
    /// # Arguments
    ///
    /// * `initial_size` - Initial amount of tokens in the bucket
    /// * `maximum_size` - Maximum amount of tokens the bucket can hold
    /// * `refill_rate` - Bucket refill rate in token per second
    ///
    /// # Errors
    ///
    /// Returns an error if any of the arguments has an invalid value.
    pub fn new(
        initial_size: u32,
        maximum_size: u32,
        refill_rate: f32,
    ) -> Result<Self, TokenBucketError> {
        let size = to_tokens_with_parts(maximum_size.min(initial_size));
        let size = AtomicU64::new(size);
        if refill_rate < 0.0 {
            return Err(TokenBucketError::InvalidRefillRate(refill_rate));
        }
        if !refill_rate.is_normal() && refill_rate != 0.0 {
            return Err(TokenBucketError::InvalidRefillRate(refill_rate));
        }
        Ok(Self { maximum_size, size, refill_rate })
    }

    /// Makes an attempt to acquire `token` tokens.
    ///
    /// If the tokens are available they are subtracted from the current `size` and
    /// the method returns `true`. Otherwise, `size` is not changed and the method
    /// returns `false`.
    pub fn acquire(&self, tokens: u32) -> bool {
        let tokens = to_tokens_with_parts(tokens);
        self.size
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |size| {
                if size >= tokens {
                    Some(size - tokens)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Refills the bucket with the right number of tokens according to
    /// the `refill_rate`, assuming an interval of time equal to `duration`
    /// has passed. Negative durations are ignored.
    ///
    /// For example: if `refill_rate` == 1 and `duration` == 1s then exactly 1 token
    /// will be added.
    pub fn refill(&self, duration: Duration) {
        if duration.is_negative() || duration.is_zero() {
            return;
        }
        let tokens_to_add = duration.as_seconds_f64() * self.refill_rate as f64;
        let tokens_to_add = tokens_to_add * TOKEN_PARTS_NUMBER as f64;
        // Always returns `Some`.
        let _ = self.size.fetch_update(Ordering::AcqRel, Ordering::Acquire, |size| {
            let new_size = size
                .saturating_add(tokens_to_add as u64)
                .min(to_tokens_with_parts(self.maximum_size));
            Some(new_size)
        });
    }

    #[cfg(test)]
    fn size(&self) -> u64 {
        self.size.load(Ordering::Acquire)
    }
}

/// Transforms a value of `tokens` without a fractional part into a representation
/// having a fractional part.
fn to_tokens_with_parts(tokens: u32) -> u64 {
    tokens as u64 * TOKEN_PARTS_NUMBER
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        thread::{self, sleep},
    };

    use near_async::time::Instant;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn token_fractional_representation_cant_overflow() {
        assert!(TOKEN_PARTS_NUMBER.saturating_mul(u32::MAX as u64) < u64::MAX);
    }

    #[test]
    fn initial_more_than_max() {
        let bucket = TokenBucket::new(5, 2, 1.0).expect("bucket should be well formed");
        assert_eq!(bucket.size(), to_tokens_with_parts(2));
        assert_eq!(bucket.maximum_size, 2);
    }

    #[test]
    fn invalid_refill_rate() {
        assert!(TokenBucket::new(2, 2, f32::NAN).is_err());
        assert!(TokenBucket::new(2, 2, f32::INFINITY).is_err());
        assert!(TokenBucket::new(2, 2, f32::NEG_INFINITY).is_err());
        assert!(TokenBucket::new(2, 2, -1.0).is_err());
    }

    #[test]
    fn valid_refill_rate() {
        assert!(TokenBucket::new(2, 2, 0.0).is_ok());
        assert!(TokenBucket::new(2, 2, 0.3).is_ok());
    }

    #[test]
    fn acquire() {
        let bucket = TokenBucket::new(5, 10, 1.0).expect("bucket should be well formed");

        assert!(bucket.acquire(0));
        assert_eq!(bucket.size(), to_tokens_with_parts(5));

        assert!(bucket.acquire(1));
        assert_eq!(bucket.size(), to_tokens_with_parts(4));

        assert!(!bucket.acquire(10));
        assert_eq!(bucket.size(), to_tokens_with_parts(4));

        assert!(bucket.acquire(4));
        assert_eq!(bucket.size(), to_tokens_with_parts(0));

        assert!(!bucket.acquire(1));
        assert_eq!(bucket.size(), to_tokens_with_parts(0));
    }

    #[test]
    fn max_is_zero() {
        let bucket = TokenBucket::new(0, 0, 0.0).expect("bucket should be well formed");
        assert!(bucket.acquire(0));
        assert!(!bucket.acquire(1));
    }

    #[test]
    fn refill() {
        let bucket = TokenBucket::new(0, 1000, 10.0).expect("bucket should be well formed");
        bucket.refill(Duration::seconds(2));
        assert_eq!(bucket.size(), to_tokens_with_parts(20));
        bucket.refill(Duration::milliseconds(500));
        assert_eq!(bucket.size(), to_tokens_with_parts(20 + 5));
    }

    #[test]
    fn refill_zero_rate() {
        let bucket = TokenBucket::new(10, 10, 0.0).expect("bucket should be well formed");
        assert!(bucket.acquire(10));
        assert!(!bucket.acquire(1));
        bucket.refill(Duration::seconds(100));
        assert!(!bucket.acquire(1));
    }

    #[test]
    fn refill_non_positive_duration() {
        let bucket = TokenBucket::new(10, 10, 1.0).expect("bucket should be well formed");
        let size = bucket.size();
        bucket.refill(Duration::seconds(0));
        bucket.refill(Duration::seconds(-1));
        assert_eq!(bucket.size(), size);
    }

    #[test]
    fn refill_partial_token() {
        let bucket = TokenBucket::new(0, 5, 0.4).expect("bucket should be well formed");
        assert!(!bucket.acquire(1));
        bucket.refill(Duration::seconds(1));
        assert!(!bucket.acquire(1));
        bucket.refill(Duration::seconds(1));
        assert!(!bucket.acquire(1));
        bucket.refill(Duration::seconds(1));
        assert!(bucket.acquire(1));
    }

    #[test]
    fn refill_overflow_bucket_max_size() {
        let bucket = TokenBucket::new(2, 5, 1.0).expect("bucket should be well formed");
        bucket.refill(Duration::seconds(2));
        assert_eq!(bucket.size(), to_tokens_with_parts(4));
        bucket.refill(Duration::seconds(2));
        assert_eq!(bucket.size(), to_tokens_with_parts(5));
        assert!(bucket.acquire(5));
        bucket.refill(Duration::seconds(10));
        assert_eq!(bucket.size(), to_tokens_with_parts(5));
    }

    #[test]
    fn check_with_numeric_limits() {
        let bucket =
            TokenBucket::new(u32::MAX, u32::MAX, 1000.0).expect("bucket should be well formed");
        assert!(bucket.acquire(u32::MAX));
        assert!(!bucket.acquire(1));
        bucket.refill(Duration::seconds(i64::MAX));
        assert!(bucket.acquire(u32::MAX));
        assert!(!bucket.acquire(1));
    }

    #[test]
    /// Validate if `TokenBucket` meets the requirement of being able to refresh tokens successfully
    /// when both the refill rate and the elapsed time are very low.
    fn validate_guaranteed_resolution() {
        let bucket = TokenBucket::new(0, 10, 0.001).expect("bucket should be well formed");
        // Up to 999s: no new token added.
        for _ in 0..99_900 {
            bucket.refill(Duration::milliseconds(10));
        }
        assert!(!bucket.acquire(1));
        // From 999s to 1001s: the new token should get added.
        let mut tokens_added = 0;
        for _ in 99_900..100_100 {
            bucket.refill(Duration::milliseconds(10));
            if bucket.acquire(1) {
                tokens_added += 1;
            }
        }
        assert_eq!(tokens_added, 1);
    }

    #[test]
    fn parallelism_happy_path() {
        // Start two thread.
        // Thread A refills tokens.
        // Thread B consumes tokens.
        // Thread B should successfully consume tokens as they get in.
        let bucket = TokenBucket::new(0, 10000, 100.0).expect("bucket should be well formed");
        let bucket = Arc::new(bucket);

        let bucket_clone = bucket.clone();
        let thread_a = thread::spawn(move || {
            for _ in 0..10 {
                // Sleep 10 times x 100ms => produce 100 tokens
                const PERIOD: Duration = Duration::milliseconds(100);
                sleep(PERIOD.try_into().unwrap());
                bucket_clone.refill(PERIOD);
            }
        });

        let thread_b = thread::spawn(move || {
            // Must get 100 tokens.
            let mut acquired = 0;
            let shutdown_time = Instant::now() + Duration::seconds(3);
            loop {
                if bucket.acquire(1) {
                    acquired += 1;
                }
                if acquired >= 100 || Instant::now() >= shutdown_time {
                    break;
                }
                sleep(std::time::Duration::from_millis(1));
            }
            acquired
        });

        thread_a.join().unwrap();
        let acquired = thread_b.join().unwrap();
        assert_eq!(acquired, 100);
    }
}
