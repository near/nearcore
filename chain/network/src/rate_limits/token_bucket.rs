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

use near_async::time::Instant;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum TokenBucketError {
    #[error("invalid value for refill rate ({0})")]
    InvalidRefillRate(f32),
}

/// Into how many parts a token can be divided.
const TOKEN_PARTS_NUMBER: u64 = 1 << 31;

/// Struct to hold the state for the token bucket algorithm.
///
/// The precision guarantee is, at least, such that a bucket having `refill_rate` = 0.001s
/// update at regular intervals every 10ms will successfully generate a token after 1000±1s.
pub struct TokenBucket {
    /// Maximum number of tokens the bucket can hold.
    maximum_size: u32,
    /// Tokens in the bucket. They are stored as `tokens * TOKEN_PARTS_NUMBER`.
    /// In this way we can refill the bucket at shorter intervals.  
    size: u64,
    /// Refill rate in token per second.
    refill_rate: f32,
    /// Last time the bucket was refreshed.
    last_refill: Option<Instant>,
}

impl TokenBucket {
    /// Creates a new token bucket.
    ///
    /// # Arguments
    ///
    /// * `initial_size` - Initial amount of tokens in the bucket
    /// * `maximum_size` - Maximum amount of tokens the bucket can hold
    /// * `refill_rate` - Bucket refill rate in token per second
    /// * `start_time` - Point in time used as a start to calculate the bucket refill.
    /// Set it to `None` to start counting the first time [TokenBucket::acquire] is called.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the arguments has an invalid value.
    pub fn new(
        initial_size: u32,
        maximum_size: u32,
        refill_rate: f32,
        start_time: Option<Instant>,
    ) -> Result<Self, TokenBucketError> {
        let size = to_tokens_with_parts(maximum_size.min(initial_size));
        TokenBucket::validate_refill_rate(refill_rate)?;
        Ok(Self { maximum_size, size, refill_rate, last_refill: start_time })
    }

    /// Makes an attempt to acquire `token` tokens.
    ///
    /// This method takes a parameter called `now` which should be equivalent to the current time.
    /// The latter is used to refill the bucket before subtracting tokens.
    ///
    /// If the tokens are available they are subtracted from the current `size` and
    /// the method returns `true`. Otherwise, `size` is not changed and the method
    /// returns `false`.
    pub fn acquire(&mut self, tokens: u32, now: Instant) -> bool {
        self.refill(now);
        let tokens = to_tokens_with_parts(tokens);
        if self.size >= tokens {
            self.size -= tokens;
            true
        } else {
            false
        }
    }

    /// Refills the bucket with the right number of tokens according to
    /// the `refill_rate` and the new current time `now`.
    ///
    /// For example: if `refill_rate` == 1 and `now - last_refill` == 1s then exactly 1 token
    /// will be added.
    fn refill(&mut self, now: Instant) {
        let last_refill = if let Some(val) = self.last_refill {
            val
        } else {
            // Simply set `last_refill` if this's the first refill.
            self.last_refill = Some(now);
            return;
        };
        // Sanity check: now should be bigger than the last refill time.
        if now <= last_refill {
            return;
        }
        // Compute how many tokens should be added to the current size.
        let duration = now - last_refill;
        let tokens_to_add = duration.as_secs_f64() * self.refill_rate as f64;
        let tokens_to_add = (tokens_to_add * TOKEN_PARTS_NUMBER as f64) as u64;
        // Update `last_refill` and `size` only if there's a change. This is done to prevent
        // losing token parts to clamping if the duration is too small.
        if tokens_to_add > 0 {
            self.size = self
                .size
                .saturating_add(tokens_to_add)
                .min(to_tokens_with_parts(self.maximum_size));
            self.last_refill = Some(now);
        }
    }

    /// Returns an error if the value provided is not in the correct range for
    /// `refill_rate`.
    pub(crate) fn validate_refill_rate(refill_rate: f32) -> Result<(), TokenBucketError> {
        if refill_rate < 0.0 {
            return Err(TokenBucketError::InvalidRefillRate(refill_rate));
        }
        if !refill_rate.is_normal() && refill_rate != 0.0 {
            return Err(TokenBucketError::InvalidRefillRate(refill_rate));
        }
        Ok(())
    }
}

/// Transforms a value of `tokens` without a fractional part into a representation
/// having a fractional part.
fn to_tokens_with_parts(tokens: u32) -> u64 {
    // Safe (check the test `token_fractional_representation_cant_overflow`).
    tokens as u64 * TOKEN_PARTS_NUMBER
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_async::time::{Duration, Instant};

    #[test]
    fn token_fractional_representation_cant_overflow() {
        assert!(TOKEN_PARTS_NUMBER.saturating_mul(u32::MAX as u64) < u64::MAX);
    }

    #[test]
    fn initial_more_than_max() {
        let bucket = TokenBucket::new(5, 2, 1.0, None).expect("bucket should be well formed");
        assert_eq!(bucket.size, to_tokens_with_parts(2));
        assert_eq!(bucket.maximum_size, 2);
    }

    #[test]
    fn invalid_refill_rate() {
        assert!(TokenBucket::new(2, 2, f32::NAN, None).is_err());
        assert!(TokenBucket::new(2, 2, f32::INFINITY, None).is_err());
        assert!(TokenBucket::new(2, 2, f32::NEG_INFINITY, None).is_err());
        assert!(TokenBucket::new(2, 2, -1.0, None).is_err());
    }

    #[test]
    fn valid_refill_rate() {
        assert!(TokenBucket::new(2, 2, 0.0, None).is_ok());
        assert!(TokenBucket::new(2, 2, 0.3, None).is_ok());
    }

    #[test]
    fn acquire() {
        let mut bucket = TokenBucket::new(5, 10, 1.0, None).expect("bucket should be well formed");
        let now = Instant::now();

        assert!(bucket.acquire(0, now));
        assert_eq!(bucket.size, to_tokens_with_parts(5));

        assert!(bucket.acquire(1, now));
        assert_eq!(bucket.size, to_tokens_with_parts(4));

        assert!(!bucket.acquire(10, now));
        assert_eq!(bucket.size, to_tokens_with_parts(4));

        assert!(bucket.acquire(4, now));
        assert_eq!(bucket.size, to_tokens_with_parts(0));

        assert!(!bucket.acquire(1, now));
        assert_eq!(bucket.size, to_tokens_with_parts(0));
    }

    #[test]
    fn max_is_zero() {
        let mut bucket = TokenBucket::new(0, 0, 0.0, None).expect("bucket should be well formed");
        let now = Instant::now();
        assert!(bucket.acquire(0, now));
        assert!(!bucket.acquire(1, now));
    }

    #[test]
    fn buckets_get_refilled() {
        let now = Instant::now();
        let mut bucket =
            TokenBucket::new(0, 1000, 10.0, Some(now)).expect("bucket should be well formed");
        assert!(!bucket.acquire(1, now));
        assert!(bucket.acquire(1, now + Duration::milliseconds(500)));
    }

    #[test]
    fn time_is_initialized_correctly() {
        let now = Instant::now();

        let mut bucket = TokenBucket::new(0, 5, 1.0, None).expect("bucket should be well formed");
        assert!(bucket.last_refill.is_none());
        let size = bucket.size;
        bucket.refill(now);
        assert_eq!(bucket.last_refill, Some(now));
        assert_eq!(bucket.size, size);

        let mut bucket =
            TokenBucket::new(0, 5, 1.0, Some(now)).expect("bucket should be well formed");
        assert!(bucket.last_refill.is_some());
        assert!(bucket.acquire(1, now + Duration::seconds(1)));
    }

    #[test]
    fn zero_refill_rate() {
        let mut bucket = TokenBucket::new(10, 10, 0.0, None).expect("bucket should be well formed");
        let now = Instant::now();
        assert!(bucket.acquire(10, now));
        assert!(!bucket.acquire(1, now));
        assert!(!bucket.acquire(1, now + Duration::seconds(100)));
    }

    #[test]
    fn refill_no_time_elapsed() {
        let now = Instant::now();
        let mut bucket =
            TokenBucket::new(10, 10, 1.0, Some(now)).expect("bucket should be well formed");
        let size = bucket.size;
        bucket.refill(now);
        assert_eq!(bucket.size, size);
    }

    #[test]
    fn check_non_monotonic_clocks_safety() {
        let now = Instant::now();
        let mut bucket =
            TokenBucket::new(10, 10, 1.0, Some(now)).expect("bucket should be well formed");
        let size = bucket.size;
        bucket.refill(now - Duration::seconds(100));
        assert_eq!(bucket.size, size);
    }

    #[test]
    fn refill_partial_token() {
        let now = Instant::now();
        let mut bucket =
            TokenBucket::new(0, 5, 0.4, Some(now)).expect("bucket should be well formed");
        assert!(!bucket.acquire(1, now));
        assert!(!bucket.acquire(1, now + Duration::seconds(1)));
        assert!(!bucket.acquire(1, now + Duration::seconds(2)));
        assert!(bucket.acquire(1, now + Duration::seconds(3)));
    }

    #[test]
    fn refill_overflow_bucket_max_size() {
        let now = Instant::now();
        let mut bucket =
            TokenBucket::new(2, 5, 1.0, Some(now)).expect("bucket should be well formed");

        bucket.refill(now + Duration::seconds(2));
        assert_eq!(bucket.size, to_tokens_with_parts(4));

        bucket.refill(now + Duration::seconds(4));
        assert_eq!(bucket.size, to_tokens_with_parts(5));

        assert!(bucket.acquire(5, now + Duration::seconds(4)));
        assert_eq!(bucket.size, to_tokens_with_parts(0));

        assert!(bucket.acquire(5, now + Duration::seconds(10)));
        assert_eq!(bucket.size, to_tokens_with_parts(0));
    }

    #[test]
    fn check_with_numeric_limits() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new(u32::MAX, u32::MAX, 1_000_000.0, Some(now))
            .expect("bucket should be well formed");

        assert!(bucket.acquire(u32::MAX, now));
        assert!(!bucket.acquire(1, now));

        let now = now + Duration::days(100);
        assert!(bucket.acquire(u32::MAX, now));
        assert!(!bucket.acquire(1, now));
    }

    #[test]
    /// Validate if `TokenBucket` meets the requirement of being able to refresh tokens successfully
    /// when both the refill rate and the elapsed time are very low.
    fn validate_guaranteed_resolution() {
        let mut now = Instant::now();
        let mut bucket =
            TokenBucket::new(0, 10, 0.001, Some(now)).expect("bucket should be well formed");
        // Up to 999s: no new token added.
        for _ in 0..99_900 {
            now += Duration::milliseconds(10);
            assert!(!bucket.acquire(1, now));
        }
        // From 999s to 1001s: the new token should get added.
        let mut tokens_added = 0;
        for _ in 99_900..100_100 {
            now += Duration::milliseconds(10);
            if bucket.acquire(1, now) {
                tokens_added += 1;
            }
        }
        assert_eq!(tokens_added, 1);
    }
}
