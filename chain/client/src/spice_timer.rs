use crate::metrics::{SPICE_BLOCK_PRODUCTION_DELAY_MS, SPICE_CERTIFICATION_LAG};
use near_async::time::{Clock, Duration, Instant};
use near_chain::spice_core::get_last_certified_block_header;
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_store::adapter::chain_store::ChainStoreAdapter;

/// SpiceTimer controls block production timing based on certification lag.
///
/// Implements a linear backoff:
/// - If certification is up-to-date, produce blocks at min_block_time
/// - As certification lags behind, increase the delay linearly
/// - Cap the maximum delay at max_block_time
///
/// `delay = min(max_block_time, min_block_time + block_time_step *(certification_lag - 2))`
pub struct SpiceTimer {
    clock: Clock,
    /// Minimum time to wait before producing a block when certification is up-to-date
    min_block_time: Duration,
    /// Incremental delay added per block of certification lag
    block_time_step: Duration,
    /// Maximum time to wait before producing a block regardless of certification lag
    max_block_time: Duration,
    /// Cached certification head height to avoid frequent recalculation
    /// Tuple contains: (head_hash, certification_height, cached_time)
    cached_certification_height: Option<(CryptoHash, BlockHeight, Instant)>,
}

/// Time-to-live for the values in the certification height cache. This defines the reaction time
/// to the decrease of the certification lag, and so to the recovery of the block production rate
/// after execution/certification starts catching up. Should be balanced between responsiveness and
/// load we put on the storage layer to fetch the certified block header. The value of 100ms means
/// under normal conditions we currently serve 90% of the requests from the cache, without causing
/// much reaction/recovery delay.
const CACHE_TTL: Duration = Duration::milliseconds(100);

impl SpiceTimer {
    pub fn new(
        clock: Clock,
        min_block_time: Duration,
        max_block_time: Duration,
        block_time_step: Duration,
    ) -> Self {
        assert!(min_block_time <= max_block_time, "min_block_time must be <= max_block_time");
        assert!(block_time_step >= Duration::ZERO, "block_time_step must be non-negative");

        Self {
            clock,
            min_block_time,
            block_time_step,
            max_block_time,
            cached_certification_height: None,
        }
    }

    /// Returns the current certification head height.
    /// Uses caching to avoid expensive lookups on every call (we currently check for block readiness every 10ms)
    fn get_certification_head_height(
        &mut self,
        chain_store: &ChainStoreAdapter,
        head_hash: &CryptoHash,
    ) -> Result<BlockHeight, Error> {
        let now = self.clock.now();

        if let Some((cached_head_hash, cached_height, cached_time)) =
            self.cached_certification_height
        {
            if cached_head_hash == *head_hash && now.duration_since(cached_time) < CACHE_TTL {
                return Ok(cached_height);
            }
        }

        // Cache miss or expired, recalculate
        let cert_header = get_last_certified_block_header(chain_store, head_hash)?;
        let cert_height = cert_header.height();

        self.cached_certification_height = Some((*head_hash, cert_height, now));

        Ok(cert_height)
    }

    /// Calculates the required delay before producing a block based on certification lag.
    fn calculate_production_delay_ns(&self, certification_lag: u64) -> u128 {
        let certification_lag = u32::try_from(certification_lag).unwrap_or(u32::MAX);
        let total_delay =
            self.min_block_time + self.block_time_step * certification_lag.saturating_sub(2_u32);

        std::cmp::min(total_delay, self.max_block_time).unsigned_abs().as_nanos()
    }

    /// Determines if a block is ready to be produced at the target height.
    ///
    /// # Returns
    /// `Ok(true)` if it is time to produce the block now
    /// `Ok(false)` we should wait longer
    /// `Err(_)` if there was an error checking certification
    pub fn ready_to_produce_block(
        &mut self,
        target_height: BlockHeight,
        chain_store: &ChainStoreAdapter,
        head_hash: &CryptoHash,
        last_block_timestamp_ns: u64,
    ) -> Result<bool, Error> {
        let now = u128::try_from(self.clock.now_utc().unix_timestamp_nanos()).unwrap_or(0);
        let time_since_last_block_ns = now.saturating_sub(last_block_timestamp_ns as u128);

        let certification_height = self.get_certification_head_height(chain_store, head_hash)?;

        // Target height should always be > certification height in normal operation
        let certification_lag = target_height.saturating_sub(certification_height);

        let required_delay_ns = self.calculate_production_delay_ns(certification_lag);

        SPICE_CERTIFICATION_LAG.set(certification_lag as i64);
        SPICE_BLOCK_PRODUCTION_DELAY_MS.set((required_delay_ns / (1000 * 1000)) as i64);

        let ready = required_delay_ns <= time_since_last_block_ns;

        tracing::debug!(
            target: "client",
            target_height,
            certification_height,
            certification_lag,
            time_since_last_block_ms = time_since_last_block_ns/(1000*1000),
            required_delay_ms = required_delay_ns/(1000*1000),
            ready,
            "SpiceTimer::ready_to_produce_block"
        );

        Ok(ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_async::time::{FakeClock, Utc};

    #[test]
    fn test_calculate_production_delay_no_lag() {
        let clock = FakeClock::new(Utc::UNIX_EPOCH);
        let timer = SpiceTimer::new(
            clock.clock(),
            Duration::milliseconds(600),
            Duration::milliseconds(2000),
            Duration::milliseconds(100),
        );

        let delay = timer.calculate_production_delay_ns(0);
        assert_eq!(delay, Duration::milliseconds(600).unsigned_abs().as_nanos());

        let delay = timer.calculate_production_delay_ns(2);
        assert_eq!(delay, Duration::milliseconds(600).unsigned_abs().as_nanos());
    }

    #[test]
    fn test_calculate_production_delay_with_lag() {
        let clock = FakeClock::new(Utc::UNIX_EPOCH);
        let timer = SpiceTimer::new(
            clock.clock(),
            Duration::milliseconds(600),
            Duration::milliseconds(2000),
            Duration::milliseconds(100),
        );

        // Lag of 5 blocks: 600 + 100*3 = 900ms
        let delay = timer.calculate_production_delay_ns(5);
        assert_eq!(delay, Duration::milliseconds(900).unsigned_abs().as_nanos());

        // Lag of 10 blocks: 600 + 100*8 = 1400ms
        let delay = timer.calculate_production_delay_ns(10);
        assert_eq!(delay, Duration::milliseconds(1400).unsigned_abs().as_nanos());
    }

    #[test]
    fn test_calculate_production_delay_capped() {
        let clock = FakeClock::new(Utc::UNIX_EPOCH);
        let timer = SpiceTimer::new(
            clock.clock(),
            Duration::milliseconds(600),
            Duration::milliseconds(2000),
            Duration::milliseconds(100),
        );

        // Lag of 22 blocks: 600 + 100*20 = 2600ms, but capped at 2000ms
        let delay = timer.calculate_production_delay_ns(22);
        assert_eq!(delay, Duration::milliseconds(2000).unsigned_abs().as_nanos());

        // Lag of 102 blocks: should still be capped at 2000ms
        let delay = timer.calculate_production_delay_ns(102);
        assert_eq!(delay, Duration::milliseconds(2000).unsigned_abs().as_nanos());
    }

    #[test]
    #[should_panic(expected = "min_block_time must be <= max_block_time")]
    fn test_invalid_config_panics() {
        let clock = FakeClock::new(Utc::UNIX_EPOCH);
        SpiceTimer::new(
            clock.clock(),
            Duration::milliseconds(2000),
            Duration::milliseconds(600), // max < min, should panic
            Duration::milliseconds(100),
        );
    }
}
