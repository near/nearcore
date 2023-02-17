use near_network::concurrency::ctx::{OrCanceled,Ctx};
use near_network::time;
use std::sync::Arc;

struct RateLimiter_ {
    interval: time::Duration,
    burst: u64,
    tokens: u64,
    ticks_processed: u64,
    start: time::Instant,
}

impl RateLimiter_ {
    fn ticks(&self, t: time::Instant) -> u64 {
        return ((t - self.start).as_seconds_f64() / self.interval.as_seconds_f64()) as u64;
    }
    fn instant(&self, ticks: u64) -> time::Instant {
        return self.start + self.interval * (ticks as f64);
    }
}

// RateLimiter is a Semaphore with periodically added permits.
// It allows to rate limit any async-based operations.
// It is parametrized by:
// - interval - the amount of time after which a new permit is added.
// - burst - the maximal number of permits in the semaphore.
pub struct RateLimiter(Arc<tokio::sync::Mutex<RateLimiter_>>);

impl RateLimiter {
    pub fn new(interval: time::Duration, burst: u64) -> RateLimiter {
        if interval.is_zero() {
            panic!("interval has to be non-zero");
        }
        return RateLimiter(Arc::new(tokio::sync::Mutex::new(RateLimiter_ {
            interval,
            burst,
            tokens: burst,
            start: time::Instant::now(),
            ticks_processed: 0,
        })));
    }

    // See semantics of https://pkg.go.dev/golang.org/x/time/rate
    pub async fn allow(&self) -> OrCanceled<()> {
        let mut rl = Ctx::wait(self.0.lock()).await?;
        let ticks_now = rl.ticks(time::Instant::now());
        rl.tokens = std::cmp::min(
            rl.burst,
            rl.tokens.wrapping_add(ticks_now.wrapping_sub(rl.ticks_processed)),
        );
        rl.ticks_processed = ticks_now;
        if rl.tokens > 0 {
            rl.tokens -= 1;
            return Ok(());
        }
        Ctx::sleep_until(rl.instant(rl.ticks_processed + 1)).await?;
        rl.ticks_processed += 1;
        Ok(())
    }
}
