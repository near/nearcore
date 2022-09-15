use std::cmp;
use crate::time;

/// Config of a rate limiter algorithm, which behaves like a semaphore
/// - with maximal capacity `burst`
/// - with a new ticket added automatically every 1/qps seconds (qps stands for "queries per
///   second")
/// In case of large load, semaphore will be empty most of the time,
/// letting through requests at frequency `qps`.
/// In case a number of requests come after a period of inactivity, semaphore will immediately
/// let through up to `burst` requests, before going into the previous mode.
#[derive(Copy, Clone)]
pub struct Limit {
    pub burst: u64,
    pub qps: f64,
}

impl Limit {
    // TODO(gprusak): consider having a constructor for RateLimit which enforces validation
    // and getters for fields, so that they cannot be modified after construction.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.qps <= 0. {
            anyhow::bail!("qps has to be >0");
        }
        if self.burst <= 0 {
            anyhow::bail!("burst has to be >0");
        }
        Ok(())
    }
}

struct LimiterInner {
    permits: u64,
    last_refresh: time::Instant,
}

/// Limiter is a Semaphore with periodically added permits.
/// It allows to rate limit any async-based operations.
pub(crate) struct Limiter {
    state: tokio::sync::Mutex<LimiterInner>,
    limit: Limit,
}

impl Limiter {
    pub fn new(clock: &time::Clock, limit: Limit) -> Limiter {
        return Limiter{ 
            state: tokio::sync::Mutex::new(LimiterInner{
                last_refresh: clock.now(),
                permits: limit.burst,
            }),
            limit,
        };
    }

    /// Acquire waits until `n` permits are available, then consumes them.
    /// It is possible to acquire more than `burst` permits, but it will always require waiting.
    /// It is cancel-safe and has FIFO semantics: subsequent call have to wait until the previous
    /// call acquires its permits. In case a call is cancelled, no permits are consumed.
    pub async fn acquire(&self, clock: &time::Clock, n:u64) {
        let mut state = self.state.lock().await;
        if n <= state.permits {
            let now = clock.now();
            let new = ((now-state.last_refresh).as_seconds_f64()*self.limit.qps) as u64;
            state.last_refresh += time::Duration::seconds_f64((new as f64)/self.limit.qps);
            state.permits = cmp::min(self.limit.burst, (state.permits-n).saturating_add(new));
            if state.last_refresh>now {
                state.last_refresh = now;
            }
        } else {
            let t = state.last_refresh + time::Duration::seconds_f64((n-state.permits) as f64/self.limit.qps);
            clock.sleep_until(t).await;
            state.last_refresh = t;
            state.permits = 0;
        }
    }
}
