use std::sync::Arc;
use tokio::time;

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

/// CoarseLimiter is a Semaphore with periodically added permits in batches.
/// It allows to rate limit any async-based operations, which acquire permits in batches.
/// It should be used when adding permits one by one would be too inefficient.
#[derive(Clone)]
pub struct CoarseLimiter(Arc<tokio::sync::Semaphore>);

impl CoarseLimiter {
    /// Creates a new CoarseLimiter with the given rate limit,
    /// with permits added every `refresh_period`. The refreshing
    /// task is spawned within provided context.
    // TODO(gprusak): this can be implemented with better precision,
    // as follows: replace Semaphore with a Mutex
    pub fn spawn<A: actix::Actor>(
        ctx: &mut actix::Context<A>,
        limit: Limit,
        refresh_period: time::Duration,
    ) -> BytesLimiter {
        assert!(refresh_period > 0);
        let mut refresh_period = tokio::time::interval(refresh_period.try_into().unwrap());
        refresh_period.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let sem = Arc::new(tokio::sync::Semaphore::new(l.burst));
        ctx.spawn(wrap_future(async {
            let mut t0 = clock.now();
            loop {
                refresh_period.tick().await;
                let got = sem.available_permits();
                let t1 = clock.now();
                // Casting to usize is saturating and rounds towards 0.
                let new: usize = ((t1 - t0).as_secs_f64() * limit.qps) as usize;
                // Progress time only by full units.
                t0 += time::Duration::seconds_64(new as f64 / limit.qps);
                if got < limit.burst {
                    sem.add_permits(cmp::min(limit.burst as usize - got, new));
                }
            }
        }));
        BytesLimiter(sem)
    }

    // See semantics of https://pkg.go.dev/golang.org/x/time/rate
    pub async fn acquire(&self) -> anyhow::Result<()> {
        let mut rl = ctx.wrap(self.0.lock()).await?;
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
        ctx.wait_until(rl.instant(rl.ticks_processed + 1)).await?;
        rl.ticks_processed += 1;
        Ok(())
    }
}
