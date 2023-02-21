use crate::concurrency::ctx;
use crate::time;

pub fn now() -> time::Instant {
    ctx::local().0.clock.now()
}

pub fn now_utc() -> time::Utc {
    ctx::local().0.clock.now_utc()
}

pub async fn sleep(d: time::Duration) -> ctx::OrCanceled<()> {
    let ctx = ctx::local();
    ctx.wait(ctx.0.clock.sleep(d)).await
}

pub async fn sleep_until(t: time::Instant) -> ctx::OrCanceled<()> {
    let ctx = ctx::local();
    ctx.wait(ctx.0.clock.sleep_until(t)).await
}

/// Interval equivalent to tokio::time::Interval with
/// MissedTickBehavior::Skip.
pub struct Interval {
    next: time::Instant,
    period: time::Duration,
}

impl Interval {
    pub fn new(next: time::Instant, period: time::Duration) -> Self {
        Self { next, period }
    }

    /// Cancel-safe.
    pub async fn tick(&mut self) -> OrCanceled<()> {
        sleep_until(self.next).await?;
        let now = now();
        // Implementation of `tokio::time::MissedTickBehavior::Skip`.
        // Please refer to https://docs.rs/tokio/latest/tokio/time/enum.MissedTickBehavior.html#
        // for details. In essence, if more than `period` of time passes between consecutive
        // calls to tick, then the second tick completes immediately and the next one will be
        // aligned to the original schedule.
        self.next = now + self.period
            - Duration::nanoseconds(
                ((now - self.next).whole_nanoseconds() % self.period.whole_nanoseconds())
                    .try_into()
                    // This operation is practically guaranteed not to
                    // fail, as in order for it to fail, `period` would
                    // have to be longer than `now - timeout`, and both
                    // would have to be longer than 584 years.
                    //
                    // If it did fail, there's not a good way to pass
                    // the error along to the user, so we just panic.
                    .expect("too much time has elapsed since the interval was supposed to tick"),
            );
    }
}
