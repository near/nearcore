use crate::concurrency::ctx;
use near_async::time;

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
