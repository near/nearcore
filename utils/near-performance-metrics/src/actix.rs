use log::info;
use std::time::{Duration, Instant};

use crate::stats::{MyFuture, REF_COUNTER, SLOW_CALL_THRESHOLD, STATS, TID};

#[allow(unused_variables)]
pub fn spawn<F>(class_name: &'static str, file: &'static str, line: u32, f: F)
where
    F: futures::Future<Output = ()> + 'static,
{
    #[cfg(not(feature = "performance_stats"))]
    {
        actix_rt::spawn(f);
    }

    #[cfg(feature = "performance_stats")]
    {
        *REF_COUNTER.lock().unwrap().entry((file, line)).or_insert_with(|| 0) += 1;
        actix_rt::spawn(MyFuture { f, class_name, file, line });
    }
}

#[allow(unused_variables)]
pub fn run_later<F, A, B>(
    ctx: &mut B,
    file: &'static str,
    line: u32,
    dur: Duration,
    f: F,
) -> actix::SpawnHandle
where
    B: actix::AsyncContext<A>,
    A: actix::Actor<Context = B>,
    F: FnOnce(&mut A, &mut A::Context) + 'static,
{
    #[cfg(not(feature = "performance_stats"))]
    {
        ctx.run_later(dur, f)
    }

    #[cfg(feature = "performance_stats")]
    {
        *REF_COUNTER.lock().unwrap().entry((file, line)).or_insert_with(|| 0) += 1;
        let f2 = move |a: &mut A, b: &mut A::Context| {
            let now = Instant::now();
            f(a, b);

            let took = now.elapsed();
            STATS.lock().unwrap().log("run_later", file, line, took);
            if took > SLOW_CALL_THRESHOLD {
                info!(
                    "Slow function call {}:{} {}:{} took: {}ms",
                    "run_later",
                    TID.with(|x| *x.borrow()),
                    file,
                    line,
                    took.as_millis()
                );
            }
            *REF_COUNTER.lock().unwrap().entry((file, line)).or_insert_with(|| 0) -= 1;
        };
        ctx.run_later(dur, f2)
    }
}
