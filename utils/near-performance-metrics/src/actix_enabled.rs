use log::warn;
use std::time::Duration;
use std::time::Instant;

use crate::stats_enabled::{MyFuture, REF_COUNTER, SLOW_CALL_THRESHOLD, STATS};

use near_rust_allocator_proxy::allocator::get_tid;

pub fn spawn<F>(class_name: &'static str, file: &'static str, line: u32, f: F)
where
    F: futures::Future<Output = ()> + 'static,
{
    *REF_COUNTER.lock().unwrap().entry((file, line)).or_insert_with(|| 0) += 1;
    actix_rt::spawn(MyFuture { f, class_name, file, line });
}

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
    *REF_COUNTER.lock().unwrap().entry((file, line)).or_insert_with(|| 0) += 1;
    let f2 = move |a: &mut A, b: &mut A::Context| {
        let now = Instant::now();
        f(a, b);

        let took = now.elapsed();
        STATS.lock().unwrap().log("run_later", file, line, took);
        if took > SLOW_CALL_THRESHOLD {
            warn!(
                "Slow function call {}:{} {}:{} took: {}ms",
                "run_later",
                get_tid(),
                file,
                line,
                took.as_millis()
            );
        }
        *REF_COUNTER.lock().unwrap().entry((file, line)).or_insert_with(|| 0) -= 1;
    };
    ctx.run_later(dur, f2)
}
