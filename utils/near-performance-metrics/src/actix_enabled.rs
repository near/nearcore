use log::warn;
use std::panic::Location;
use std::time::Duration;
use std::time::Instant;

use crate::stats_enabled::{get_thread_stats_logger, MyFuture, REF_COUNTER, SLOW_CALL_THRESHOLD};

use near_rust_allocator_proxy::allocator::get_tid;

#[track_caller]
pub fn spawn<F>(class_name: &'static str, f: F)
where
    F: futures::Future<Output = ()> + 'static,
{
    let loc = Location::caller();
    *REF_COUNTER.lock().unwrap().entry((loc.file(), loc.line())).or_insert_with(|| 0) += 1;
    actix::spawn(MyFuture { f, class_name, file: loc.file(), line: loc.line() });
}

#[track_caller]
pub fn run_later<F, A, B>(ctx: &mut B, dur: Duration, f: F) -> actix::SpawnHandle
where
    B: actix::AsyncContext<A>,
    A: actix::Actor<Context = B>,
    F: FnOnce(&mut A, &mut A::Context) + 'static,
{
    let loc = Location::caller();
    *REF_COUNTER.lock().unwrap().entry((loc.file(), loc.line())).or_insert_with(|| 0) += 1;

    let f2 = move |a: &mut A, b: &mut A::Context| {
        let stat = get_thread_stats_logger();
        let now = Instant::now();
        stat.lock().unwrap().pre_log(now);

        f(a, b);

        let ended = Instant::now();
        let took = ended - now;
        stat.lock().unwrap().log("run_later", loc.file(), loc.line(), took, ended, "");
        if took > SLOW_CALL_THRESHOLD {
            warn!(
                "Slow function call {}:{} {}:{} took: {}ms",
                "run_later",
                get_tid(),
                loc.file(),
                loc.line(),
                took.as_millis()
            );
        }
        *REF_COUNTER.lock().unwrap().entry((loc.file(), loc.line())).or_insert_with(|| 0) -= 1;
    };
    ctx.run_later(dur, f2)
}
