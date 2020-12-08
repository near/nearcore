use log::info;
use std::cell::RefCell;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::task::Poll;
use std::time::{Duration, Instant};

use actix;
use futures;
use futures::task::Context;
use std::mem;
use std::pin::Pin;

pub static NTHREADS: AtomicUsize = AtomicUsize::new(0);
const SLOW_CALL_THRESHOLD: Duration = Duration::from_millis(500);
const MIN_OCCUPANCY_RATIO_THRESHOLD: f64 = 0.00;
#[cfg(feature = "performance_stats")]
const MESSAGE_LIMIT: usize = 250;

lazy_static! {
    static ref STATS: RwLock<Stats> = RwLock::new(Stats::new());
    static ref REF_COUNTER: RwLock<HashMap<(&'static str, u32), u128>> =
        RwLock::new(HashMap::new());
}

thread_local! {
    static TID: RefCell<usize> = RefCell::new(0);
}

#[derive(Default)]
struct Entry {
    cnt: u128,
    time: Duration,
    max_time: Duration,
}

struct ThreadStats {
    stat: HashMap<(&'static str, u32), Entry>,
    cnt: u128,
    time: Duration,
    classes: HashSet<&'static str>,
}

impl ThreadStats {
    fn new() -> Self {
        Self { stat: HashMap::new(), cnt: 0, time: Duration::default(), classes: HashSet::new() }
    }

    fn log(&mut self, class_name: &'static str, msg: &'static str, line: u32, took: Duration) {
        let entry = self.stat.entry((msg, line)).or_insert_with(|| Entry {
            cnt: 0,
            time: Duration::default(),
            max_time: Duration::default(),
        });
        entry.cnt += 1;
        entry.time += took;
        entry.max_time = max(took, entry.max_time);

        self.cnt += 1;
        self.time += took;
        self.classes.insert(class_name);
    }

    fn print_stats(&self, tid: usize, sleep_time: Duration) {
        let ratio = (self.time.as_nanos() as f64) / (sleep_time.as_nanos() as f64);

        let class_name = format!("{:?}", self.classes);

        if ratio > MIN_OCCUPANCY_RATIO_THRESHOLD {
            info!(
                "    Thread:{} {}:{} ratio: {:.4}",
                tid,
                class_name,
                TID.with(|x| *x.borrow()),
                ratio
            );
            for entry in self.stat.iter() {
                info!(
                    "        func {} {}:{} cnt: {} total: {}ms max: {}ms",
                    TID.with(|x| *x.borrow()),
                    (entry.0).0,
                    (entry.0).1,
                    entry.1.cnt,
                    ((entry.1.time.as_millis()) as f64),
                    ((entry.1.max_time.as_millis()) as f64)
                );
            }
        }
    }
}

struct Stats {
    stats: HashMap<usize, ThreadStats>,
}

impl Stats {
    fn new() -> Self {
        Self { stats: HashMap::new() }
    }

    fn log(&mut self, class_name: &'static str, msg: &'static str, line: u32, took: Duration) {
        let tid = TID.with(|x| {
            if *x.borrow_mut() == 0 {
                *x.borrow_mut() = NTHREADS.fetch_add(1, Ordering::SeqCst);
                self.stats.insert(*x.borrow_mut(), ThreadStats::new());
            }
            *x.borrow_mut()
        });
        if let Some(val) = self.stats.get_mut(&tid) {
            val.log(class_name, msg, line, took)
        }
    }

    fn print_stats(&self, sleep_time: Duration) {
        info!("Performance stats {} threads", self.stats.len());
        for entry in self.stats.iter() {
            entry.1.print_stats(*entry.0, sleep_time);
        }
    }
}

pub fn measure_performance<F, Message, Result>(
    class_name: &'static str,
    msg: Message,
    f: F,
) -> Result
where
    F: FnOnce(Message) -> Result,
{
    let now = Instant::now();
    let result = f(msg);

    let took = now.elapsed();
    if took > SLOW_CALL_THRESHOLD {
        info!(
            "SLOW {}:{} {:?} took: {}ms",
            class_name,
            TID.with(|x| *x.borrow()),
            std::any::type_name::<Message>(),
            took.as_millis(),
        );
    }

    STATS.write().unwrap().log(class_name, std::any::type_name::<Message>(), 0, took);
    result
}

pub fn measure_performance2<F, Result>(
    class_name: &'static str,
    msg_name: &'static str,
    f: F,
) -> Result
where
    F: FnOnce() -> Result,
{
    let now = Instant::now();
    let result = f();

    let took = now.elapsed();
    if took > SLOW_CALL_THRESHOLD {
        info!(
            "SLOW {}:{} {:?} took: {}ms",
            class_name,
            TID.with(|x| *x.borrow()),
            msg_name,
            took.as_millis(),
        );
    }

    STATS.write().unwrap().log(class_name, msg_name, 0, took);
    result
}

#[allow(unused_variables)]
pub fn performance_with_debug<F, Message, Result>(
    class_name: &'static str,
    msg: Message,
    f: F,
) -> Result
where
    F: FnOnce(Message) -> Result,
    Message: Debug,
{
    #[cfg(not(feature = "performance_stats"))]
    {
        f(msg)
    }

    #[cfg(feature = "performance_stats")]
    {
        let now = Instant::now();
        let msg_test = format!("{:?}", msg);
        let result = f(msg);

        let took = now.elapsed();
        if took > SLOW_CALL_THRESHOLD {
            let msg_length = msg_test.len();
            let mut msg_test = msg_test;
            if msg_length >= MESSAGE_LIMIT {
                msg_test = msg_test.as_str()[..MESSAGE_LIMIT].to_string();
            }
            info!(
                "SLOW {}:{} {:?} took: {}ms len: {} {}",
                class_name,
                TID.with(|x| *x.borrow()),
                std::any::type_name::<Message>(),
                took.as_millis(),
                msg_length,
                msg_test
            );
        }

        STATS.write().unwrap().log(class_name, std::any::type_name::<Message>(), 0, took);
        result
    }
}

pub struct MyFuture<F>
where
    F: futures::Future<Output = ()> + 'static,
{
    f: F,
    class_name: &'static str,
    file: &'static str,
    line: u32,
}

impl<F> futures::Future for MyFuture<F>
where
    F: futures::Future<Output = ()> + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let now = Instant::now();
        let res = unsafe { Pin::new_unchecked(&mut this.f) }.poll(cx);
        let took = now.elapsed();
        STATS.write().unwrap().log(this.class_name, this.file, this.line, took);

        if took > SLOW_CALL_THRESHOLD {
            info!(
                "SLOW {}:{} {}:{} took: {}ms",
                this.class_name,
                TID.with(|x| *x.borrow()),
                this.file,
                this.line,
                took.as_millis()
            );
        }
        match res {
            Poll::Ready(x) => {
                *REF_COUNTER.write().unwrap().entry((this.file, this.line)).or_insert_with(|| 0) -=
                    1;
                Poll::Ready(x)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

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
        *REF_COUNTER.write().unwrap().entry((file, line)).or_insert_with(|| 0) += 1;
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
        *REF_COUNTER.write().unwrap().entry((file, line)).or_insert_with(|| 0) += 1;
        let f2 = move |a: &mut A, b: &mut A::Context| {
            let now = Instant::now();
            f(a, b);

            let took = now.elapsed();
            STATS.write().unwrap().log("run_later", file, line, took);
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
            *REF_COUNTER.write().unwrap().entry((file, line)).or_insert_with(|| 0) -= 1;
        };
        ctx.run_later(dur, f2)
    }
}

pub fn print_performance_stats(sleep_time: Duration) {
    let stats: Stats = {
        let mut old_stats = STATS.write().unwrap();
        let mut new_stats = Stats::new();
        mem::swap(&mut *old_stats, &mut new_stats);
        new_stats
    };

    stats.print_stats(sleep_time);
    info!("NUM_REFERENCES_TYPES {}", REF_COUNTER.write().unwrap().len());
    for entry in REF_COUNTER.write().unwrap().iter() {
        if *entry.1 > 0 {
            info!("    NUM_REFS {}:{} {}", (entry.0).0, (entry.0).1, entry.1);
        }
    }
}
