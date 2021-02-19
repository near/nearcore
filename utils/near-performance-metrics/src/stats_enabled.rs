use futures;
use futures::task::Context;
use log::{info, warn};
use near_rust_allocator_proxy::allocator::{
    current_thread_memory_usage, current_thread_peak_memory_usage, get_tid, reset_memory_usage_max,
    thread_memory_count, thread_memory_usage,
};
use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use strum::AsStaticRef;

const MEBIBYTE: usize = 1024 * 1024;
const MEMORY_LIMIT: usize = 512 * MEBIBYTE;
const MIN_MEM_USAGE_REPORT_SIZE: usize = 100 * MEBIBYTE;
pub static NTHREADS: AtomicUsize = AtomicUsize::new(0);
pub(crate) const SLOW_CALL_THRESHOLD: Duration = Duration::from_millis(500);
const MIN_OCCUPANCY_RATIO_THRESHOLD: f64 = 0.002;

pub(crate) static STATS: Lazy<Arc<Mutex<Stats>>> = Lazy::new(|| Arc::new(Mutex::new(Stats::new())));
pub(crate) static REF_COUNTER: Lazy<Mutex<HashMap<(&'static str, u32), u128>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

thread_local! {
    pub static INITIALIZED: RefCell<u32> = RefCell::new(0);
    pub(crate) static LOCAL_STATS: Lazy<Arc<Mutex<ThreadStats>>> = Lazy::new(|| Arc::new(Mutex::new(ThreadStats::new())));
}

#[derive(Default)]
struct Entry {
    cnt: u128,
    time: Duration,
    max_time: Duration,
}

pub struct ThreadStats {
    stat: HashMap<(&'static str, u32, &'static str), Entry>,
    cnt: u128,
    time: Duration,
    classes: HashSet<&'static str>,
    in_progress_since: Option<Instant>,
    last_check: Instant,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            stat: HashMap::new(),
            cnt: 0,
            time: Duration::default(),
            classes: HashSet::new(),
            in_progress_since: None,
            last_check: Instant::now(),
        }
    }

    pub fn pre_log(&mut self, now: Instant) {
        self.in_progress_since = Some(now);
    }

    pub fn log(
        &mut self,
        class_name: &'static str,
        msg: &'static str,
        line: u32,
        took: Duration,
        now: Instant,
        msg_text: &'static str,
    ) {
        self.in_progress_since = None;

        let took_since_last_check = min(took, max(self.last_check, now) - self.last_check);

        let entry = self.stat.entry((msg, line, msg_text)).or_insert_with(|| Entry {
            cnt: 0,
            time: Duration::default(),
            max_time: Duration::default(),
        });
        entry.cnt += 1;
        entry.time += took_since_last_check;
        entry.max_time = max(took, entry.max_time);

        self.cnt += 1;
        self.time += took_since_last_check;
        self.classes.insert(class_name);
    }

    fn print_stats_and_clear(
        &mut self,
        tid: usize,
        sleep_time: Duration,
        now: Instant,
    ) -> (f64, f64, usize, usize) {
        let mut ratio = self.time.as_nanos() as f64;
        if let Some(in_progress_since) = self.in_progress_since {
            let from = max(in_progress_since, self.last_check);
            ratio += (max(now, from) - from).as_nanos() as f64;
        }
        ratio /= sleep_time.as_nanos() as f64;

        let tmu = thread_memory_usage(tid);
        let show_stats = ratio >= MIN_OCCUPANCY_RATIO_THRESHOLD || tmu >= MIN_MEM_USAGE_REPORT_SIZE;

        if show_stats {
            let class_name = format!("{:?}", self.classes);
            warn!(
                "    Thread:{} ratio: {:.3} {}:{} memory: {}MiB({})",
                tid,
                ratio,
                class_name,
                get_tid(),
                tmu / MEBIBYTE,
                thread_memory_count(tid),
            );
            let mut stat: Vec<_> = self.stat.iter().collect();
            stat.sort_by(|x, y| (*x).0.cmp(&(*y).0));

            for entry in stat {
                warn!(
                    "        func {} {}:{}:{} cnt: {} total: {}ms max: {}ms",
                    get_tid(),
                    (entry.0).0,
                    (entry.0).1,
                    (entry.0).2,
                    entry.1.cnt,
                    ((entry.1.time.as_millis()) as f64),
                    ((entry.1.max_time.as_millis()) as f64)
                );
            }
        }
        self.last_check = now;
        self.clear();

        if show_stats {
            (ratio, 0.0, 0, 0)
        } else {
            (ratio, ratio, thread_memory_usage(tid), thread_memory_count(tid))
        }
    }

    fn clear(&mut self) {
        self.time = Duration::default();
        self.cnt = 0;
        self.stat.clear();
    }
}

pub(crate) struct Stats {
    stats: HashMap<usize, Arc<Mutex<ThreadStats>>>,
}

pub(crate) fn get_entry() -> Arc<Mutex<ThreadStats>> {
    INITIALIZED.with(|x| {
        if *x.borrow() == 0 {
            *x.borrow_mut() = 1;
            STATS.lock().unwrap().add_entry();
        }
    });

    LOCAL_STATS.with(|x| (*x).clone())
}

#[cfg(target_os = "linux")]
fn get_rocksdb_memory_usage() -> usize {
    // hack to get memory usage stats for rocksdb
    unsafe { libc::malloc(usize::MAX) as usize }
}

#[cfg(not(target_os = "linux"))]
fn get_rocksdb_memory_usage() -> usize {
    0
}

impl Stats {
    fn new() -> Self {
        Self { stats: HashMap::new() }
    }

    pub(crate) fn add_entry(&mut self) {
        let tid = get_tid();
        self.stats.entry(tid).or_insert_with(|| LOCAL_STATS.with(|x| (*x).clone()));
    }

    fn print_stats(&mut self, sleep_time: Duration) {
        info!(
            "Performance stats {} threads (min ratio = {})",
            self.stats.len(),
            MIN_OCCUPANCY_RATIO_THRESHOLD
        );
        let mut s: Vec<_> = self.stats.iter().collect();
        s.sort_by(|x, y| (*x).0.cmp(&(*y).0));

        let mut ratio = 0.0;
        let mut other_ratio = 0.0;
        let mut other_memory_size = 0;
        let mut other_memory_count = 0;
        let now = Instant::now();
        for entry in s {
            let (tmp_ratio, tmp_other_ratio, tmp_other_memory_size, tmp_other_memory_count) =
                entry.1.lock().unwrap().print_stats_and_clear(*entry.0, sleep_time, now);
            ratio += tmp_ratio;
            other_ratio += tmp_other_ratio;
            other_memory_size += tmp_other_memory_size;
            other_memory_count += tmp_other_memory_count;
        }
        info!(
            "    Other threads ratio {:.3} memory: {}MiB({})",
            other_ratio,
            other_memory_size / MEBIBYTE,
            other_memory_count
        );
        let rocksdb_memory_usage = get_rocksdb_memory_usage();
        if rocksdb_memory_usage > 0 {
            info!("    RocksDB memory usage: {}MiB", rocksdb_memory_usage / MEBIBYTE);
        }
        info!("Total ratio = {:.3}", ratio);
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
    measure_performance_internal(class_name, msg, f, None)
}

pub fn measure_performance_with_debug<F, Message, Result>(
    class_name: &'static str,
    msg: Message,
    f: F,
) -> Result
where
    F: FnOnce(Message) -> Result,
    Message: AsStaticRef<str>,
{
    let msg_text: &'static str = msg.as_static();
    measure_performance_internal(class_name, msg, f, Some(msg_text))
}

pub fn measure_performance_internal<F, Message, Result>(
    class_name: &'static str,
    msg: Message,
    f: F,
    msg_text: Option<&'static str>,
) -> Result
where
    F: FnOnce(Message) -> Result,
{
    let stat = get_entry();

    reset_memory_usage_max();
    let initial_memory_usage = current_thread_memory_usage();
    let now = Instant::now();
    stat.lock().unwrap().pre_log(now);
    let result = f(msg);

    let took = now.elapsed();

    let peak_memory = current_thread_peak_memory_usage() - initial_memory_usage;

    if peak_memory >= MEMORY_LIMIT {
        warn!(
            "Function exceeded memory limit {}:{} {:?} took: {}ms peak_memory: {}MiB",
            class_name,
            get_tid(),
            std::any::type_name::<Message>(),
            took.as_millis(),
            peak_memory / MEBIBYTE,
        );
    }

    if took >= SLOW_CALL_THRESHOLD {
        let text_field = msg_text.map(|x| format!(" msg: {}", x)).unwrap_or(format!(""));
        if peak_memory > 0 {
            warn!(
                "Function exceeded time limit {}:{} {:?} took: {}ms {} peak_memory: {}MiB",
                class_name,
                get_tid(),
                std::any::type_name::<Message>(),
                took.as_millis(),
                text_field,
                peak_memory / MEBIBYTE,
            );
        } else {
            warn!(
                "Function exceeded time limit {}:{} {:?} took: {}ms {}",
                class_name,
                get_tid(),
                std::any::type_name::<Message>(),
                took.as_millis(),
                text_field,
            );
        }
    }
    let ended = Instant::now();
    let took = ended - now;
    stat.lock().unwrap().log(
        class_name,
        std::any::type_name::<Message>(),
        0,
        took,
        ended,
        msg_text.unwrap_or_default(),
    );
    result
}

pub struct MyFuture<F>
where
    F: futures::Future<Output = ()> + 'static,
{
    pub f: F,
    pub class_name: &'static str,
    pub file: &'static str,
    pub line: u32,
}

impl<F> futures::Future for MyFuture<F>
where
    F: futures::Future<Output = ()> + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let stat = get_entry();
        let now = Instant::now();
        stat.lock().unwrap().pre_log(now);

        let res = unsafe { Pin::new_unchecked(&mut this.f) }.poll(cx);
        let ended = Instant::now();
        let took = ended - now;
        stat.lock().unwrap().log(this.class_name, this.file, this.line, took, ended, "");

        if took > SLOW_CALL_THRESHOLD {
            warn!(
                "Function exceeded time limit {}:{} {}:{} took: {}ms",
                this.class_name,
                get_tid(),
                this.file,
                this.line,
                took.as_millis()
            );
        }
        match res {
            Poll::Ready(x) => {
                *REF_COUNTER.lock().unwrap().entry((this.file, this.line)).or_insert_with(|| 0) -=
                    1;
                Poll::Ready(x)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub fn print_performance_stats(sleep_time: Duration) {
    STATS.lock().unwrap().print_stats(sleep_time);
    info!("Futures waiting for completion");
    for entry in REF_COUNTER.lock().unwrap().iter() {
        if *entry.1 > 0 {
            info!("    future {}:{} {}", (entry.0).0, (entry.0).1, entry.1);
        }
    }
}
