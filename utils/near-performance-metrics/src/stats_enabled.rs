use futures;
use futures::task::Context;
use log::{info, warn};
use near_rust_allocator_proxy::allocator::{
    current_thread_memory_usage, current_thread_peak_memory_usage, get_tid, reset_memory_usage_max,
    thread_memory_count, thread_memory_usage, total_memory_usage,
};
use once_cell::sync::Lazy;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytesize::ByteSize;
use strum::AsStaticRef;

static MEMORY_LIMIT: u64 = 512 * bytesize::MIB;
static MIN_MEM_USAGE_REPORT_SIZE: u64 = 100 * bytesize::MIB;

pub static NTHREADS: AtomicUsize = AtomicUsize::new(0);
pub(crate) const SLOW_CALL_THRESHOLD: Duration = Duration::from_millis(500);
const MIN_OCCUPANCY_RATIO_THRESHOLD: f64 = 0.02;

pub(crate) static STATS: Lazy<Arc<Mutex<Stats>>> = Lazy::new(|| Arc::new(Mutex::new(Stats::new())));
pub(crate) static REF_COUNTER: Lazy<Mutex<HashMap<(&'static str, u32), u128>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn get_thread_stats_logger() -> Arc<Mutex<ThreadStats>> {
    thread_local! {
        static LOCAL_STATS: Arc<Mutex<ThreadStats>> = {
            let res = Arc::new(Mutex::new(ThreadStats::new()));
            STATS.lock().unwrap().add_entry(&res);
            res
        }
    }

    LOCAL_STATS.with(Arc::clone)
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
    c_mem: ByteSize,

    // Write buffer stats
    write_buf_len: ByteSize,
    write_buf_capacity: ByteSize,
    write_buf_added: ByteSize,
    write_buf_drained: ByteSize,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            stat: Default::default(),
            cnt: Default::default(),
            time: Duration::default(),
            classes: Default::default(),
            in_progress_since: Default::default(),
            last_check: Instant::now(),
            c_mem: Default::default(),

            write_buf_len: Default::default(),
            write_buf_capacity: Default::default(),
            write_buf_added: Default::default(),
            write_buf_drained: Default::default(),
        }
    }

    pub fn log_add_write_buffer(&mut self, bytes: usize, buf_len: usize, buf_capacity: usize) {
        self.write_buf_added = self.write_buf_added + ByteSize::b(bytes as u64);
        self.write_buf_len = ByteSize::b(buf_len as u64);
        self.write_buf_capacity = ByteSize::b(buf_capacity as u64);
    }

    pub fn log_drain_write_buffer(&mut self, bytes: usize, buf_len: usize, buf_capacity: usize) {
        self.write_buf_drained = self.write_buf_drained + ByteSize::b(bytes as u64);
        self.write_buf_len = ByteSize::b(buf_len as u64);
        self.write_buf_capacity = ByteSize::b(buf_capacity as u64);
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
        self.c_mem = get_c_memory_usage_cur_thread();

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
    ) -> (f64, f64, ByteSize, usize) {
        let mut ratio = self.time.as_nanos() as f64;
        if let Some(in_progress_since) = self.in_progress_since {
            let from = max(in_progress_since, self.last_check);
            ratio += (max(now, from) - from).as_nanos() as f64;
        }
        ratio /= sleep_time.as_nanos() as f64;

        let tmu = ByteSize::b(thread_memory_usage(tid) as u64);
        let show_stats = ratio >= MIN_OCCUPANCY_RATIO_THRESHOLD
            || tmu + self.c_mem >= ByteSize::b(MIN_MEM_USAGE_REPORT_SIZE);

        if show_stats {
            let class_name = format!("{:?}", self.classes);
            warn!(
                "    Thread:{} ratio: {:.3} {}:{} Rust mem: {}({}) C mem: {}",
                tid,
                ratio,
                class_name,
                get_tid(),
                tmu,
                thread_memory_count(tid),
                self.c_mem,
            );
            if self.write_buf_added.as_u64() > 0
                || self.write_buf_capacity.as_u64() > 0
                || self.write_buf_added.as_u64() > 0
                || self.write_buf_drained.as_u64() > 0
            {
                info!(
                    "        Write_buffer len: {} cap: {} added: {} drained: {}",
                    self.write_buf_len,
                    self.write_buf_capacity,
                    self.write_buf_added,
                    self.write_buf_drained,
                );
            }
            self.write_buf_added = Default::default();
            self.write_buf_drained = Default::default();

            let mut stat: Vec<_> = self.stat.iter().collect();
            stat.sort_by(|x, y| (*x).0.cmp(&(*y).0));

            for entry in stat {
                warn!(
                    "        func {}:{}:{} cnt: {} total: {}ms max: {}ms",
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
        self.c_mem = ByteSize::b(0);
        self.clear();

        if show_stats {
            (ratio, 0.0, ByteSize::default(), 0)
        } else {
            (ratio, ratio, ByteSize::b(thread_memory_usage(tid) as u64), thread_memory_count(tid))
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

#[cfg(all(target_os = "linux", feature = "c_memory_stats"))]
fn get_c_memory_usage_cur_thread() -> ByteSize {
    // hack to get memory usage stats for c memory usage per thread
    // This feature will only work if near is started with environment
    // LD_PRELOAD=${PWD}/bins/near-c-allocator-proxy.so nearup ...
    // from https://github.com/near/near-memory-tracker/blob/master/near-dump-analyzer
    unsafe { ByteSize::b(libc::malloc(usize::MAX - 1) as u64) }
}

#[cfg(any(not(target_os = "linux"), not(feature = "c_memory_stats")))]
fn get_c_memory_usage_cur_thread() -> ByteSize {
    Default::default()
}

#[cfg(all(target_os = "linux", feature = "c_memory_stats"))]
fn get_c_memory_usage() -> ByteSize {
    // hack to get memory usage stats for c memory usage
    // This feature will only work if near is started with environment
    // LD_PRELOAD=${PWD}/bins/near-c-allocator-proxy.so nearup ...
    // from https://github.com/near/near-memory-tracker/blob/master/near-dump-analyzer
    unsafe { ByteSize::b(libc::malloc(usize::MAX) as u64) }
}

#[cfg(any(not(target_os = "linux"), not(feature = "c_memory_stats")))]
fn get_c_memory_usage() -> ByteSize {
    Default::default()
}

impl Stats {
    fn new() -> Self {
        Self { stats: HashMap::new() }
    }

    pub(crate) fn add_entry(&mut self, local_stats: &Arc<Mutex<ThreadStats>>) {
        let tid = get_tid();
        self.stats.entry(tid).or_insert_with(|| Arc::clone(local_stats));
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
        let mut other_memory_size = ByteSize::default();
        let mut other_memory_count = 0;
        let now = Instant::now();
        for entry in s {
            let (tmp_ratio, tmp_other_ratio, tmp_other_memory_size, tmp_other_memory_count) =
                entry.1.lock().unwrap().print_stats_and_clear(*entry.0, sleep_time, now);
            ratio += tmp_ratio;
            other_ratio += tmp_other_ratio;
            other_memory_size = other_memory_size + tmp_other_memory_size;
            other_memory_count += tmp_other_memory_count;
        }
        info!(
            "    Other threads ratio {:.3} memory: {}({})",
            other_ratio, other_memory_size, other_memory_count
        );
        info!("    Rust alloc total memory usage: {}", ByteSize::b(total_memory_usage() as u64));
        let c_memory_usage = get_c_memory_usage();
        if c_memory_usage > ByteSize::default() {
            info!("    C alloc total memory usage: {}", c_memory_usage);
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
    let stat = get_thread_stats_logger();

    reset_memory_usage_max();
    let initial_memory_usage = current_thread_memory_usage();
    let now = Instant::now();
    stat.lock().unwrap().pre_log(now);
    let result = f(msg);

    let took = now.elapsed();

    let peak_memory =
        ByteSize::b((current_thread_peak_memory_usage() - initial_memory_usage) as u64);

    if peak_memory >= ByteSize::b(MEMORY_LIMIT) {
        warn!(
            "Function exceeded memory limit {}:{} {:?} took: {}ms peak_memory: {}",
            class_name,
            get_tid(),
            std::any::type_name::<Message>(),
            took.as_millis(),
            peak_memory,
        );
    }

    if took >= SLOW_CALL_THRESHOLD {
        let text_field = msg_text.map(|x| format!(" msg: {}", x)).unwrap_or(format!(""));
        if peak_memory > ByteSize::default() {
            warn!(
                "Function exceeded time limit {}:{} {:?} took: {}ms {} peak_memory: {}",
                class_name,
                get_tid(),
                std::any::type_name::<Message>(),
                took.as_millis(),
                text_field,
                peak_memory,
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

        let stat = get_thread_stats_logger();
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
