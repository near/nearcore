use bytesize::ByteSize;
use futures;
use futures::task::Context;
use once_cell::sync::Lazy;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};
use tracing::{info, warn};

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
        self.write_buf_added += ByteSize::b(bytes as u64);
        self.write_buf_len = ByteSize::b(buf_len as u64);
        self.write_buf_capacity = ByteSize::b(buf_capacity as u64);
        self.write_buf_capacity = ByteSize::b(buf_capacity as u64);
    }

    pub fn log_drain_write_buffer(&mut self, bytes: usize, buf_len: usize, buf_capacity: usize) {
        self.write_buf_drained += ByteSize::b(bytes as u64);
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

        let took_since_last_check = min(took, now.saturating_duration_since(self.last_check));

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
        tid: std::thread::ThreadId,
        sleep_time: Duration,
        now: Instant,
    ) -> (f64, f64) {
        let mut ratio = self.time.as_nanos() as f64;
        if let Some(in_progress_since) = self.in_progress_since {
            let from = max(in_progress_since, self.last_check);
            ratio += (now.saturating_duration_since(from)).as_nanos() as f64;
        }
        ratio /= sleep_time.as_nanos() as f64;

        let show_stats = ratio >= MIN_OCCUPANCY_RATIO_THRESHOLD;

        if show_stats {
            let class_name = format!("{:?}", self.classes);
            warn!(
                "    {:?}: ratio: {:.3} {}:{:?} C mem: {}",
                tid,
                ratio,
                class_name,
                std::thread::current().id(),
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
            stat.sort_by_key(|f| f.0);

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
            (ratio, 0.0)
        } else {
            (ratio, ratio)
        }
    }

    fn clear(&mut self) {
        self.time = Duration::default();
        self.cnt = 0;
        self.stat.clear();
    }
}

pub(crate) struct Stats {
    stats: HashMap<std::thread::ThreadId, Arc<Mutex<ThreadStats>>>,
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
        let tid = std::thread::current().id();
        self.stats.entry(tid).or_insert_with(|| Arc::clone(local_stats));
    }

    fn print_stats(&mut self, sleep_time: Duration) {
        info!(
            "Performance stats {} threads (min ratio = {})",
            self.stats.len(),
            MIN_OCCUPANCY_RATIO_THRESHOLD
        );
        let s: Vec<_> = self.stats.iter().collect();

        let mut ratio = 0.0;
        let mut other_ratio = 0.0;
        let now = Instant::now();
        for entry in s {
            let (tmp_ratio, tmp_other_ratio) =
                entry.1.lock().unwrap().print_stats_and_clear(*entry.0, sleep_time, now);
            ratio += tmp_ratio;
            other_ratio += tmp_other_ratio;
        }
        info!("    Other threads ratio {:.3}", other_ratio);
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
    for<'a> &'a Message: Into<&'static str>,
{
    let msg_text = (&msg).into();
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

    let start = Instant::now();
    stat.lock().unwrap().pre_log(start);
    let result = f(msg);

    let ended = Instant::now();
    let took = ended.saturating_duration_since(start);

    if took >= SLOW_CALL_THRESHOLD {
        let text_field = msg_text.map(|x| format!(" msg: {}", x)).unwrap_or(format!(""));
        warn!(
            "Function exceeded time limit {}:{:?} {:?} took: {}ms {}",
            class_name,
            std::thread::current().id(),
            std::any::type_name::<Message>(),
            took.as_millis(),
            text_field,
        );
    }
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
        let start = Instant::now();
        stat.lock().unwrap().pre_log(start);

        let res = unsafe { Pin::new_unchecked(&mut this.f) }.poll(cx);
        let ended = Instant::now();
        let took = ended.saturating_duration_since(start);
        stat.lock().unwrap().log(this.class_name, this.file, this.line, took, ended, "");

        if took > SLOW_CALL_THRESHOLD {
            warn!(
                "Function exceeded time limit {}:{:?} {}:{} took: {}ms",
                this.class_name,
                std::thread::current().id(),
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
