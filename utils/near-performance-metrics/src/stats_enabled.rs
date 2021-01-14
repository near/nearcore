use log::info;
use std::{
    cell::RefCell,
    cmp::max,
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Poll,
    time::{Duration, Instant},
};

use futures::{self, task::Context};
use once_cell::sync::Lazy;
use std::pin::Pin;
use strum::AsStaticRef;

pub static NTHREADS: AtomicUsize = AtomicUsize::new(0);
pub(crate) const SLOW_CALL_THRESHOLD: Duration = Duration::from_millis(500);
const MIN_OCCUPANCY_RATIO_THRESHOLD: f64 = 0.02;

pub(crate) static STATS: Lazy<Arc<Mutex<Stats>>> = Lazy::new(|| Arc::new(Mutex::new(Stats::new())));
pub(crate) static REF_COUNTER: Lazy<Mutex<HashMap<(&'static str, u32), u128>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

thread_local! {
    pub(crate) static TID: RefCell<usize> = RefCell::new(0);
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

        if ratio > MIN_OCCUPANCY_RATIO_THRESHOLD {
            let class_name = format!("{:?}", self.classes);
            info!(
                "    Thread:{} ratio: {:.4} {}:{} ",
                tid,
                ratio,
                class_name,
                TID.with(|x| *x.borrow()),
            );
            let mut s: Vec<_> = self.stat.iter().collect();
            s.sort_by(|x, y| (*x).0.cmp(&(*y).0));

            for entry in s {
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

pub(crate) struct Stats {
    stats: HashMap<usize, ThreadStats>,
}

impl Stats {
    fn new() -> Self {
        Self { stats: HashMap::new() }
    }

    pub(crate) fn log(
        &mut self,
        class_name: &'static str,
        msg: &'static str,
        line: u32,
        took: Duration,
    ) {
        let tid = TID.with(|x| {
            if *x.borrow_mut() == 0 {
                *x.borrow_mut() = NTHREADS.fetch_add(1, Ordering::SeqCst);
            }
            *x.borrow_mut()
        });
        let entry = self.stats.entry(tid).or_insert_with(|| ThreadStats::new());
        entry.log(class_name, msg, line, took)
    }

    fn print_stats(&mut self, sleep_time: Duration) {
        info!("Performance stats {} threads", self.stats.len());
        let mut s: Vec<_> = self.stats.iter().collect();
        s.sort_by(|x, y| (*x).0.cmp(&(*y).0));

        for entry in s {
            entry.1.print_stats(*entry.0, sleep_time);
        }
        self.stats.clear();
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
    let now = Instant::now();
    let result = f(msg);

    let took = now.elapsed();
    if took > SLOW_CALL_THRESHOLD {
        let text_field = msg_text.map(|x| format!(" msg: {}", x)).unwrap_or(format!(""));
        info!(
            "Function exceeded time limit {}:{} {:?} took: {}ms {}",
            class_name,
            TID.with(|x| *x.borrow()),
            std::any::type_name::<Message>(),
            took.as_millis(),
            text_field
        );
    }
    STATS.lock().unwrap().log(class_name, std::any::type_name::<Message>(), 0, took);
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

        let now = Instant::now();
        let res = unsafe { Pin::new_unchecked(&mut this.f) }.poll(cx);
        let took = now.elapsed();
        STATS.lock().unwrap().log(this.class_name, this.file, this.line, took);

        if took > SLOW_CALL_THRESHOLD {
            info!(
                "Function exceeded time limit {}:{} {}:{} took: {}ms",
                this.class_name,
                TID.with(|x| *x.borrow()),
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
    info!("Futures waiting for completition {}", REF_COUNTER.lock().unwrap().len());
    for entry in REF_COUNTER.lock().unwrap().iter() {
        if *entry.1 > 0 {
            info!("    future {}:{} {}", (entry.0).0, (entry.0).1, entry.1);
        }
    }
}
