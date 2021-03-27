use std::borrow::Cow;
use std::collections::HashMap;
use std::default::Default;
use std::mem;
use std::sync::{Arc, Mutex, Once};
use std::time;

use chrono;

pub use chrono::Utc;
pub use std::time::Instant;

pub struct UtcProxy;
pub struct InstantProxy;

pub struct TimeTravelSingleton {
    pub last_check_utc: chrono::DateTime<chrono::Utc>,
    pub last_check_instant: time::Instant,
    pub diff: i64,
    pub rate: f64,
    pub proxify: HashMap<FileLocation, bool>,
}

#[derive(Eq, PartialEq, Hash, Debug)]
pub struct FileLocation {
    pub file: Cow<'static, str>,
    pub line: u32,
}

#[derive(Clone)]
struct SingletonReader {
    // Since we might be used in many threads, we need to protect
    // concurrent access
    inner: Arc<Mutex<TimeTravelSingleton>>,
}

impl Default for TimeTravelSingleton {
    fn default() -> Self {
        Self {
            last_check_utc: chrono::Utc::now(),
            last_check_instant: time::Instant::now(),
            diff: 0,
            rate: 1.0,
            proxify: HashMap::new(),
        }
    }
}

impl TimeTravelSingleton {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(value: Self) {
        SingletonReader::set(value)
    }
}

static mut SINGLETON: *const SingletonReader = 0 as *const SingletonReader;
static ONCE: Once = Once::new();

impl SingletonReader {
    fn get() -> SingletonReader {
        unsafe {
            ONCE.call_once(|| {
                SingletonReader::set(TimeTravelSingleton::default());
            });

            (*SINGLETON).clone()
        }
    }

    fn set(value: TimeTravelSingleton) {
        let singleton = SingletonReader { inner: Arc::new(Mutex::new(value)) };
        unsafe {
            SINGLETON = mem::transmute(Box::new(singleton));
        }
    }
}

impl UtcProxy {
    pub fn now(file: &'static str, line: u32) -> chrono::DateTime<chrono::Utc> {
        let now = chrono::Utc::now();
        let singleton = SingletonReader::get();
        let time_travel = singleton.inner.lock().expect("poisoned mutex");
        if let Some(&false) = time_travel.proxify.get(&FileLocation { file: file.into(), line }) {
            now
        } else {
            let const_diff = chrono::Duration::milliseconds(time_travel.diff);
            let last_check = time_travel.last_check_utc;
            let speed_diff = (now - last_check).num_milliseconds() as f64 * time_travel.rate;
            let speed_diff = chrono::Duration::milliseconds(speed_diff as i64);
            last_check + const_diff + speed_diff
        }
    }
}

impl InstantProxy {
    pub fn now(file: &'static str, line: u32) -> time::Instant {
        let now = time::Instant::now();
        let singleton = SingletonReader::get();
        let time_travel = singleton.inner.lock().expect("poisoned mutex");
        if let Some(&false) = time_travel.proxify.get(&FileLocation { file: file.into(), line }) {
            now
        } else {
            let const_diff = time::Duration::from_millis(time_travel.diff as u64);
            let last_check = time_travel.last_check_instant;
            let speed_diff =
                now.saturating_duration_since(last_check).as_millis() as f64 * time_travel.rate;
            let speed_diff = time::Duration::from_millis(speed_diff as u64);
            last_check + const_diff + speed_diff
        }
    }
}

pub trait Time {
    type Value;

    fn now_in_test() -> Self::Value;

    fn system_time(file: &'static str, line: u32) -> Self::Value;
}

impl Time for Utc {
    type Value = chrono::DateTime<chrono::Utc>;

    fn now_in_test() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    fn system_time(_file: &'static str, _line: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl Time for Instant {
    type Value = time::Instant;

    fn now_in_test() -> time::Instant {
        time::Instant::now()
    }

    fn system_time(_file: &'static str, _line: u32) -> time::Instant {
        time::Instant::now()
    }
}
