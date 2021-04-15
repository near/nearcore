use std::default::Default;
use std::sync::Arc;
use std::time;

use arc_swap::ArcSwap;
use chrono;
use once_cell::sync::Lazy;

pub use chrono::Utc;
pub use std::time::Instant;

pub struct UtcProxy;
pub struct InstantProxy;

pub struct TimeTravelSingleton {
    pub last_check_utc: chrono::DateTime<chrono::Utc>,
    pub last_check_instant: time::Instant,
    pub diff: i64,
    pub rate: f64,
}

impl Default for TimeTravelSingleton {
    fn default() -> Self {
        Self {
            last_check_utc: chrono::Utc::now(),
            last_check_instant: time::Instant::now(),
            diff: 0,
            rate: 1.0,
        }
    }
}

static SINGLETON: Lazy<ArcSwap<TimeTravelSingleton>> =
    Lazy::new(|| ArcSwap::from_pointee(TimeTravelSingleton::new()));

impl TimeTravelSingleton {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get() -> Arc<TimeTravelSingleton> {
        SINGLETON.load_full()
    }

    pub fn set(value: TimeTravelSingleton) {
        SINGLETON.store(Arc::new(value))
    }
}

impl UtcProxy {
    pub fn now() -> chrono::DateTime<chrono::Utc> {
        let now = chrono::Utc::now();
        let time_travel = TimeTravelSingleton::get();
        let const_diff = chrono::Duration::milliseconds(time_travel.diff);
        let last_check = time_travel.last_check_utc;
        let speed_diff = (now - last_check).num_milliseconds() as f64 * time_travel.rate;
        let speed_diff = chrono::Duration::milliseconds(speed_diff as i64);
        last_check + const_diff + speed_diff
    }
}

impl InstantProxy {
    pub fn now() -> time::Instant {
        let now = time::Instant::now();
        let time_travel = TimeTravelSingleton::get();
        let const_diff = time::Duration::from_millis(time_travel.diff.abs() as u64);
        let last_check = time_travel.last_check_instant;
        let last_check_with_const_diff =
            if time_travel.diff >= 0 { last_check + const_diff } else { last_check - const_diff };
        let speed_diff =
            now.saturating_duration_since(last_check).as_millis() as f64 * time_travel.rate;
        let speed_diff = time::Duration::from_millis(speed_diff as u64);
        last_check_with_const_diff + speed_diff
    }
}

pub trait Time {
    type Value;

    fn now_in_test() -> Self::Value;

    fn system_time() -> Self::Value;
}

impl Time for Utc {
    type Value = chrono::DateTime<chrono::Utc>;

    fn now_in_test() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    fn system_time() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl Time for Instant {
    type Value = time::Instant;

    fn now_in_test() -> time::Instant {
        time::Instant::now()
    }

    fn system_time() -> time::Instant {
        time::Instant::now()
    }
}
