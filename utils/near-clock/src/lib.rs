use std::cell::RefCell;
use std::ops::{Add, Mul, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

thread_local! {
    // A shared pointer. We should propagate it across multiple threads.
    // Replace by AtomicU128 by 2262-04-12, after that date 64 bits won't be enough to
    // represent time in nanoseconds. AtomicU128 is still in nightly build only.
    static TIME_OVERRIDE: RefCell<Arc<AtomicU64>> = RefCell::new(Arc::new(AtomicU64::new(0)));
}

// Used for tests to set clock time manually.
pub fn mock_time(new_time: NearClock) {
    TIME_OVERRIDE.with(|time_override| {
        let ns = new_time.duration_since(&UNIX_EPOCH);
        time_override.borrow().store(ns.as_nanos() as u64, Ordering::SeqCst)
    });
}

pub const UNIX_EPOCH: NearClock = NearClock::from_system_time(SystemTime::UNIX_EPOCH);

// A pointer to shared atomic, which is used to override time.
// This pointer is meant to be shared between threads. A typical usage would be to get pointer
// to atomic on the main thread running unit test, and then propagate is to sub threads
pub fn get_time_override() -> Arc<AtomicU64> {
    TIME_OVERRIDE.with(|x| x.borrow().clone())
}

// sets override on child threads
pub fn set_time_override(new_time_override: Arc<AtomicU64>) {
    TIME_OVERRIDE.with(|x| *x.borrow_mut() = new_time_override);
}

// get current time; we should use it everywhere in our code base
pub fn now() -> NearClock {
    TIME_OVERRIDE.with(|time_override| {
        let time_override = time_override.borrow().load(Ordering::SeqCst);
        if time_override == 0 {
            NearClock { time: SystemTime::now() }
        } else {
            NearClock { time: SystemTime::UNIX_EPOCH + Duration::from_nanos(time_override) }
        }
    })
}

// TODO: Add other methods that `SystemTime` has.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NearClock {
    time: SystemTime,
}

impl NearClock {
    pub const fn from_system_time(system_time: SystemTime) -> NearClock {
        NearClock { time: system_time }
    }

    pub fn now() -> NearClock {
        now()
    }

    pub fn duration_since(&self, rhs: &NearClock) -> Duration {
        self.time.duration_since(rhs.time).unwrap_or_default()
    }

    pub fn elapsed(&self) -> Duration {
        NearClock::now().duration_since(self)
    }

    pub fn from_timestamp(timestamp: u64) -> Self {
        UNIX_EPOCH + Duration::from_nanos(timestamp)
    }

    pub fn to_timestamp(&self) -> u64 {
        self.duration_since(&UNIX_EPOCH).as_nanos() as u64
    }
}

impl Add<Duration> for NearClock {
    type Output = Self;

    fn add(self, other: Duration) -> Self {
        Self { time: self.time + other }
    }
}

impl Sub for NearClock {
    type Output = Duration;

    fn sub(self, other: Self) -> Self::Output {
        self.time.duration_since(other.time).unwrap_or(Duration::from_millis(0))
    }
}

#[cfg(test)]
mod tests {
    use crate::{get_time_override, mock_time, set_time_override, NearClock, UNIX_EPOCH};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_mock_override() {
        let time_override = get_time_override();

        let near_time = UNIX_EPOCH + Duration::from_millis(1001);
        let near_time2 = near_time.clone();

        let _ = thread::spawn(move || {
            set_time_override(time_override);

            mock_time(near_time2);
        })
        .join();

        assert_eq!(NearClock::now(), near_time);
    }
}
