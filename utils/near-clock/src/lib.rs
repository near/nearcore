use std::cell::RefCell;
use std::ops::Sub;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

thread_local! {
    static TIME_OVERRIDE: RefCell<Arc<AtomicU64>> = RefCell::new(Arc::new(AtomicU64::new(0)));
}

pub fn mock_time(new_time: NearClock) {
    TIME_OVERRIDE.with(|time_override| {
        let ns = new_time.duration_since(UNIX_EPOCH);
        time_override.borrow().store(ns.as_millis() as u64, Ordering::SeqCst)
    });
}

pub fn propagate_time_mock(time_mock: Arc<AtomicU64>) {
    TIME_OVERRIDE.with(|time_override| {
        *time_override.borrow_mut() = time_mock;
    });
}

pub const UNIX_EPOCH: NearClock = NearClock::from_system_time(SystemTime::UNIX_EPOCH);

pub fn get_time_override() -> Arc<AtomicU64> {
    TIME_OVERRIDE.with(|x| x.borrow().clone())
}

pub fn now() -> NearClock {
    TIME_OVERRIDE.with(|time_override| {
        let time_override = time_override.borrow().load(Ordering::SeqCst);
        if time_override == 0 {
            NearClock { time: SystemTime::now() }
        } else {
            NearClock { time: SystemTime::UNIX_EPOCH + Duration::from_millis(time_override) }
        }
    })
}

pub struct NearClock {
    time: SystemTime,
}

impl NearClock {
    pub const fn from_system_time(system_time: SystemTime) -> NearClock {
        NearClock { time: system_time }
    }

    pub fn duration_since(&self, rhs: NearClock) -> NearDuration {
        NearDuration {
            duration: self.time.duration_since(rhs.time).unwrap_or(Duration::from_millis(0)),
        }
    }
}

pub struct NearDuration {
    duration: Duration,
}

impl NearDuration {
    pub fn to_std(&self) -> Duration {
        self.duration
    }

    pub fn as_millis(&self) -> u128 {
        self.duration.as_millis()
    }
}

// Notice that the implementation uses the associated type `Output`.
impl Sub for NearClock {
    type Output = NearDuration;

    fn sub(self, other: Self) -> Self::Output {
        let duration = self.time.duration_since(other.time).unwrap_or(Duration::from_millis(0));

        NearDuration { duration }
    }
}
