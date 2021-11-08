use std::cell::RefCell;
use std::ops::{Add, Sub};
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

pub fn set_time_override(new_time_override: Arc<AtomicU64>) {
    TIME_OVERRIDE.with(|x| *x.borrow_mut() = new_time_override);
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

// TODO: Add other methods that `SystemTime` has.
#[derive(Eq, PartialEq, Debug, Clone)]
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

    pub fn duration_since(&self, rhs: NearClock) -> NearDuration {
        NearDuration {
            duration: self.time.duration_since(rhs.time).unwrap_or(Duration::from_millis(0)),
        }
    }
}

// TODO: Add other methods that `Duration` has.
#[derive(Eq, PartialEq, Debug, Clone)]
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

    pub fn from_millis(ns: u64) -> NearDuration {
        Self { duration: Duration::from_millis(ns) }
    }
}

impl Add<NearDuration> for NearClock {
    type Output = Self;

    fn add(self, other: NearDuration) -> Self {
        Self { time: self.time + other.duration }
    }
}

impl Sub for NearClock {
    type Output = NearDuration;

    fn sub(self, other: Self) -> Self::Output {
        let duration = self.time.duration_since(other.time).unwrap_or(Duration::from_millis(0));

        NearDuration { duration }
    }
}

#[cfg(test)]
mod tests {
    use crate::{get_time_override, mock_time, set_time_override, NearClock, NearDuration};
    use std::thread;

    #[test]
    fn test_mock_override() {
        let time_override = get_time_override();

        let near_time = NearClock::now() + NearDuration::from_millis(1000);
        let near_time2 = near_time.clone();

        let _ = thread::spawn(move || {
            set_time_override(time_override);

            mock_time(near_time2);

            // some work here
        })
        .join();

        assert_eq!(NearClock::now(), near_time);
    }
}
