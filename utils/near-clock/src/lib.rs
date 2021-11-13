use std::cell::RefCell;
use std::ops::{Add, Mul, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub const UNIX_EPOCH: NearClock = NearClock::from_system_time(SystemTime::UNIX_EPOCH);

// get current time; we should use it everywhere in our code base
pub fn now() -> NearClock {
    NearClock { time: SystemTime::now() }
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
