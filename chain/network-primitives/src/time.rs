/// Time module provides a non-global clock, which should be passed
/// as an argument to functions which need to read the current time.
/// In particular try to avoid storing the clock instances in the objects.
/// Functions which use system clock directly are non-hermetic, which
/// makes them effectively non-deterministic and hard to test.
///
/// Clock provides 2 types of time reads:
/// 1. now() (aka POSIX CLOCK_MONOTONIC, aka time::Instant)
///    time as perceived by the machine making the measurement.
///    The subsequent calls to now() are guaranteed to return monotonic
///    results. It should be used for measuring the latency of operations
///    as observed by the machine. The time::Instant itself doesn't
///    translate to any specific timestamp, so it is not meaningful for
///    anyone other than the machine doing the measurement.
/// 2. utc_now() (aka POSIX CLOCK_REALTIME, aka time::Utc)
///    expected to approximate the (global) UTC time.
///    There is NO guarantee that the subsequent reads will be monotonic,
///    as CLOCK_REALTIME it configurable in the OS settings, or can be updated
///    during NTP sync. Should be used whenever you need to communicate a timestamp
///    over the network, or store it for later use. Remember that clocks
///    of different machines are not perfectly synchronized, and in extreme
///    cases can be totally skewed.
use borsh::{BorshDeserialize, BorshSerialize};
use once_cell::sync::Lazy;
use std::sync::{Arc, RwLock};
pub use time::error;

// TODO: consider wrapping these types to prevent interactions
// with other time libraries, especially to prevent the direct access
// to the realtime (i.e. not through the Clock).
pub type Instant = time::Instant;
// TODO: OffsetDateTime stores the timestamp in a decomposed form of
// (year,month,day,hour,...). If we find it inefficient, we should
// probably migrate to a pure UNIX timestamp and convert is to datetime
// only when needed.
pub type Utc = time::OffsetDateTime;
pub type Duration = time::Duration;

// Instant doesn't have a deterministic contructor,
// however since Instant is not convertible to an unix timestamp,
// we can snapshot Instant::now() once and treat it as a constant.
// All observable effects will be then deterministic.
static FAKE_CLOCK_MONO_START: Lazy<Instant> = Lazy::new(Instant::now);

// An arbitrary non-trivial deterministic Utc timestamp.
const FAKE_CLOCK_UTC_START: Lazy<Utc> = Lazy::new(|| Utc::from_unix_timestamp(89108233).unwrap());

/// A wrapper around `Utc` which makes that type serializable with borsh.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct UtcSerializable {
    pub nanoseconds: i128,
}

impl UtcSerializable {
    pub fn to_instant(&self) -> Result<Utc, time::error::ComponentRange> {
        Utc::from_unix_timestamp_nanos(self.nanoseconds)
    }

    pub fn from_instant(t: Utc) -> Self {
        Self { nanoseconds: t.unix_timestamp_nanos() }
    }
}

#[derive(Clone)]
enum ClockInner {
    Real,
    Fake(FakeClock),
}

/// Clock encapsulates a system clock, allowing to replace it
/// with a fake in tests.
/// Since system clock is a source of external information,
/// it has to be replaced with a fake double, if we want our
/// tests to be deterministic.
///
/// This is a reimplementation of primitives/src/time.rs
/// with a more systematic approach.
/// TODO: add tests, put it is some reusable package and use
/// throughout the nearcore codebase.
#[derive(Clone)]
pub struct Clock(ClockInner);

impl Clock {
    /// Constructor of the real clock. Use it in production code.
    /// Preferrably construct it directly in the main() function,
    /// so that it can be faked out in every other function.
    pub fn real() -> Clock {
        Clock(ClockInner::Real)
    }
    /// Current time according to the monotonic clock.
    pub fn now(&self) -> Instant {
        match &self.0 {
            ClockInner::Real => Instant::now(),
            ClockInner::Fake(fake) => fake.now(),
        }
    }
    /// Current time according to the system/walltime clock.
    pub fn now_utc(&self) -> Utc {
        match &self.0 {
            ClockInner::Real => Utc::now_utc(),
            ClockInner::Fake(fake) => fake.now_utc(),
        }
    }
}

struct FakeClockInner {
    mono: Instant,
    utc: Utc,
}

/// TEST-ONLY
#[derive(Clone)]
pub struct FakeClock(Arc<RwLock<FakeClockInner>>);

impl FakeClock {
    /// Constructor of a fake clock. Use it in tests.
    /// It allows for manually moving the time forward (via advance())
    /// and arbitrarly setting the UTC time in runtime.
    /// Use FakeClock::clock() when calling prod code from tests.
    // TODO: add support for auto-advancing the clock at each read.
    pub fn new(utc: Utc) -> Self {
        Self(Arc::new(RwLock::new(FakeClockInner { utc, mono: *FAKE_CLOCK_MONO_START })))
    }
    pub fn now(&self) -> Instant {
        self.0.read().unwrap().mono
    }
    pub fn now_utc(&self) -> Utc {
        self.0.read().unwrap().utc
    }
    pub fn clock(&self) -> Clock {
        Clock(ClockInner::Fake(self.clone()))
    }
    pub fn advance(&self, d: Duration) {
        assert!(d >= Duration::ZERO);
        let mut c = self.0.write().unwrap();
        c.mono += d;
        c.utc += d;
    }
    pub fn set_utc(&self, utc: Utc) {
        self.0.write().unwrap().utc = utc;
    }
}

impl Default for FakeClock {
    fn default() -> FakeClock {
        Self::new(*FAKE_CLOCK_UTC_START)
    }
}
