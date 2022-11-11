//! Time module provides a non-global clock, which should be passed
//! as an argument to functions which need to read the current time.
//! In particular try to avoid storing the clock instances in the objects.
//! Functions which use system clock directly are non-hermetic, which
//! makes them effectively non-deterministic and hard to test.
//!
//! Clock provides 2 types of time reads:
//! 1. now() (aka POSIX CLOCK_MONOTONIC, aka time::Instant)
//!    time as perceived by the machine making the measurement.
//!    The subsequent calls to now() are guaranteed to return monotonic
//!    results. It should be used for measuring the latency of operations
//!    as observed by the machine. The time::Instant itself doesn't
//!    translate to any specific timestamp, so it is not meaningful for
//!    anyone other than the machine doing the measurement.
//! 2. utc_now() (aka POSIX CLOCK_REALTIME, aka time::Utc)
//!    expected to approximate the (global) UTC time.
//!    There is NO guarantee that the subsequent reads will be monotonic,
//!    as CLOCK_REALTIME it configurable in the OS settings, or can be updated
//!    during NTP sync. Should be used whenever you need to communicate a timestamp
//!    over the network, or store it for later use. Remember that clocks
//!    of different machines are not perfectly synchronized, and in extreme
//!    cases can be totally skewed.
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use tokio::sync::watch; 
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

    /// Cancellable.
    pub async fn sleep_until(&self, t: Instant) {
        match &self.0 {
            ClockInner::Real => tokio::time::sleep_until(t.into_inner().into()).await,
            ClockInner::Fake(fake) => fake.sleep_until(t).await,
        }
    }

    /// Cancellable.
    pub async fn sleep(&self, d: Duration) {
        match &self.0 {
            ClockInner::Real => tokio::time::sleep(d.try_into().unwrap()).await,
            ClockInner::Fake(fake) => fake.sleep(d).await,
        }
    }
}

struct FakeClockInner {
    auto_advance: Duration,
    mono: watch::Sender<Instant>,
    utc: Utc,
    /// We need to keep it so that mono.send() always succeeds. 
    _mono_recv: watch::Receiver<Instant>,
}

impl FakeClockInner {
    pub fn new(utc: Utc) -> Self {
        let (mono,_mono_recv) = watch::channel(*FAKE_CLOCK_MONO_START);
        Self {
            auto_advance: Duration::seconds(1),
            utc,
            mono,
            _mono_recv,
        }
    }

    pub fn now(&mut self) -> Instant {
        self.advance(self.auto_advance);
        *self.mono.borrow()
    }
    pub fn now_utc(&mut self) -> Utc {
        self.advance(self.auto_advance);
        self.utc
    }
    pub fn advance(&mut self, d: Duration) {
        if d <= Duration::ZERO {
            return;
        }
        let now = *self.mono.borrow();
        self.mono.send(now+d).unwrap();
        self.utc += d;
    }
    pub fn advance_until(&mut self, t: Instant) {
        let now = *self.mono.borrow();
        if t <= now {
            return;
        }
        self.mono.send(t).unwrap();
        self.utc += t - now;
    }
}

/// TEST-ONLY
#[derive(Clone)]
pub struct FakeClock(Arc<Mutex<FakeClockInner>>);

impl FakeClock {
    /// Constructor of a fake clock. Use it in tests.
    /// It support both automatically progressing time 
    /// (current time moves forward by auto_advance at every clock read)
    /// and manually moving time forward (via advance()).
    /// You can also arbitrarly set the UTC time in runtime.
    /// Use FakeClock::clock() when calling prod code from tests.
    pub fn new(utc: Utc) -> Self {
        Self(Arc::new(Mutex::new(FakeClockInner::new(utc)))) 
    }
    pub fn now(&self) -> Instant {
        self.0.lock().unwrap().now()
    }
    pub fn now_utc(&self) -> Utc {
        self.0.lock().unwrap().now_utc()
    }
    pub fn advance(&self, d: Duration) {
        self.0.lock().unwrap().advance(d);
    }
    pub fn advance_until(&self, t: Instant) {
        self.0.lock().unwrap().advance_until(t);
    }
    pub fn clock(&self) -> Clock {
        Clock(ClockInner::Fake(self.clone()))
    } 
    pub fn set_utc(&self, utc: Utc) {
        self.0.lock().unwrap().utc = utc;
    }
    /// Set how much to move forward at every call to now/now_utc.
    /// Set to Duration::ZERO, if you want to disable auto_advance completely.
    pub fn set_auto_advance(&self, auto_advance: Duration) {
        self.0.lock().unwrap().auto_advance = auto_advance;
    }

    /// Cancel-safe.
    pub async fn sleep(&self, d: Duration) {
        let mut watch = self.0.lock().unwrap().mono.subscribe();
        let t = *watch.borrow() + d;
        while *watch.borrow()<t {
            watch.changed().await.unwrap();
        }
    }

    /// Cancel-safe.
    pub async fn sleep_until(&self, t: Instant) {
        let mut watch = self.0.lock().unwrap().mono.subscribe(); 
        while *watch.borrow()<t {
            watch.changed().await.unwrap();
        }
    }
}

impl Default for FakeClock {
    fn default() -> FakeClock {
        Self::new(*FAKE_CLOCK_UTC_START)
    }
}
