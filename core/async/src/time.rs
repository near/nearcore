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
//! 2. now_utc() (aka POSIX CLOCK_REALTIME, aka time::Utc)
//!    expected to approximate the (global) UTC time.
//!    There is NO guarantee that the subsequent reads will be monotonic,
//!    as CLOCK_REALTIME it configurable in the OS settings, or can be updated
//!    during NTP sync. Should be used whenever you need to communicate a timestamp
//!    over the network, or store it for later use. Remember that clocks
//!    of different machines are not perfectly synchronized, and in extreme
//!    cases can be totally skewed.
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
pub use time::error;
use tokio::sync::watch;

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

// By the definition of derive(PartialEq), Finite(...) < Infinite.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum Deadline {
    Finite(Instant),
    Infinite,
}

impl From<Instant> for Deadline {
    fn from(t: Instant) -> Deadline {
        Deadline::Finite(t)
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
/// TODO: add tests.
#[derive(Clone)]
pub struct Clock(ClockInner);

impl Clock {
    /// Constructor of the real clock. Use it in production code.
    /// Preferrably construct it directly in the main() function,
    /// so that it can be faked out in every other function.
    pub fn real() -> Clock {
        Clock(ClockInner::Real)
    }

    /// Current time according to the monotone clock.
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
    pub async fn sleep_until_deadline(&self, t: Deadline) {
        match t {
            Deadline::Infinite => std::future::pending().await,
            Deadline::Finite(t) => self.sleep_until(t).await,
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
    /// `mono` keeps the current time of the monotonic clock.
    /// It is wrapped in watch::Sender, so that the value can
    /// be observed from the clock::sleep() futures.
    mono: watch::Sender<Instant>,
    utc: Utc,
    /// We need to keep it so that mono.send() always succeeds.
    _mono_recv: watch::Receiver<Instant>,
}

impl FakeClockInner {
    pub fn new(utc: Utc) -> Self {
        let (mono, _mono_recv) = watch::channel(*FAKE_CLOCK_MONO_START);
        Self { utc, mono, _mono_recv }
    }

    pub fn now(&mut self) -> Instant {
        *self.mono.borrow()
    }
    pub fn now_utc(&mut self) -> Utc {
        self.utc
    }
    pub fn advance(&mut self, d: Duration) {
        assert!(d >= Duration::ZERO);
        if d == Duration::ZERO {
            return;
        }
        let now = *self.mono.borrow();
        self.mono.send(now + d).unwrap();
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
    /// It supports manually moving time forward (via advance()).
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

    /// Cancel-safe.
    pub async fn sleep(&self, d: Duration) {
        let mut watch = self.0.lock().unwrap().mono.subscribe();
        let t = *watch.borrow() + d;
        while *watch.borrow() < t {
            watch.changed().await.unwrap();
        }
    }

    /// Cancel-safe.
    pub async fn sleep_until(&self, t: Instant) {
        let mut watch = self.0.lock().unwrap().mono.subscribe();
        while *watch.borrow() < t {
            watch.changed().await.unwrap();
        }
    }
}

impl Default for FakeClock {
    fn default() -> FakeClock {
        Self::new(*FAKE_CLOCK_UTC_START)
    }
}

/// Interval equivalent to tokio::time::Interval with
/// MissedTickBehavior::Skip.
pub struct Interval {
    next: time::Instant,
    period: time::Duration,
}

impl Interval {
    pub fn new(next: time::Instant, period: time::Duration) -> Self {
        Self { next, period }
    }

    /// Cancel-safe.
    pub async fn tick(&mut self, clock: &Clock) {
        clock.sleep_until(self.next).await;
        let now = clock.now();
        // Implementation of `tokio::time::MissedTickBehavior::Skip`.
        // Please refer to https://docs.rs/tokio/latest/tokio/time/enum.MissedTickBehavior.html#
        // for details. In essence, if more than `period` of time passes between consecutive
        // calls to tick, then the second tick completes immediately and the next one will be
        // aligned to the original schedule.
        self.next = now + self.period
            - Duration::nanoseconds(
                ((now - self.next).whole_nanoseconds() % self.period.whole_nanoseconds())
                    .try_into()
                    // This operation is practically guaranteed not to
                    // fail, as in order for it to fail, `period` would
                    // have to be longer than `now - timeout`, and both
                    // would have to be longer than 584 years.
                    //
                    // If it did fail, there's not a good way to pass
                    // the error along to the user, so we just panic.
                    .expect("too much time has elapsed since the interval was supposed to tick"),
            );
    }
}
