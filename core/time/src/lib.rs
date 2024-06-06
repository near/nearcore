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
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
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
    utc: Utc,
    instant: Instant,
    waiters: BinaryHeap<ClockWaiterInHeap>,
}

/// Whenever a user of a FakeClock calls `sleep` for `sleep_until`, we create a
/// `ClockWaiterInHeap` so that the returned future can be completed when the
/// clock advances past the desired deadline.
struct ClockWaiterInHeap {
    deadline: Instant,
    waker: tokio::sync::oneshot::Sender<()>,
}

impl PartialEq for ClockWaiterInHeap {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl PartialOrd for ClockWaiterInHeap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ClockWaiterInHeap {}

impl Ord for ClockWaiterInHeap {
    fn cmp(&self, other: &Self) -> Ordering {
        other.deadline.cmp(&self.deadline)
    }
}

impl FakeClockInner {
    pub fn new(utc: Utc) -> Self {
        Self { utc, instant: *FAKE_CLOCK_MONO_START, waiters: BinaryHeap::new() }
    }

    pub fn now(&mut self) -> Instant {
        self.instant
    }
    pub fn now_utc(&mut self) -> Utc {
        self.utc
    }
    pub fn advance(&mut self, d: Duration) {
        assert!(d >= Duration::ZERO);
        if d == Duration::ZERO {
            return;
        }
        self.instant += d;
        self.utc += d;
        while let Some(earliest_waiter) = self.waiters.peek() {
            if earliest_waiter.deadline <= self.instant {
                self.waiters.pop().unwrap().waker.send(()).ok();
            } else {
                break;
            }
        }
    }
    pub fn advance_until(&mut self, t: Instant) {
        let by = t - self.now();
        self.advance(by);
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
        if d <= Duration::ZERO {
            return;
        }
        let receiver = {
            let mut inner = self.0.lock().unwrap();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let waiter = ClockWaiterInHeap { waker: sender, deadline: inner.now() + d };
            inner.waiters.push(waiter);
            receiver
        };
        receiver.await.unwrap();
    }

    /// Cancel-safe.
    pub async fn sleep_until(&self, t: Instant) {
        let receiver = {
            let mut inner = self.0.lock().unwrap();
            if inner.now() >= t {
                return;
            }
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let waiter = ClockWaiterInHeap { waker: sender, deadline: t };
            inner.waiters.push(waiter);
            receiver
        };
        receiver.await.unwrap();
    }

    /// Returns the earliest waiter, or None if no one is waiting on the clock.
    /// The returned instant is guaranteed to be <= any waiter that is currently
    /// waiting on the clock to advance.
    pub fn first_waiter(&self) -> Option<Instant> {
        let inner = self.0.lock().unwrap();
        inner.waiters.peek().map(|waiter| waiter.deadline)
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

/// Provides serialization of Duration as std::time::Duration.
pub mod serde_duration_as_std {
    use crate::Duration;
    use serde::Deserialize;
    use serde::Serialize;

    pub fn serialize<S>(dur: &Duration, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let std: std::time::Duration = (*dur)
            .try_into()
            .map_err(|_| serde::ser::Error::custom("Duration conversion failed"))?;
        std.serialize(s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Duration, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let std: std::time::Duration = Deserialize::deserialize(d)?;
        Ok(std.try_into().map_err(|_| serde::de::Error::custom("Duration conversion failed"))?)
    }
}

/// Provides serialization of Duration as std::time::Duration.
pub mod serde_opt_duration_as_std {
    use crate::Duration;
    use serde::Deserialize;
    use serde::Serialize;

    pub fn serialize<S>(dur: &Option<Duration>, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match dur {
            Some(dur) => {
                let std: std::time::Duration = (*dur)
                    .try_into()
                    .map_err(|_| serde::ser::Error::custom("Duration conversion failed"))?;
                std.serialize(s)
            }
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Duration>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let std: Option<std::time::Duration> = Deserialize::deserialize(d)?;
        Ok(std
            .map(|std| {
                std.try_into().map_err(|_| serde::de::Error::custom("Duration conversion failed"))
            })
            .transpose()?)
    }
}

pub mod serde_utc_as_iso {
    use crate::Utc;
    use serde::{Deserialize, Serialize};
    use time::format_description::well_known::Iso8601;

    pub fn serialize<S>(utc: &Utc, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        utc.format(&Iso8601::DEFAULT).map_err(<S::Error as serde::ser::Error>::custom)?.serialize(s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Utc, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: String = Deserialize::deserialize(d)?;
        Utc::parse(&str, &Iso8601::DEFAULT).map_err(<D::Error as serde::de::Error>::custom)
    }
}

pub mod serde_opt_utc_as_iso {
    use crate::Utc;
    use serde::{Deserialize, Serialize};
    use time::format_description::well_known::Iso8601;

    pub fn serialize<S>(utc: &Option<Utc>, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match utc {
            Some(utc) => utc
                .format(&Iso8601::DEFAULT)
                .map_err(<S::Error as serde::ser::Error>::custom)?
                .serialize(s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Utc>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: Option<String> = Deserialize::deserialize(d)?;
        Ok(str
            .map(|str| {
                Utc::parse(&str, &Iso8601::DEFAULT).map_err(<D::Error as serde::de::Error>::custom)
            })
            .transpose()?)
    }
}

#[cfg(test)]
mod tests {
    use crate::Duration;
    use serde_json;

    #[test]
    fn test_serde_duration() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Test(#[serde(with = "super::serde_duration_as_std")] Duration);

        let expected = Test(Duration::milliseconds(1234));
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#"{"secs":1,"nanos":234000000}"#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_serde_opt_duration() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Test(#[serde(with = "super::serde_opt_duration_as_std")] Option<Duration>);

        let expected = Test(Some(Duration::milliseconds(1234)));
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#"{"secs":1,"nanos":234000000}"#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);

        let expected = Test(None);
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#"null"#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_serde_utc() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Test(#[serde(with = "super::serde_utc_as_iso")] super::Utc);

        let expected =
            Test(super::Utc::from_unix_timestamp_nanos(1709582343123456789i128).unwrap());
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#""2024-03-04T19:59:03.123456789Z""#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_serde_opt_utc() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Test(#[serde(with = "super::serde_opt_utc_as_iso")] Option<super::Utc>);

        let expected =
            Test(Some(super::Utc::from_unix_timestamp_nanos(1709582343123456789i128).unwrap()));
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#""2024-03-04T19:59:03.123456789Z""#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);

        let expected = Test(None);
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#"null"#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);
    }
}
