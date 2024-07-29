use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
use time::ext::InstantExt;

use std::sync::LazyLock;

use crate::{Deadline, Duration, Instant, Utc};

// Instant doesn't have a deterministic constructor,
// however since Instant is not convertible to an unix timestamp,
// we can snapshot Instant::now() once and treat it as a constant.
// All observable effects will be then deterministic.
static FAKE_CLOCK_MONO_START: LazyLock<Instant> = LazyLock::new(Instant::now);

// An arbitrary non-trivial deterministic Utc timestamp.
const FAKE_CLOCK_UTC_START: LazyLock<Utc> =
    LazyLock::new(|| Utc::from_unix_timestamp(89108233).unwrap());

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
    /// Preferably construct it directly in the main() function,
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
            ClockInner::Real => tokio::time::sleep_until(t.into()).await,
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
        let by = t.signed_duration_since(self.now());
        self.advance(by);
    }
}

/// TEST-ONLY
#[derive(Clone)]
pub struct FakeClock(Arc<Mutex<FakeClockInner>>);

impl FakeClock {
    /// Constructor of a fake clock. Use it in tests.
    /// It supports manually moving time forward (via advance()).
    /// You can also arbitrarily set the UTC time in runtime.
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
    next: Instant,
    period: time::Duration,
}

impl Interval {
    pub fn new(next: Instant, period: time::Duration) -> Self {
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
        self.next = now.add_signed(self.period).sub_signed(Duration::nanoseconds(
            ((now.signed_duration_since(self.next)).whole_nanoseconds()
                % self.period.whole_nanoseconds())
            .try_into()
            // This operation is practically guaranteed not to
            // fail, as in order for it to fail, `period` would
            // have to be longer than `now - timeout`, and both
            // would have to be longer than 584 years.
            //
            // If it did fail, there's not a good way to pass
            // the error along to the user, so we just panic.
            .expect("too much time has elapsed since the interval was supposed to tick"),
        ));
    }
}
