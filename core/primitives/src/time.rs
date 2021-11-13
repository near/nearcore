<<<<<<< HEAD
/// Provides structs used for getting time.
/// WARNING WARNING WARNING
/// WARNING WARNING WARNING
/// Use at your own risk. The implementation is not complete, we have a places in code not mocked properly.
/// For example, it's possible to call `::elapsed()` on `Instant` returned by `Instant::now()`.
/// We use that that function, throughout the code, and the current mocking of `Instant` is not done properly.
///
/// Example:
/// ```rust, ignore
/// fn some_production_function() {
///     let start = Clock::instant();
///     // some computation
///     let end = Clock::instant();
///     assert!(end.duration_since(start) == Duration::from_secs(1));
/// }
///
/// Test:
/// fn test() {
///     let mock_clock_guard = MockClockGuard::default();
///     mock_clock_guard.add_instant(Instant::now());
///     mock_clock_guard.add_instant(Instant::now() + Duration::from_secs(1));
///
///     // This won't crash anymore as long as mock works.
///     some_production_function();
/// }
/// ```
use chrono;
use chrono::DateTime;
pub use chrono::Utc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::default::Default;
pub use std::time::{Duration, Instant};

#[derive(Default)]
struct MockClockPerState {
    /// List of timestamps, we will return one timestamp for each call of `Clock::utc()`.
    utc_list: VecDeque<DateTime<Utc>>,
    /// List of timestamps, we will return one timestamp to each call of `Clock::instant()`.
    instant_list: VecDeque<Instant>,
    /// Number of times `Clock::utc()` method was called since we started mocking.
    utc_call_count: u64,
    /// Number of times `Clock::instant()` method was called since we started mocking.
    instant_call_count: u64,
}

/// Stores the mocking state.
#[derive(Default)]
struct MockClockPerThread {
    mock: Option<MockClockPerState>,
}

impl MockClockPerThread {
    fn with<F, T>(f: F) -> T
    where
        F: FnOnce(&mut MockClockPerThread) -> T,
    {
        thread_local! {
            static INSTANCE: RefCell<MockClockPerThread> = RefCell::default()
=======
// TODO#(5174) don't export external types from your own crate
pub use chrono::Utc;
pub use clock::{Clock, MockClockGuard};
// TODO#(5174) don't export external types from your own crate
pub use std::time::{Duration, Instant};
pub use time::Time;
pub use time::UnixTime;

mod clock {
    use crate::time::{Duration, Instant, Time, Utc};
    use chrono;
    use chrono::DateTime;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::default::Default;

    struct MockClockPerThread {
        utc: VecDeque<DateTime<Utc>>,
        durations: VecDeque<Duration>,
        utc_call_count: u64,
        instant_call_count: u64,
        instant: Instant,
        is_mock: bool,
    }

    pub struct Clock {}

    impl MockClockPerThread {
        pub fn reset(&mut self) {
            self.utc.clear();
            self.durations.clear();
            self.utc_call_count = 0;
            self.instant_call_count = 0;
            self.instant = Instant::now();
            self.is_mock = false;
        }

        fn with<F, T>(f: F) -> T
        where
            F: FnOnce(&mut MockClockPerThread) -> T,
        {
            thread_local! {
                static INSTANCE: RefCell<MockClockPerThread> = RefCell::default()
            }
            INSTANCE.with(|it| f(&mut *it.borrow_mut()))
        }

        fn pop_utc(&mut self) -> Option<DateTime<chrono::Utc>> {
            self.utc_call_count += 1;
            self.utc.pop_front()
        }
        fn pop_instant(&mut self) -> Option<Instant> {
            self.instant_call_count += 1;
            let x = self.durations.pop_front();
            match x {
                Some(t) => self.instant.checked_add(t),
                None => None,
            }
        }
    }

    impl Default for MockClockPerThread {
        fn default() -> Self {
            Self {
                utc: VecDeque::with_capacity(16),
                durations: VecDeque::with_capacity(16),
                utc_call_count: 0,
                instant_call_count: 0,
                instant: Instant::now(),
                is_mock: false,
            }
>>>>>>> Reorganize imports in near-network
        }
    }
}

<<<<<<< HEAD
pub struct MockClockGuard {}

impl MockClockGuard {
    /// Adds timestamp to queue, it will be returned in `Self::utc()`.
    pub fn add_utc(&self, mock_date: DateTime<chrono::Utc>) {
        MockClockPerThread::with(|clock| match &mut clock.mock {
            Some(clock) => {
                clock.utc_list.push_back(mock_date);
            }
            None => {
                panic!("Use MockClockGuard in your test");
            }
        });
    }

    /// Adds timestamp to queue, it will be returned in `Self::utc()`.
    pub fn add_instant(&self, mock_date: Instant) {
        MockClockPerThread::with(|clock| match &mut clock.mock {
            Some(clock) => {
                clock.instant_list.push_back(mock_date);
            }
            None => {
                panic!("Use MockClockGuard in your test");
            }
        });
    }

    /// Returns number of calls  to `Self::utc` since `Self::mock()` was called.
    pub fn utc_call_count(&self) -> u64 {
        MockClockPerThread::with(|clock| match &mut clock.mock {
            Some(clock) => clock.utc_call_count,
            None => {
                panic!("Use MockClockGuard in your test");
            }
        })
    }

    /// Returns number of calls  to `Self::instant` since `Self::mock()` was called.
    pub fn instant_call_count(&self) -> u64 {
        MockClockPerThread::with(|clock| match &mut clock.mock {
            Some(clock) => clock.instant_call_count,
            None => {
                panic!("Use MockClockGuard in your test");
            }
        })
    }
}
=======
    pub struct MockClockGuard {}

    impl Default for MockClockGuard {
        fn default() -> Self {
            Clock::set_mock();
            Self {}
        }
    }

    impl Drop for MockClockGuard {
        fn drop(&mut self) {
            Clock::reset();
        }
    }

    impl Clock {
        fn set_mock() {
            MockClockPerThread::with(|clock| {
                clock.is_mock = true;
            });
        }
        fn reset() {
            MockClockPerThread::with(|clock| {
                clock.reset();
            });
        }
        pub fn add_utc(mock_date: DateTime<chrono::Utc>) {
            MockClockPerThread::with(|clock| {
                if clock.is_mock {
                    clock.utc.push_back(mock_date);
                } else {
                    panic!("Use MockClockGuard in your test");
                }
            });
        }

        pub fn add_instant(mock_instant: Duration) {
            MockClockPerThread::with(|clock| {
                if clock.is_mock {
                    clock.durations.push_back(mock_instant);
                } else {
                    panic!("Use MockClockGuard in your test");
                }
            });
        }

        pub fn utc() -> DateTime<chrono::Utc> {
            MockClockPerThread::with(|clock| {
                if clock.is_mock {
                    let x = clock.pop_utc();
                    match x {
                        Some(t) => t,
                        None => {
                            panic!("Mock clock run out of samples");
                        }
                    }
                } else {
                    chrono::Utc::now()
                }
            })
        }

        pub fn instant() -> Instant {
            MockClockPerThread::with(|clock| {
                if clock.is_mock {
                    let x = clock.pop_instant();
                    match x {
                        Some(t) => t,
                        None => {
                            panic!("Mock clock run out of samples");
                        }
                    }
                } else {
                    Instant::now()
                }
            })
        }

        pub fn now() -> Time {
            Clock::utc().into()
        }

        pub fn instant_call_count() -> u64 {
            MockClockPerThread::with(|clock| clock.instant_call_count)
        }

        pub fn utc_call_count() -> u64 {
            MockClockPerThread::with(|clock| clock.utc_call_count)
        }
    }

    #[cfg(test)]
    mod tests {
        // TODO(#5345) Add tests for mock
        use super::*;
>>>>>>> Reorganize imports in near-network

        #[test]
        fn test_mock() {
            Clock::set_mock();
            let utc = Utc::now();

            Clock::add_utc(utc);

            assert_eq!(Clock::utc(), utc);
        }
    }
}

mod time {
    use crate::time::{Clock, Duration, Utc};
    use borsh::{BorshDeserialize, BorshSerialize};
    use chrono::DateTime;
    use std::ops::{Add, Sub};
    use std::time::SystemTime;
    pub type UnixTime = u64;

    #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Time {
        system_time: SystemTime,
    }

<<<<<<< HEAD
/// We switched to using `Clock` in production for time mocking.
/// You are supposed to use `Clock` to get current time.
pub struct Clock {}

impl Clock {
    /// Turns the mocking logic on.
    fn set_mock() {
        MockClockPerThread::with(|clock| {
            assert!(clock.mock.is_none());
            clock.mock = Some(MockClockPerState::default())
        })
    }

    /// Resets mocks to default state.
    fn reset() {
        MockClockPerThread::with(|clock| clock.mock = None);
    }

    /// This methods gets current time as `std::Instant`
    /// unless it's mocked, then returns time added by `Self::add_utc(...)`
    pub fn instant() -> Instant {
        MockClockPerThread::with(|clock| match &mut clock.mock {
            Some(clock) => {
                clock.instant_call_count += 1;
                let x = clock.instant_list.pop_front();
                match x {
                    Some(t) => t,
                    None => {
                        panic!("Mock clock run out of samples");
                    }
                }
            }
            None => Instant::now(),
        })
    }

    /// This methods gets current time as `std::Instant`
    /// unless it's mocked, then returns time added by `Self::add_instant(...)`
    pub fn utc() -> DateTime<chrono::Utc> {
        MockClockPerThread::with(|clock| match &mut clock.mock {
            Some(clock) => {
                clock.utc_call_count += 1;
                let x = clock.utc_list.pop_front();
                match x {
                    Some(t) => t,
                    None => {
                        panic!("Mock clock run out of samples");
                    }
                }
            }
            None => chrono::Utc::now(),
        })
=======
    impl BorshSerialize for Time {
        fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
            let nanos = self.to_unix_timestamp() as u64;
            BorshSerialize::serialize(&nanos, writer).unwrap();
            Ok(())
        }
    }

    impl BorshDeserialize for Time {
        fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
            let nanos: u64 = borsh::BorshDeserialize::deserialize(buf)?;

            Ok(Time::from_unix_timestamp(nanos))
        }
    }

    impl From<SystemTime> for Time {
        fn from(system_time: SystemTime) -> Self {
            Self { system_time }
        }
    }

    impl From<DateTime<Utc>> for Time {
        fn from(utc: DateTime<Utc>) -> Self {
            // utc.timestamp_nanos() returns i64
            let nanos = utc.timestamp_nanos() as u64;

            Self::UNIX_EPOCH + Duration::from_nanos(nanos)
        }
>>>>>>> Reorganize imports in near-network
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Add;
    use std::thread;
    use std::thread::sleep;

    #[test]
    #[should_panic]
    fn test_clock_panic_utc() {
        let _mock_clock_guard = MockClockGuard::default();
        Clock::utc();
    }

    #[test]
    #[should_panic]
    fn test_clock_panic_instant() {
        let _mock_clock_guard = MockClockGuard::default();
        Clock::instant();
    }

    #[test]
    #[should_panic]
    fn test_two_guards() {
        let mock_clock_guard1 = MockClockGuard::default();
        let mock_clock_guard2 = MockClockGuard::default();
        assert_eq!(mock_clock_guard1.instant_call_count(), 0);
        assert_eq!(mock_clock_guard2.instant_call_count(), 0);
    }

<<<<<<< HEAD
    #[test]
    fn test_clock_utc() {
        let mock_clock_guard = MockClockGuard::default();

        let utc_now = Utc::now();
        mock_clock_guard.add_utc(
            utc_now
                .checked_add_signed(chrono::Duration::from_std(Duration::from_secs(1)).unwrap())
                .unwrap(),
        );
        mock_clock_guard.add_utc(
            utc_now
                .checked_add_signed(chrono::Duration::from_std(Duration::from_secs(2)).unwrap())
                .unwrap(),
        );
        mock_clock_guard.add_utc(
            utc_now
                .checked_add_signed(chrono::Duration::from_std(Duration::from_secs(3)).unwrap())
                .unwrap(),
        );
        assert_eq!(
            Clock::utc(),
            utc_now
                .checked_add_signed(chrono::Duration::from_std(Duration::from_secs(1)).unwrap())
                .unwrap(),
        );
        assert_eq!(
            Clock::utc(),
            utc_now
                .checked_add_signed(chrono::Duration::from_std(Duration::from_secs(2)).unwrap())
                .unwrap(),
        );
        assert_eq!(
            Clock::utc(),
            utc_now
                .checked_add_signed(chrono::Duration::from_std(Duration::from_secs(3)).unwrap())
                .unwrap(),
        );

        assert_eq!(mock_clock_guard.utc_call_count(), 3);
        drop(mock_clock_guard);

        let mock_clock_guard = MockClockGuard::default();
        assert_eq!(mock_clock_guard.utc_call_count(), 0);
    }

    #[test]
    fn test_clock_instant() {
        let mock_clock_guard = MockClockGuard::default();

        let instant_now = Instant::now();
        mock_clock_guard.add_instant(instant_now.add(Duration::from_secs(1)));
        mock_clock_guard.add_instant(instant_now.add(Duration::from_secs(2)));
        mock_clock_guard.add_instant(instant_now.add(Duration::from_secs(3)));
        assert_eq!(Clock::instant(), instant_now.add(Duration::from_secs(1)));
        assert_eq!(Clock::instant(), instant_now.add(Duration::from_secs(2)));
        assert_eq!(Clock::instant(), instant_now.add(Duration::from_secs(3)));

        assert_eq!(mock_clock_guard.instant_call_count(), 3);
        drop(mock_clock_guard);

        let mock_clock_guard = MockClockGuard::default();
        assert_eq!(mock_clock_guard.instant_call_count(), 0);
    }

    #[test]
    fn test_threading() {
        thread::spawn(|| {
            for _i in 1..10 {
                // This would panic if every thread used the same mocking.
                let mock_clock_guard = MockClockGuard::default();
                sleep(Duration::from_millis(100));
                assert_eq!(mock_clock_guard.instant_call_count(), 0);
            }
        });
=======
    impl Time {
        pub const UNIX_EPOCH: Time = Time { system_time: SystemTime::UNIX_EPOCH };

        pub fn now() -> Self {
            Self::UNIX_EPOCH + Duration::from_nanos(Clock::utc().timestamp_nanos() as u64)
        }

        pub fn duration_since(&self, rhs: &Self) -> Duration {
            self.system_time.duration_since(rhs.system_time).unwrap_or(Duration::from_millis(0))
        }

        pub fn saturating_duration_since(&self, rhs: &Self) -> Duration {
            self.system_time.duration_since(rhs.system_time).unwrap_or(Duration::from_millis(0))
        }

        pub fn elapsed(&self) -> Duration {
            Self::now().duration_since(self)
        }

        pub fn from_unix_timestamp(unix_time: UnixTime) -> Self {
            Self::UNIX_EPOCH + Duration::from_nanos(unix_time)
        }

        pub fn to_unix_timestamp(&self) -> UnixTime {
            // doesn't truncate, because self::UNIX_EPOCH is 0
            self.duration_since(&Self::UNIX_EPOCH).as_nanos() as UnixTime
        }

        pub fn inner(self) -> SystemTime {
            self.system_time
        }
    }

    impl Add<Duration> for Time {
        type Output = Self;

        fn add(self, other: Duration) -> Self {
            Self { system_time: self.system_time + other }
        }
    }

    impl Sub for Time {
        type Output = Duration;

        fn sub(self, other: Self) -> Self::Output {
            self.system_time.duration_since(other.system_time).unwrap_or(Duration::from_millis(0))
        }
    }

    impl Sub<Duration> for Time {
        type Output = Time;

        fn sub(self, other: Duration) -> Self::Output {
            Time {
                system_time: self.system_time.checked_sub(other).unwrap_or(SystemTime::UNIX_EPOCH),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use borsh::{BorshDeserialize, BorshSerialize};

        #[test]
        fn test_operator() {
            let now_st = SystemTime::now();

            let now_nc: Time = now_st.into();

            let t_nc = now_nc + Duration::from_nanos(123456);
            let t_st = now_st + Duration::from_nanos(123456);

            assert_eq!(t_nc.inner(), t_st);
        }

        #[test]
        fn test_borsh() {
            let now_nc = Time::now();

            let mut v = Vec::new();
            BorshSerialize::serialize(&now_nc, &mut v).unwrap();

            let v2: &mut &[u8] = &mut v.as_slice();

            let now2: Time = BorshDeserialize::deserialize(v2).unwrap();
            assert_eq!(now_nc, now2);
        }
>>>>>>> Reorganize imports in near-network
    }
}
