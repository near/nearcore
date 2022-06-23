/// Provides structs used for getting time.
/// TODO: this module is deprecated and scheduled to be replaced with
/// chain/network-primitives/time.
///
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
pub use time::Time;

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
        }
        INSTANCE.with(|it| f(&mut *it.borrow_mut()))
    }
}

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
    }
}

mod time {
    use crate::time::{Clock, Duration, Utc};
    use borsh::{BorshDeserialize, BorshSerialize};
    use chrono::DateTime;
    use std::ops::{Add, Sub};
    use std::time::SystemTime;

    #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Time {
        system_time: SystemTime,
    }

    impl BorshSerialize for Time {
        fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
            let nanos = self.to_unix_timestamp_nanos().as_nanos() as u64;
            BorshSerialize::serialize(&nanos, writer).unwrap();
            Ok(())
        }
    }

    impl BorshDeserialize for Time {
        fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
            let nanos: u64 = borsh::BorshDeserialize::deserialize(buf)?;

            Ok(Time::from_unix_timestamp(Duration::from_nanos(nanos)))
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
    }

    impl Time {
        pub const UNIX_EPOCH: Time = Time { system_time: SystemTime::UNIX_EPOCH };

        pub fn now() -> Self {
            Self::UNIX_EPOCH + Duration::from_nanos(Clock::utc().timestamp_nanos() as u64)
        }

        pub fn duration_since(&self, rhs: &Self) -> Duration {
            self.system_time.duration_since(rhs.system_time).unwrap_or(Duration::from_millis(0))
        }

        pub fn elapsed(&self) -> Duration {
            Self::now().duration_since(self)
        }

        pub fn from_unix_timestamp(duration: Duration) -> Self {
            Self::UNIX_EPOCH + duration
        }

        pub fn to_unix_timestamp_nanos(&self) -> Duration {
            // doesn't truncate, because self::UNIX_EPOCH is 0
            self.duration_since(&Self::UNIX_EPOCH)
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
    }
}
