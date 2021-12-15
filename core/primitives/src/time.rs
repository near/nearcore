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
