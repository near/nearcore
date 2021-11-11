use std::default::Default;

use chrono;

pub use chrono::Utc;
pub use std::time::{Duration, Instant};

use chrono::DateTime;
use std::cell::RefCell;
use std::collections::VecDeque;

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
    }
}

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
    pub fn set_mock() {
        MockClockPerThread::with(|clock| {
            clock.is_mock = true;
        });
    }
    pub fn reset() {
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

    pub fn instant_call_count() -> u64 {
        MockClockPerThread::with(|clock| clock.instant_call_count)
    }

    pub fn utc_call_count() -> u64 {
        MockClockPerThread::with(|clock| clock.utc_call_count)
    }
}
