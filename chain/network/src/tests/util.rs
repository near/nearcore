pub type Instant = chrono::DateTime<chrono::Utc>;
pub type Duration = chrono::Duration;

pub trait Clock {
    fn now(&self) -> Instant;
}

pub struct RealClock {}

impl Clock for RealClock {
    fn now(&self) -> Instant {
        chrono::Utc::now()
    }
}

pub struct FakeClock {
    now: Instant,
}

impl FakeClock {
    pub fn new(now: Instant) -> FakeClock {
        FakeClock { now }
    }
    pub fn advance(&mut self, d: Duration) {
        assert!(d >= Duration::zero());
        self.now = self.now + d;
    }
}

impl Default for FakeClock {
    fn default() -> FakeClock {
        Self::new(chrono::DateTime::from_utc(
            chrono::NaiveDateTime::from_timestamp(89108233, 0),
            chrono::Utc,
        ))
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        self.now
    }
}

pub fn make_rng(seed: u64) -> rand_pcg::Pcg32 {
    rand_pcg::Pcg32::new(seed, 0xa02bdbf7bb3c0a7)
}
