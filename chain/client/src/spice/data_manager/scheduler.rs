use super::Lane;
use near_async::time::{Duration, Instant};
use rand::Rng;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Retry and request timing parameters.
#[derive(Debug, Clone)]
pub(crate) struct TimingConfig {
    /// How long a pull request may go unanswered before it counts as timed out.
    pub(crate) request_timeout: Duration,
    /// First interval of the retry ladder.
    pub(crate) backoff_base: Duration,
    /// Geometric growth factor of the retry periods.
    pub(crate) backoff_multiplier: u32,
    /// Upper bound the retry period is capped at.
    pub(crate) backoff_cap: Duration,
    /// Every interval is jittered by up to this fraction in either direction.
    pub(crate) jitter_frac: f64,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::seconds(1),
            backoff_base: Duration::milliseconds(200),
            backoff_multiplier: 2,
            backoff_cap: Duration::seconds(2),
            jitter_frac: 0.25,
        }
    }
}

/// Keeps track of retry progress.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    retries: u32,
}

impl Backoff {
    /// Next retry interval, jittered
    pub(crate) fn next_interval(&self, config: &TimingConfig, rng: &mut impl Rng) -> Duration {
        let unjittered = (config.backoff_base.as_seconds_f64()
            * f64::from(config.backoff_multiplier).powf(f64::from(self.retries)))
        .min(config.backoff_cap.as_seconds_f64());
        let jitter = rng.gen_range(-config.jitter_frac..=config.jitter_frac);
        Duration::seconds_f64(unjittered * (1.0 + jitter))
    }

    pub(crate) fn note_retry(&mut self) {
        self.retries = self.retries.saturating_add(1);
    }

    #[cfg(test)]
    pub(crate) fn retries(&self) -> u32 {
        self.retries
    }
}

/// A queued wake-up for `key`.
#[derive(Debug)]
struct Deadline<K> {
    at: Instant,
    lane: Lane,
    key: K,
}

// Max-heap order: earliest `at` on top, `Priority` before `Background` for the same instant.
// The key does not participate.
impl<K> Ord for Deadline<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.at.cmp(&self.at).then_with(|| self.lane.cmp(&other.lane))
    }
}

impl<K> PartialOrd for Deadline<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> PartialEq for Deadline<K> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<K> Eq for Deadline<K> {}

/// Instant the caller must call [`DeadlineScheduler::pop_due`] at; `None` when a wake-up
/// already reported covers the earliest entry.
#[must_use = "an unreported wake-up is never processed"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WakeAt(pub(crate) Option<Instant>);

/// Wake-ups for keyed items, earliest first.
/// Scheduling a key again does not cancel its earlier wake-up; a superseded wake-up still pops, and the
/// caller can tell it apart by its instant no longer matching the item's `next_deadline`.
pub(crate) struct DeadlineScheduler<K> {
    heap: BinaryHeap<Deadline<K>>,
    /// The earliest entry's instant as last reported by [`Self::take_wake`], until it pops.
    reported: Option<Instant>,
}

impl<K> Default for DeadlineScheduler<K> {
    fn default() -> Self {
        Self { heap: BinaryHeap::new(), reported: None }
    }
}

impl<K> DeadlineScheduler<K> {
    pub(crate) fn schedule(&mut self, key: K, at: Instant, lane: Lane) {
        self.heap.push(Deadline { at, lane, key });
    }

    /// The earliest entry's instant if it is not covered by the last reported one: it was
    /// scheduled ahead of it, or the reported one has popped. Stale entries included.
    pub(crate) fn take_wake(&mut self) -> WakeAt {
        let Some(front) = self.heap.peek().map(|deadline| deadline.at) else {
            return WakeAt(None);
        };
        if self.reported.is_some_and(|reported| reported <= front) {
            return WakeAt(None);
        }
        self.reported = Some(front);
        WakeAt(Some(front))
    }

    /// Pops every entry due at or before `now`, paired with the instant it was scheduled for.
    pub(crate) fn pop_due(&mut self, now: Instant) -> Vec<(K, Instant)> {
        if self.reported.is_some_and(|reported| reported <= now) {
            self.reported = None;
        }
        let mut due = Vec::new();
        while self.heap.peek().is_some_and(|deadline| deadline.at <= now) {
            let deadline = self.heap.pop().unwrap();
            due.push((deadline.key, deadline.at));
        }
        due
    }
}
