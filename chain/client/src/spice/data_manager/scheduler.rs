//! SKETCH. Per-item deadline scheduler + recovery timing — not a single global ticker.
//! Most "timing" is event-gated: arming is a condition (the existence gate), expiry is
//! head-driven. Fetch-side only — produce items are reactive and never arm deadlines.

use super::QosClass;
use super::item::DataId;
use near_async::time::{Duration, Instant};
use near_primitives::types::BlockHeightDelta;
use std::collections::BinaryHeap;

/// Tuning knobs, relative to block time `T_b` and push RTT `T_rtt`; all configurable.
/// Defaults are starting points to validate under load. Reputation decay knobs live in
/// [`super::reputation::ReputationConfig`].
#[derive(Debug, Clone)]
pub(crate) struct TimingConfig {
    /// After a unit should exist, wait this long for straggler pushes before pulling
    /// (~1–2·T_rtt). Premature pulls during execution lag time out against innocent
    /// producers and pollute the liveness channel.
    pub(crate) push_grace: Duration,
    /// After the first unit arrives, wait this long for the rest before pulling missing
    /// ordinals (~1–2·T_rtt).
    pub(crate) first_unit_pull_delay: Duration,
    /// How long an in_flight request may go unanswered before it converts into
    /// `note_timeout(who)` + removal. Distinct from backoff (when to retry the *item*):
    /// enforced via the arming rule (see `on_deadline`), so detection never waits out a
    /// longer backoff interval. Default ~1·T_b.
    pub(crate) request_timeout: Duration,
    /// Retry backoff: `base`, geometric `multiplier`, `cap`, and ± jitter fraction.
    pub(crate) backoff_base: Duration, // ≈ T_rtt
    pub(crate) backoff_multiplier: u32, // 2
    pub(crate) backoff_cap: Duration,   // ≈ 1–2·T_b (≈2 s)
    /// ± jitter fraction, decorrelating items and nodes. The rng (like the clock) is
    /// injected at construction for deterministic test schedules.
    pub(crate) jitter_frac: f64, // 0.25
    /// On decode-timeout, contact this many producers (weighted sampling, excluding
    /// in-flight peers) and stripe the missing ordinals disjointly across them — the
    /// union covers the hole once, not the full set to each. At least 3: the full missing
    /// set is `~1/0.6` redundant, so losing one of three stripes still leaves enough parts
    /// to decode the same round, while two stripes would not. Default 3.
    pub(crate) escalation_fanout: u8,
    /// How many heights above a shard's certified frontier we speculatively pull a
    /// witness for. Kept small: too large pulls not-yet-executed heights and times out
    /// against innocent producers; too small just waits for the push. See
    /// [`super::SpiceDataManager::on_heads_advanced`].
    pub(crate) witness_pull_margin: BlockHeightDelta,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            push_grace: Duration::milliseconds(200),
            first_unit_pull_delay: Duration::milliseconds(200),
            request_timeout: Duration::seconds(1),
            backoff_base: Duration::milliseconds(200),
            backoff_multiplier: 2,
            backoff_cap: Duration::seconds(2),
            jitter_frac: 0.25,
            escalation_fanout: 3,
            witness_pull_margin: 2,
        }
    }
}

/// A queued wake-up. The engine pops due entries and calls `on_deadline(id)`.
#[derive(Debug, PartialEq, Eq)]
struct Deadline {
    at: Instant,
    id: DataId,
    qos: QosClass,
}

// Ordered so `BinaryHeap` (a max-heap) yields the earliest deadline first, and within one
// instant the higher-priority (`Priority`) lane first. Both comparisons are flipped
// (`other` on the left): earliest `at` and lowest `QosClass` discriminant must be the max.
impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.at.cmp(&self.at).then_with(|| other.qos.cmp(&self.qos))
    }
}
impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-item retry-ladder position, advanced on each retry *decision* (escalate first, then
/// back off) — never by a timeout-triggered wake (see `on_deadline`). Separate from the
/// one-shot timing anchors (`FetchItem::first_unit_at`) and the armed deadline
/// (`next_deadline`): those are facts, this is a progression (advances on miss; reset on
/// progress — exact rule TBD). Lives on the `FetchItem`; the scheduler owns only deadlines.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    attempts: u32,
}

impl Backoff {
    /// Next interval = `min(cap, base · multiplier^attempts)` with ± jitter.
    pub(crate) fn next_interval(&self, _cfg: &TimingConfig) -> Duration {
        Duration::milliseconds(0) // sketch
    }
}

/// Min-heap of deadlines across all items and both lanes; O(log n) per op, O(due) per
/// wake-up (vs O(all waiting) every second today).
#[derive(Default)]
pub(crate) struct DeadlineScheduler {
    heap: BinaryHeap<Deadline>,
}

impl DeadlineScheduler {
    pub(crate) fn arm(&mut self, _id: DataId, _at: Instant, _qos: QosClass) {}

    /// Pop everything due at/before `now`. Heap entries can't be removed, so completed/
    /// expired/re-armed items leave stale entries behind — the engine must validate each
    /// popped id (still exists, still `Collecting`, popped `at` == the item's
    /// `next_deadline`) and discard mismatches, else every completion triggers a spurious
    /// pull.
    pub(crate) fn drain_due(&mut self, _now: Instant) -> Vec<DataId> {
        Vec::new() // sketch
    }
}
