//! SKETCH. Per-item deadline scheduler + recovery timing. Replaces
//! the single global 1 s ticker (`schedule_data_fetching`). Most "timing" is actually
//! event-gated: *arming* is a condition (existence gate), *expiry* is head-driven.

use super::QosClass;
use super::item::DataId;
use near_async::time::{Duration, Instant};
use std::collections::BinaryHeap;

/// Tuning knobs, expressed relative to block time `T_b` and push RTT `T_rtt`; all
/// configurable (`data_distributor_actor.rs:935`). Defaults are *starting points* to
/// be validated under load (Spice-under-load / benchmark CI). Reputation decay knobs
/// live in [`super::reputation::ReputationConfig`].
#[derive(Debug, Clone)]
pub(crate) struct TimingConfig {
    /// After a unit *should* exist, wait this long for straggler pushes before pulling.
    /// Default ~1–2·T_rtt (≈200 ms). Matters beyond politeness: premature pulls
    /// during execution lag time out against innocent producers and pollute the
    /// liveness channel.
    pub(crate) push_grace: Duration,
    /// After the first unit arrives, wait this long for the rest before pulling missing
    /// ordinals. Default ~1–2·T_rtt (≈200 ms).
    pub(crate) first_unit_pull_delay: Duration,
    /// How long an outstanding `in_flight` request may go unanswered before it converts
    /// into `note_timeout(who)` + removal. Distinct from backoff: backoff says when to
    /// *retry the item*, this says when a *peer* counts as having failed to provide.
    /// Default ~1·T_b (≈1 s).
    pub(crate) request_timeout: Duration,
    /// Retry backoff: `base`, geometric `multiplier`, `cap`, and ± jitter fraction.
    pub(crate) backoff_base: Duration, // ≈ T_rtt
    pub(crate) backoff_multiplier: u32, // 2
    pub(crate) backoff_cap: Duration,   // ≈ 1–2·T_b (≈2 s)
    /// ± jitter fraction, decorrelating items and nodes. NOTE: the jitter rng (like
    /// the clock) must be injected at construction, not sampled from thread rng —
    /// test-loop tests need deterministic schedules.
    pub(crate) jitter_frac: f64, // 0.25
    /// On decode-timeout, contact this many producers via `select_sources` (weighted
    /// sampling, excluding peers with an in-flight request), EACH asked for the same
    /// explicit full missing-ordinal set (any producer holds the full data). Default 2–3.
    pub(crate) escalation_fanout: u8,
    /// Coarse backstop scan for anything the event path missed (insurance, not primary).
    /// Default ~5·T_b (≈5 s).
    pub(crate) safety_sweep: Duration,
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
            safety_sweep: Duration::seconds(5),
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

// Ordered so `BinaryHeap` (a max-heap) yields the *earliest* deadline first, and within
// the same instant the higher-priority (`Priority`) lane first. Both comparisons are
// deliberately flipped (`other` on the left): earliest `at` and *lowest* `QosClass`
// discriminant (`Priority < Background`) must compare as the heap maximum.
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

/// Per-item retry-ladder position, advanced on each miss (escalate first, then back
/// off) — and only that: one-shot timing anchors (`FetchItem::first_unit_at`) and the
/// armed deadline (`next_deadline`) are separate fields, because they are immutable
/// facts while this is a progression (advances on miss; plausibly resets on progress
/// — new parts arriving are evidence the pipe works; exact reset rule TBD).
/// Lives on the `FetchItem` — the single copy; the scheduler owns only deadlines.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    attempts: u32,
}

impl Backoff {
    /// Next interval = `min(cap, base * multiplier^attempts)` with ± jitter.
    pub(crate) fn next_interval(&self, _cfg: &TimingConfig) -> Duration {
        Duration::milliseconds(0) // sketch
    }
}

/// Min-heap of deadlines across all items and both QoS lanes; O(log n) per op, O(due)
/// work per wake-up (vs O(all waiting) every second today).
#[derive(Default)]
pub(crate) struct DeadlineScheduler {
    heap: BinaryHeap<Deadline>,
}

impl DeadlineScheduler {
    pub(crate) fn arm(&mut self, _id: DataId, _at: Instant, _qos: QosClass) {}

    /// Pop everything due at/before `now`. Heap entries cannot be removed, so
    /// completed/expired/re-armed items leave stale entries behind — the engine MUST
    /// lazily validate each popped id (item still exists, still `Collecting`, and the
    /// popped `at` equals the item's `next_deadline`) and discard mismatches;
    /// otherwise every completion is followed by a spurious pull.
    pub(crate) fn drain_due(&mut self, _now: Instant) -> Vec<DataId> {
        Vec::new() // sketch
    }
}
