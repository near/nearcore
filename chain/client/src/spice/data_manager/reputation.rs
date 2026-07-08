//! SKETCH. Two-layer accountability + pull source selection.
//! A SPICE-internal reputation table ranks pull sources AND exports its signals to the
//! network peer scorer. "SPICE informs; the network enforces banning."
//!
//! Selection is reputation-driven: there is NO per-item source bookkeeping (the item
//! keeps only its transient `in_flight` plumbing). Failure memory is shared across
//! items — a dead producer is learned once, not re-learned by every waiting item — and
//! pool exhaustion needs no reset rule: ranking among all-recently-failed peers
//! naturally prefers the least-recently-failed one.

use super::item::DataId;
use near_async::time::{Duration, Instant};
use near_primitives::types::AccountId;
use std::collections::HashMap;

/// Attributable faults funnelled here. Distribution-level faults are detected in the
/// engine; semantic faults are reported by consumer actors via `FailedEvent`.
#[derive(Debug, Clone)]
pub(crate) enum Misbehavior {
    /// A part failed its Merkle proof against the commitment root. (Direct: the sender.)
    BadMerkleProof,
    /// Reassembled bytes don't match the commitment hash. (That commitment's vouchers —
    /// per-commitment sender map, not everyone who ever sent a part for the item.)
    DecodeGarbage,
    /// A content-addressed blob's bytes don't hash to the requested code hash.
    BadCodeBytes,
    /// Receipt proof failed the source `outgoing_receipts_root` check. Verified late
    /// (once execution results land) — attribution needs the retained sender map.
    InvalidReceiptProof,
    /// Contract accesses inconsistent with the witness's `contract_accesses_hash`.
    AccessesInconsistent,
    /// Denied/withheld data it should hold: direct if it earlier pushed a part (signed
    /// contradiction), else retroactive once certification proves the data existed.
    /// A signed `NotAvailable` NAK later contradicted by certification lands here.
    DeniedHeldData,
    /// Our endorsed execution result ≠ the certified one: the witness executed cleanly
    /// but was built on rotten inputs (locally undetectable — endorsing is
    /// execute-and-sign, not a verdict). Detected by the certification comparator;
    /// attributed via the attribution retained until expiry. May equally indicate a
    /// local bug, so the response is reputation + telemetry only — never anything
    /// automatic against our own signing.
    CertifiedResultMismatch,
    /// Oversized / out-of-window / unsolicited data (from the admission gate).
    ProtocolViolation,
}

impl Misbehavior {
    /// Relative penalty weight; tuned later.
    pub(crate) fn weight(&self) -> f64 {
        0.0 // sketch
    }
}

/// Decay/clamp knobs. Two channels, two time constants — conflating them was the
/// original sin of a single `value` score.
#[derive(Debug, Clone)]
pub(crate) struct ReputationConfig {
    /// Slow channel (honesty) half-life, ~hours. A forgiveness horizon for pull
    /// selection only — enforcement (network export) fires at report time, pre-decay.
    pub(crate) score_halflife: Duration,
    /// Fast channel (responsiveness) half-life, ~seconds (order of `backoff_cap`) —
    /// a timeout must be cheap and quickly forgiven.
    pub(crate) load_halflife: Duration,
    /// Floor for `score`, bounding worst-case recovery to a few half-lives (an honest
    /// peer hit by a long-lived bug must not be exiled for days). Ceiling is 0: no
    /// positive reinforcement — credit could be banked and spent on misbehavior, and
    /// clearing timeout load on success would whitewash a flapping peer.
    pub(crate) score_floor: f64,
}

/// Per-producer score. Both channels decay lazily from one shared anchor (exponential
/// decay composes: `decay(decay(x, t1), t2) = decay(x, t1 + t2)`), so there is no
/// background sweep and untouched entries cost nothing.
#[derive(Debug, Clone)]
pub(crate) struct PeerScore {
    /// Slow channel: honesty. Misbehavior penalties weighted by kind; decays toward 0
    /// with `score_halflife`; clamped to `[score_floor, 0]`.
    score: f64,
    /// Fast channel: responsiveness. `+1` per timeout, decays with `load_halflife`.
    /// A decayed accumulator (EWMA), NOT a monotonic count — see [`PeerScore::touch`]:
    /// 100 ancient timeouts + 1 fresh ≈ load 1; 100 fresh ≈ load 100.
    timeout_load: f64,
    /// Shared decay anchor: when both channels were last normalized.
    last_update_at: Instant,
}

impl PeerScore {
    /// Write path: normalize both channels to `now` (decay-then-bump — stale history
    /// melts BEFORE the new event lands), then the call site applies its delta.
    fn touch(&mut self, _now: Instant, _cfg: &ReputationConfig) {}

    /// Read path: pure — effective `(score, timeout_load)` at `now`, no mutation, so
    /// ranking keeps `&self` and the hot path never writes.
    fn effective(&self, _now: Instant, _cfg: &ReputationConfig) -> (f64, f64) {
        (0.0, 0.0) // sketch
    }
}

/// SPICE-internal reputation, keyed by producer `AccountId`. Sparse: only peers with
/// something on their record have entries — a missing entry reads as neutral-and-live —
/// so the honest, responsive common case allocates nothing.
pub(crate) struct Reputation {
    scores: HashMap<AccountId, PeerScore>,
    config: ReputationConfig,
    // exporter: NetworkPeerScoreExporter — the account→peer bridge (open design seam).
}

impl Reputation {
    /// Misbehavior funnel: `touch` + `score -= weight`, then export to the network
    /// scorer (enforcement is the export at report time; decay never weakens it).
    /// Call sites: engine-immediate (Merkle / decode-garbage / code bytes / admission
    /// violations), consumer `FailedEvent` (attributed via the retained sender map), and
    /// retroactive denial (via certification — may arrive long after the request;
    /// `touch` normalizes from whatever the anchor was, that's the point).
    pub(crate) fn report(&mut self, _who: &[AccountId], _what: Misbehavior, _about: &DataId) {}

    /// Timeout funnel: `touch` + `timeout_load += 1`. The ONLY other write path.
    /// Deliberate no-ops everywhere else: successful responses and NAKs carry no
    /// score effect (see `ReputationConfig::score_floor` docs), reads never write,
    /// epoch switch only deletes.
    ///
    /// Possible extension (not decided): when an item's push window closes, producers
    /// whose own pushed part never arrived are weak evidence of slow/offline — a
    /// fractional load bump here would steer even the FIRST pull away from them.
    /// Needs care not to punish mere push packet loss; revisit with load-test data.
    pub(crate) fn note_timeout(&mut self, _who: &AccountId, _now: Instant) {}

    /// Weighted-sample `n` sources from `pool` by effective score (slow ⊕ fast),
    /// excluding `outstanding` (peers with an in-flight request for this item).
    /// Sampling, not argmax: deterministic ranking herds every recovering node onto
    /// the same best producer.
    ///
    /// Cost: a stateless O(P) fold (lookup, decay-evaluate, weigh, sample) with
    /// P ≈ tens — recovery-path only. No ordered structure fits anyway: the two
    /// half-lives make the *combined* order time-varying (a single exponential channel
    /// admits the static key `ln(x) + t/τ`, the mixture does not), and sampling needs
    /// the full weight vector regardless. If profiling ever cares, memoize the weight
    /// vector per (pool, ~100 ms bucket) — don't build a heap.
    pub(crate) fn select_sources(
        &self,
        pool: &[AccountId],
        _outstanding: &[AccountId],
        _n: usize,
        _now: Instant,
    ) -> Vec<AccountId> {
        pool.to_vec() // sketch
    }

    /// Epoch GC — the real end-of-life (decay only handles forgiveness within an
    /// account's tenure): drop entries for accounts no longer in any relevant producer
    /// set, bounding the table at ~pool size.
    pub(crate) fn on_epoch_switch(&mut self, _still_relevant: &dyn Fn(&AccountId) -> bool) {}
}

/// The open seam: SPICE reputation is per-`AccountId`, network scoring is
/// per-`PeerId`/connection. This maps account→peer (Tier1 mapping) and pushes signals
/// into the network peer scorer, which owns actual banning. A producer with no
/// mappable connection makes `export` a silent no-op — the internal table must remain
/// the primary defense.
pub(crate) trait NetworkPeerScoreExporter {
    fn export(&self, account: &AccountId, what: &Misbehavior);
}
