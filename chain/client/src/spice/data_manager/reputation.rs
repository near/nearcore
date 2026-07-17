//! SKETCH. Two-layer accountability + pull source selection. A SPICE-internal
//! reputation table ranks pull sources and exports its signals to the network peer
//! scorer — SPICE informs, the network enforces bans.
//!
//! Selection is reputation-driven with no per-item source bookkeeping: failure memory
//! is global, so a dead producer is learned once, not once per waiting item.

use super::item::DataId;
use near_async::time::{Duration, Instant};
use near_primitives::types::AccountId;
use std::collections::HashMap;

/// Attributable faults. Distribution-level ones are detected in the engine; semantic
/// ones come from consumer actors via `FailedEvent`.
#[derive(Debug, Clone)]
pub(crate) enum Misbehavior {
    /// Part failed its Merkle proof against the commitment root. Blame the sender.
    BadMerkleProof,
    /// Reassembled bytes don't match the commitment hash. Blame that commitment's
    /// senders — not everyone who sent a part for the item.
    DecodeGarbage,
    /// A blob's bytes don't hash to the requested code hash.
    BadCodeBytes,
    /// Receipt proof failed the source `outgoing_receipts_root` check. Verified late
    /// (once execution results land), so attribution needs the retained sender map.
    InvalidReceiptProof,
    /// Accesses message disagrees with the witness's embedded accesses list.
    AccessesInconsistent,
    /// NAKed an item it earlier pushed a signed part of — the only attributable denial.
    /// Any other NAK is penalty-free (a lagging producer honestly answers "not yet");
    /// suspected withholding is telemetry only.
    DeniedHeldData,
    /// Our endorsed result ≠ the certified one: the witness executed cleanly on rotten
    /// inputs (locally undetectable, since endorsing is execute-and-sign). May also be a
    /// local bug, so the response is reputation + telemetry only.
    CertifiedResultMismatch,
    /// Sent parts under two different commitments for one item.
    ConflictingCommitment,
    /// Oversized / out-of-window / foreign-ordinal / unsolicited (admission gate).
    ProtocolViolation,
}

impl Misbehavior {
    /// Relative penalty weight; tuned later.
    pub(crate) fn weight(&self) -> f64 {
        0.0 // sketch
    }
}

/// Decay/clamp knobs — two channels, two time constants.
#[derive(Debug, Clone)]
pub(crate) struct ReputationConfig {
    /// Slow (honesty) half-life, ~hours. Forgiveness for selection only; the network
    /// export fires at report time, pre-decay.
    pub(crate) score_halflife: Duration,
    /// Fast (responsiveness) half-life, ~seconds (order of `backoff_cap`).
    pub(crate) load_halflife: Duration,
    /// Floor for `score`, bounding worst-case recovery. Ceiling is 0: no positive
    /// reinforcement (credit could be banked; clearing load on success would whitewash
    /// a flapping peer).
    pub(crate) score_floor: f64,
    /// Selection weight of a floor-clamped peer vs a neutral one (e.g. 0.01 = 1% of
    /// neutral odds). Sets α = −ln(this) / |`score_floor`| in `select_sources`.
    pub(crate) score_floor_weight: f64,
}

/// Per-producer score. Both channels decay lazily from one shared anchor (exponential
/// decay composes), so there is no background sweep and untouched entries cost nothing.
#[derive(Debug, Clone)]
pub(crate) struct PeerScore {
    /// Honesty. Misbehavior penalties by kind; decays toward 0; clamped `[score_floor, 0]`.
    score: f64,
    /// Responsiveness. `+1` per timeout, decays with `load_halflife`. An EWMA, not a
    /// count: 100 old + 1 fresh ≈ 1; 100 fresh ≈ 100.
    timeout_load: f64,
    /// Shared decay anchor: when both channels were last normalized.
    last_update_at: Instant,
}

impl PeerScore {
    /// Write path: decay both channels to `now`, then the caller applies its delta.
    fn touch(&mut self, _now: Instant, _cfg: &ReputationConfig) {}

    /// Read path: pure effective `(score, timeout_load)` at `now`, so ranking keeps `&self`.
    fn effective(&self, _now: Instant, _cfg: &ReputationConfig) -> (f64, f64) {
        (0.0, 0.0) // sketch
    }
}

/// The engine's whole dependency on scoring — faults in, sources out. Injected as
/// `Box<dyn ReputationPolicy>`, so the model is swappable and tests can script it.
pub(crate) trait ReputationPolicy {
    /// Misbehavior funnel; attribution resolved by the caller.
    fn report(&mut self, who: &[AccountId], what: Misbehavior, about: &DataId);
    /// A peer left a request unanswered past `request_timeout`.
    fn note_timeout(&mut self, who: &AccountId, now: Instant);
    /// Pick `n` pull sources from `pool`, excluding `outstanding`.
    fn select_sources(
        &self,
        pool: &[AccountId],
        outstanding: &[AccountId],
        n: usize,
        now: Instant,
    ) -> Vec<AccountId>;
    /// Epoch switched: drop entries for accounts no longer relevant.
    fn on_epoch_switch(&mut self, still_relevant: &dyn Fn(&AccountId) -> bool);
}

/// Default policy: SPICE-internal reputation keyed by producer `AccountId`. Sparse — a
/// missing entry reads as neutral-and-live, so the honest case allocates nothing.
pub(crate) struct Reputation {
    scores: HashMap<AccountId, PeerScore>,
    config: ReputationConfig,
    // exporter: NetworkPeerScoreExporter — the account→peer bridge (open design seam).
}

impl ReputationPolicy for Reputation {
    /// `touch` + `score -= weight`, then export to the network scorer (enforcement is
    /// the export, at report time — decay never weakens it). Called for engine-immediate
    /// faults, consumer `FailedEvent`s, and the certification comparator.
    fn report(&mut self, _who: &[AccountId], _what: Misbehavior, _about: &DataId) {}

    /// `touch` + `timeout_load += 1` — the only other write path. Success and NAKs are
    /// deliberate no-ops (rewards invite credit-banking; the fast half-life restores a
    /// recovered peer anyway). Possible later tweak: a fractional bump for producers
    /// whose pushed part never arrived — needs load data.
    fn note_timeout(&mut self, _who: &AccountId, _now: Instant) {}

    /// Weighted-sample `n` from `pool` (minus `outstanding`) by
    /// `w = exp(α·score) / (1 + timeout_load)`: neutral ⇒ w = 1; weights never reach
    /// zero; α from `score_floor_weight` bounds exile. Sampling, not argmax — argmax
    /// herds every recovering node onto one producer. O(P) with P ≈ tens, recovery-path
    /// only; no heap, since the two half-lives make the combined order time-varying.
    fn select_sources(
        &self,
        pool: &[AccountId],
        _outstanding: &[AccountId],
        _n: usize,
        _now: Instant,
    ) -> Vec<AccountId> {
        pool.to_vec() // sketch
    }

    /// Epoch GC: drop entries for accounts no longer in any relevant producer set,
    /// bounding the table at ~pool size (decay only forgives within a tenure). Assumes
    /// sources are producers; adding non-producer archival holders would GC them wrongly
    /// here and leave them out of the account→peer bridge — both need revisiting if
    /// sources ever widen.
    fn on_epoch_switch(&mut self, _still_relevant: &dyn Fn(&AccountId) -> bool) {}
}

/// The open seam: SPICE reputation is per-`AccountId`, network scoring per-`PeerId`.
/// Maps account→peer (Tier1) and feeds the network scorer, which owns banning. No
/// mappable connection ⇒ `export` is a no-op, so the internal table stays primary.
pub(crate) trait NetworkPeerScoreExporter {
    fn export(&self, account: &AccountId, what: &Misbehavior);
}
