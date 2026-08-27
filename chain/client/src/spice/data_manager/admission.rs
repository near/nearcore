//! SKETCH. Centralized admission control: one gate every received unit passes before any
//! buffering/allocation, so all size/DoS bounds live here instead of per-buffer.

use super::Lane;
use super::item::DataId;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AdmitError {
    #[error("declared encoded_length exceeds the per-type cap")]
    OversizedDeclared,
    #[error(
        "part length is not the one the declared encoded_length implies, or blob exceeds its cap"
    )]
    OversizedUnit,
    #[error("block is outside [final head, head + max_heights_above_head]")]
    OutOfWindow,
    #[error("sender's unsolicited byte budget for this block exhausted")]
    SenderBudgetExhausted,
    #[error("global byte budget for the {0:?} lane exhausted")]
    ClassBudgetExhausted(Lane),
    #[error("sender's orphan byte budget exhausted")]
    OrphanBudgetExhausted,
    #[error("we neither need nor produce this item")]
    Irrelevant,
    #[error("part merkle proof does not verify against the commitment root")]
    InvalidMerkleProof,
    #[error("unit claims a commitment banned on this item (failed verdict or garbage decode)")]
    BannedCommitment,
    #[error("sender already provided a different commitment for this item")]
    ConflictingCommitment,
    #[error("unsolicited part for an ordinal that is not the sender's own")]
    ForeignOrdinal,
    #[error("sender is not a producer")]
    Unauthorized,
    #[error("want-list malformed: duplicate, out-of-range, or too many ordinals")]
    MalformedWant,
}

/// Byte budgets (not entry counts — one entry can be arbitrarily large). Enforced per
/// lane (so `Background` is shed first and can't starve `Priority`), per (block, sender),
/// per (item, requester) on the serve side, and on the orphan pool. These bound memory only;
/// scheduling isolation is the priority pool's job (see [`super::Lane`]).
///
/// Invariant: budgets are DoS bounds, sized above worst-case legitimate traffic. A
/// validator-lane unit bouncing off a budget is a liveness bug — tie the values to
/// protocol maxima with a debug assertion.
#[derive(Debug, Clone)]
pub(crate) struct Budgets {
    pub(crate) global_priority_bytes: u64,
    pub(crate) global_background_bytes: u64,
    /// Per-(block, sender) cap on unsolicited units, sized to the sender's role (≈ its own
    /// ordinal per produced item, with slack). Bounds displacement: a producer flooding
    /// valid-looking parts under a fabricated commitment is held to its share, so honest
    /// parts still fit. The per-block total is implied (≤ #producers × this), so it needs
    /// no knob. Solicited traffic is accounted against its `in_flight` request instead.
    pub(crate) per_block_sender_bytes: u64,
    /// Serve-side cap on bytes sent per (`Produce` item, requester), ~2× `encoded_length`.
    /// The only outbound bound: the priority pool separates lanes, not requesters within a
    /// lane. Per-block total is implied, as with `per_block_sender_bytes`. Counted on
    /// `ProduceState::ReadyToServe`; exhausted ⇒ NAK.
    pub(crate) per_item_requester_serve_bytes: u64,
    /// Orphan pool cap, per sender — pre-block, the authenticated sender is the only
    /// scarce resource, so it is the only cap. Eligible senders are bounded, so the pool
    /// total is derived (≤ |eligible| × this); size it so that total is acceptable.
    pub(crate) per_sender_orphan_bytes: u64,
}

/// Per-type maximum declared sizes, checked before allocating. For coded kinds the cap is on
/// the commitment's `encoded_length` — the serialized data length, so it needs no RS-ratio
/// adjustment. A part is not capped but fixed: exactly `ceil(encoded_length / K)` bytes, so
/// any other length is `OversizedUnit` even within the declared total.
#[derive(Debug, Clone)]
pub(crate) struct SizeCaps {
    pub(crate) max_witness_encoded_len: u64,
    pub(crate) max_receipt_proof_encoded_len: u64,
    pub(crate) max_contract_code_len: u64,
}

/// Parks inbound tied to a block not yet processed: without the block, relevance and the
/// window are unevaluable, so it waits under the per-sender byte cap and re-runs the full
/// gate at `seed_block`. Two kinds park:
/// - fetchable units (RS parts / blobs) → re-admitted into their `FetchItem`.
/// - contract-accesses signals → seed one `ContractCode{hash}` item per uncached hash.
///   Accesses precede the witness and may arrive before the block; they can't be acted on
///   earlier anyway, so parking loses no prefetch.
pub(crate) struct OrphanPool {
    // block_hash → parked units + accesses + senders; bounded per authenticated sender
    // (`Budgets::per_sender_orphan_bytes`; the total is derived). Evicted at `seed_block`,
    // on expiry, or by budget pressure.
}

pub(crate) struct AdmissionControl {
    budgets: Budgets,
    caps: SizeCaps,
    /// How far above the head a block may be for its data to be admitted.
    max_heights_above_head: BlockHeightDelta,
    orphans: OrphanPool,
    used_priority: u64,
    used_background: u64,
    /// One entry per (in-window block, sender) with unsolicited buffered bytes. Pruned
    /// when the block's items complete or expire (`release`) — must never leak entries.
    used_per_block_sender: HashMap<(CryptoHash, AccountId), u64>,
}

impl AdmissionControl {
    /// The one gate, called on every received unit (and mirrored for serve-side requests)
    /// before it touches any buffer. `declared_len` is the attacker-controllable field,
    /// rejected here rather than at decode. A unit for an unknown block goes through
    /// `admit_orphan` instead.
    pub(crate) fn admit(
        &mut self,
        _id: &DataId,
        _lane: Lane,
        _sender: &AccountId,
        _declared_len: u64,
        _unit_len: u64,
        _head_height: BlockHeight,
        _final_head_height: BlockHeight,
    ) -> Result<(), AdmitError> {
        Ok(()) // sketch
    }

    /// Reduced gate for unknown-block units (size caps + per-sender orphan budget;
    /// relevance/window are checked later, when the block arrives and the unit re-runs
    /// `admit`). Precondition: the caller has already authenticated the unit — signature
    /// verified against the sender's key in one of the block's `possible_epoch_ids`, sender
    /// a possible producer there — so `sender` is a bounded, unforgeable identity.
    /// Unverifiable units are dropped before any buffering.
    pub(crate) fn admit_orphan(
        &mut self,
        _block_hash: &CryptoHash,
        _sender: &AccountId,
        _declared_len: u64,
        _unit_len: u64,
    ) -> Result<(), AdmitError> {
        Ok(()) // sketch
    }

    /// Give back what a charge took. Per-sender, because `used_per_block_sender` is keyed
    /// that way and a single total can't decrement it; the lane refund is the sum. The
    /// caller derives `charges` from the tracker (fixed part length × each sender's count).
    /// Called at delivery for the decoded tracker, at the verdict for `residual`, on
    /// rejected trackers, and at expiry for whatever is left. Drops the block's
    /// `used_per_block_sender` entries once nothing is buffered for it.
    pub(crate) fn release(&mut self, _id: &DataId, _lane: Lane, _charges: &[(AccountId, u64)]) {}
}
