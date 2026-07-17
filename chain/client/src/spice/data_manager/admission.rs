//! SKETCH. Centralized admission control: one gate every received unit passes before any
//! buffering/allocation, so all size/DoS bounds live here instead of per-buffer.

use super::QosClass;
use super::item::DataId;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AdmitError {
    #[error("declared encoded_length exceeds the per-type cap")]
    OversizedDeclared,
    #[error("part/blob size exceeds the per-type cap")]
    OversizedUnit,
    #[error("block is outside [final head, head + speculative_allowance]")]
    OutOfWindow,
    #[error("sender's unsolicited byte budget for this block exhausted")]
    SenderBudgetExhausted,
    #[error("global byte budget for the {0:?} lane exhausted")]
    ClassBudgetExhausted(QosClass),
    #[error("sender's orphan byte budget exhausted")]
    OrphanBudgetExhausted,
    #[error("we neither need nor produce this item")]
    Irrelevant,
    #[error("unit claims a commitment banned on this item after a semantic failure")]
    BannedCommitment,
    #[error("sender already backed a different commitment for this item")]
    ConflictingCommitment,
    #[error("unsolicited part for an ordinal that is not the sender's own")]
    ForeignOrdinal,
    #[error("sender is not a producer")]
    Unauthorized,
    #[error("want-list malformed: duplicate, out-of-range, or too many ordinals")]
    MalformedWant,
}

/// Byte budgets (not entry counts — one entry can be arbitrarily large). Enforced per QoS
/// lane (so `Background` is shed first and can't starve `Priority`), per (block, sender),
/// and on the orphan pool. These bound memory only; scheduling isolation is the priority
/// pool's job (see [`super::QosClass`]).
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
    /// Orphan pool cap, per sender — pre-block, the authenticated sender is the only
    /// scarce resource, so it is the only cap. Eligible senders are bounded, so the pool
    /// total is derived (≤ |eligible| × this); size it so that total is acceptable.
    pub(crate) per_sender_orphan_bytes: u64,
    /// How far above the head we accept speculative/orphan data (distance-to-head bound).
    pub(crate) speculative_allowance: BlockHeight,
}

/// Per-type maximum declared/encoded sizes, checked before allocating. For coded kinds the
/// cap is on total `encoded_length` (≈ data_len · N / K, so it must account for the RS
/// ratio at max producer count); the implied per-part cap is `encoded cap / N`, so an
/// oversized single part is `OversizedUnit` even within the declared total.
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
        _qos: QosClass,
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

    /// Release the reservation when an item completes or expires; drops the block's
    /// `used_per_block_sender` entries once it has no buffered bytes left.
    pub(crate) fn release(&mut self, _id: &DataId, _bytes: u64, _qos: QosClass) {}
}
