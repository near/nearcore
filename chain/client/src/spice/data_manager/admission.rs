//! SKETCH. Centralized admission control. One gate every received unit
//! passes *before* any buffering/allocation — written once here instead of per-buffer.
//! Fixes the size/DoS TODOs (`:536`, `:575`, `:598`, `:266`, `:522`) in one place.

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
    #[error("per-block byte budget exhausted")]
    BlockBudgetExhausted,
    #[error("global byte budget for the {0:?} lane exhausted")]
    ClassBudgetExhausted(QosClass),
    #[error("orphan byte budget (total or per-sender) exhausted")]
    OrphanBudgetExhausted,
    #[error("we neither need nor produce this item")]
    Irrelevant,
    #[error("sender is not a producer / requester is not entitled")]
    Unauthorized,
}

/// Byte budgets (not entry counts — the current `pending_partial_data` bounds count,
/// which is the DoS hole). Enforced per block, per QoS lane, and globally so the
/// `Background` lane can be shed first and can never starve `Priority`.
///
/// INVARIANT: budgets are DoS bounds, sized strictly ABOVE worst-case legitimate
/// traffic (max encoded witness × shard count × speculative window, etc.). A
/// legitimate validator-lane unit bouncing off a budget is a liveness bug, not
/// protection — tie the values to protocol maxima with a debug assertion.
#[derive(Debug, Clone)]
pub(crate) struct Budgets {
    pub(crate) global_priority_bytes: u64,
    pub(crate) global_background_bytes: u64,
    pub(crate) per_block_bytes: u64,
    /// Orphan pool bounds: total, and per sender — pre-block, the sender identity is
    /// the only scarce resource an attacker must spend, so it is what we cap on.
    pub(crate) orphan_bytes: u64,
    pub(crate) per_sender_orphan_bytes: u64,
    /// How far above the head we accept speculative/orphan data (distance-to-head bound).
    pub(crate) speculative_allowance: BlockHeight,
}

/// Per-type maximum declared/encoded sizes, checked *before* allocating.
/// For coded kinds the cap is on the total `encoded_length` (≈ `data_len · N / K`, so
/// it must account for the RS ratio at the maximum producer count); the implied
/// per-part cap is `encoded cap / N` — a single part exceeding it is `OversizedUnit`
/// even when the declared total is within bounds.
#[derive(Debug, Clone)]
pub(crate) struct SizeCaps {
    pub(crate) max_witness_encoded_len: u64,
    pub(crate) max_receipt_proof_encoded_len: u64,
    pub(crate) max_contract_code_len: u64,
}

/// Units whose block we haven't processed yet: relevance and the window check are
/// unevaluable (the hash can't be resolved to a height, and no `FetchItem` exists), so
/// they park here under their own strict bounds and re-run the FULL gate when the
/// block arrives. Replaces all three ad-hoc orphan buffers (`pending_partial_data`,
/// `waiting_for_block`, the receipt tracker's buffer) with one bounded pool.
pub(crate) struct OrphanPool {
    // block_hash → buffered units + their senders; bounded by `Budgets::orphan_bytes`
    // total and `Budgets::per_sender_orphan_bytes` per sender. Evicted on admission
    // into an item (block arrived), on expiry (block below final head), or by budget
    // pressure (oldest-first).
}

pub(crate) struct AdmissionControl {
    budgets: Budgets,
    caps: SizeCaps,
    orphans: OrphanPool,
    used_priority: u64,
    used_background: u64,
    /// One entry per in-window block holding buffered bytes. Pruned when the block's
    /// items complete or expire (`release`) — must never leak an entry per block.
    used_per_block: HashMap<CryptoHash, u64>,
}

impl AdmissionControl {
    /// The one gate. Called on every received unit (and mirrored for serve-side
    /// requests) before it touches any buffer. `declared_len` is the
    /// attacker-controllable field — rejected here, not at decode. A unit for an
    /// unknown block cannot be window-checked and goes through `admit_orphan` instead.
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

    /// Reduced gate for unknown-block units (size caps + orphan budgets only —
    /// relevance/window are checked later, when the block arrives and the unit re-runs
    /// `admit`).
    pub(crate) fn admit_orphan(
        &mut self,
        _block_hash: &CryptoHash,
        _sender: &AccountId,
        _declared_len: u64,
        _unit_len: u64,
    ) -> Result<(), AdmitError> {
        Ok(()) // sketch
    }

    /// Release the reservation when an item completes or expires; drops the
    /// `used_per_block` entry once its block has no buffered bytes left.
    pub(crate) fn release(&mut self, _id: &DataId, _bytes: u64, _qos: QosClass) {}
}
