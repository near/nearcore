//! SKETCH. Wire request (unified across all kinds) + the consumer-facing events.
//!
//! One versioned `SpiceDataRequest` covers every kind — one path to authorize and score.
//! Requests always enumerate exactly what they want; the server never decides. A "send
//! whatever you have" form would be a bandwidth-amplification vector and would defeat
//! in-flight dedup. The request is batched: `wants` carries several entries for one peer
//! under a single requester/lane/signature.

use super::QosClass;
use super::item::DataId;
use super::reputation::Misbehavior;
use near_primitives::types::{AccountId, SpiceChunkId};

/// Which units of `data_id` the requester wants — always explicit (see module docs). On
/// escalation the missing set is striped disjointly across the fanned-out sources, so
/// each source gets its own subset (their union covers the hole once).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WantUnits {
    /// Specific RS ordinals (targeted recovery). Empty is invalid; admission dedups, caps
    /// the count, and rejects ordinals ≥ N (`MalformedWant`).
    Ordinals(Vec<u32>),
    /// The whole content-addressed blob (contract code, K=1). `chunk` is the claimed
    /// context: the server authenticates the signature in that block's epoch, applies the
    /// witness recipients rule to the chunk, and requires `hash ∈` its accessed-code set.
    /// A server that did not execute the claimed chunk answers NAK — correct, not
    /// withholding (that anchor's pool is the fork's own producers).
    Blob { chunk: SpiceChunkId },
}

/// Who is asking — identity only. The lane is a separate field (`SpiceDataRequest::lane`),
/// capped by identity + entitlement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Requester {
    /// Signed with a validator key: the recipient check is evaluable and the response may
    /// be account-addressed. Identity is not purpose — a validator pulling outside its
    /// consensus duty (state sync, catch-up) requests `Background`.
    Validator(AccountId),
    /// Unsigned (RPC / state-sync). The response must be routed back over the requesting
    /// connection, never to a requester-named destination — this prevents spoofed-
    /// destination reflection. Always `Background`, bounded by its byte budget.
    Anonymous,
}

/// Unified, versioned request. (Sketch shows only V1's payload; the wire type is
/// `enum SpiceDataRequest { V1(..) }`.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpiceDataRequest {
    /// One or more `(id, want)` entries for the same peer. One requester/lane/signature
    /// covers the batch; entitlement and the granted lane are per entry, and each entry
    /// resolves or NAKs independently in the response.
    pub(crate) wants: Vec<(DataId, WantUnits)>,
    pub(crate) requester: Requester,
    /// Requested QoS lane, granted iff identity + entitlement allow: `Priority` ⇔
    /// `Validator` ∧ `recipients(id).contains(who)` ∧ requested, else `Background`.
    /// Self-declaration can only downgrade, so it is not a DoS lever.
    pub(crate) lane: QosClass,
    // signature omitted in the sketch (present for `Validator`).
}

/// A served unit coming back (pushed parts arrive with the same shape). Carries no lane
/// tag: the receiver derives the lane from its own state, so budget attribution is never
/// sender-controlled. A batched request fans out into per-id responses, so `data_id` is
/// singular here.
#[derive(Debug)]
pub(crate) struct DataResponse {
    pub(crate) data_id: DataId,
    pub(crate) sender: AccountId,
    pub(crate) payload: ResponsePayload,
}

/// What came back.
#[derive(Debug)]
pub(crate) enum ResponsePayload {
    /// Coded parts (+ commitment/Merkle proofs) or a blob — concrete payload types
    /// (`SpiceDataPart` / `CodeBytes`) omitted in the sketch.
    Units,
    /// Signed NAK: "I don't have this (yet)". The responder is responsive, so this carries
    /// no liveness penalty and no credit — it just clears the in-flight entry and lets the
    /// item ride its backoff, distinguishing not-yet-produced from not-responding. One
    /// exception: a NAK from a sender the tracker already records a pushed part from is a
    /// self-contradiction (`Misbehavior::DeniedHeldData`). Any other NAK is penalty-free.
    NotAvailable,
}

/// Consumer → manager: semantic validation succeeded and the durable artifact is persisted
/// consumer-side. The manager releases budgets and shrinks the item to its
/// `DataAttribution` (`FetchState::ProcessedLocally`), kept until expiry — local success
/// is not the final word (see the certification comparator).
#[derive(Debug)]
pub(crate) struct VerifiedEvent {
    pub(crate) data_id: DataId,
}

/// Consumer → manager: semantic validation failed. For a witness that is
/// `spice_{pre_,}validate_chunk_state_witness` failing on inputs (source receipt proofs
/// that don't verify against prev results, tx root ≠ header's, a fraudulent
/// `proof_of_invalid_chunk`, execution erroring on the recorded state) — the witness
/// carries no claimed result to mismatch; a divergent computed result surfaces at
/// certification, not here. For a receipt: the `outgoing_receipts_root` check; for
/// accesses: consistency with the witness's embedded list. The manager attributes via the
/// retained `DataAttribution` (the winning commitment's senders; blob ⇒ the responder) and
/// funnels `kind` into reputation — this is why decode is not terminal. Also carries the
/// retroactive `Failed(CertifiedResultMismatch)` from the comparator. Arriving after the
/// item expired is a tolerated no-op.
#[derive(Debug)]
pub(crate) struct FailedEvent {
    pub(crate) data_id: DataId,
    pub(crate) kind: Misbehavior,
}
