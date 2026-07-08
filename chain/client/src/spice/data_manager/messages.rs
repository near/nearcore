//! SKETCH. Wire request (unified across all kinds) + the consumer-facing events.
//!
//! The two request messages (`SpicePartialDataRequest` for ordinals,
//! `SpiceContractCodeRequest` for hash-sets) unify into one versioned `SpiceDataRequest`
//! — one request path, one place to authorize and score. Versioned from birth,
//! alongside the versioning of the other SPICE wire types.
//!
//! Requests always enumerate exactly what they want; the server never decides what to
//! send. A "send whatever you have" form would be a bandwidth-amplification vector (a
//! tiny request provoking a full re-encode-and-ship — the pathological pull this
//! redesign kills) and would defeat targeted, in-flight-deduplicated recovery.

use super::QosClass;
use super::item::DataId;
use super::reputation::Misbehavior;
use near_primitives::types::AccountId;

/// Which units of `data_id` the requester wants — always explicit (see module docs).
/// On escalation the SAME full missing-ordinal set goes to each fanned-out source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WantUnits {
    /// Specific RS ordinals (targeted recovery). Empty is invalid.
    Ordinals(Vec<u32>),
    /// The whole content-addressed blob (contract code, K=1).
    Blob,
}

/// Who is asking — determines authorization and QoS lane. A separate lane,
/// NOT a separate actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Requester {
    /// A signed validator/producer that must be an entitled recipient for `data_id`.
    /// Served on the `Priority` lane.
    Validator(AccountId),
    /// Non-validator (RPC / state-sync). No signer — so the response MUST be routed
    /// back over the requesting connection (never to a requester-named destination),
    /// which is what prevents spoofed-destination reflection. Served on the
    /// `Background` lane, bounded by its byte budget so it can't starve validators.
    NonValidator,
}

impl Requester {
    pub(crate) fn qos(&self) -> QosClass {
        match self {
            Requester::Validator(_) => QosClass::Priority,
            Requester::NonValidator => QosClass::Background,
        }
    }
}

/// Unified, versioned request. (Sketch shows only V1's payload; the wire type is
/// `enum SpiceDataRequest { V1(..) }` per the versioning work.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpiceDataRequest {
    pub(crate) data_id: DataId,
    pub(crate) want: WantUnits,
    pub(crate) requester: Requester,
    // signature omitted in the sketch (present for `Validator`).
}

/// A served unit coming back in (pushed parts arrive with the same shape).
#[derive(Debug)]
pub(crate) struct DataResponse {
    pub(crate) data_id: DataId,
    pub(crate) sender: AccountId,
    pub(crate) qos: QosClass,
    pub(crate) payload: ResponsePayload,
}

/// What came back.
#[derive(Debug)]
pub(crate) enum ResponsePayload {
    /// Coded parts (+ commitment/Merkle proofs) or a blob — concrete payload types
    /// (`SpiceDataPart` / `CodeBytes`) omitted in the sketch.
    Units,
    /// Signed NAK: "I don't have this (yet)". The responder is *responsive*, so this
    /// carries NO liveness penalty (only silence feeds `note_timeout`) and no credit;
    /// it just clears the in-flight entry and lets the item ride its backoff —
    /// distinguishing not-yet-produced from not-responding. Retroactively becomes
    /// `Misbehavior::DeniedHeldData` evidence if certification later proves the
    /// responder must have held the data.
    NotAvailable,
}

/// Consumer → manager: semantic validation of an assembled item succeeded and the
/// durable artifact is persisted consumer-side. The manager releases budgets and
/// shrinks the item to its attribution husk (`FetchState::ProcessedLocally`),
/// retained until expiry — local success is not the final word (see the
/// certification comparator).
#[derive(Debug)]
pub(crate) struct VerifiedEvent {
    pub(crate) data_id: DataId,
}

/// Consumer → manager: semantic validation failed (state transition / receipt root /
/// accesses↔witness). Manager maps `data_id` → contributing senders (via the
/// `DataAttribution` retained until expiry) and funnels `kind` into reputation — this
/// is why decode is NOT terminal. Also carries *retroactive* reports: when
/// certification lands for a chunk we endorsed with a different result hash, the
/// comparator emits `Failed(CertifiedResultMismatch)` — which is why attribution
/// outlives `Verified`. Arriving after the item expired is a tolerated no-op.
#[derive(Debug)]
pub(crate) struct FailedEvent {
    pub(crate) data_id: DataId,
    pub(crate) kind: Misbehavior,
}
