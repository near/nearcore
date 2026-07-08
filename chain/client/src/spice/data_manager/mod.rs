//! `SpiceDataManager` — the single per-node state machine that is also a **generic
//! fetch engine** for every kind of SPICE distributed data (witness, receipt proof,
//! contract code). It replaces the six scattered lifecycle structures (distributor
//! `waiting_on_data` / `recently_decoded_data` / `pending_partial_data`, validator
//! `waiting_for_block` / `partial_chunk_data`, executor `UnverifiedReceiptTracker`)
//! and the hand-rolled contract-code path.
//!
//! - "Done"/`Have` is a role-appropriate *durable artifact*
//!   (endorsement / persisted receipt proof / produced data), NOT the raw
//!   data — stateless validators never persist the witness. The engine persists nothing.
//! - availability is *discovered* (first obtained part / speculative pull)
//! - recovery: per-item deadline scheduler, part-granular, reputation-driven
//!   source selection (weighted sampling — see [`reputation`]), over-request + fan-out
//!   escalation. Requests always enumerate explicit ordinals; failure memory is global
//!   (the reputation table), the item keeps only transient `in_flight` plumbing.
//! - [`admission`]: one gate before buffering (size / distance-to-head /
//!   per-class byte budgets), plus the bounded [`admission::OrphanPool`] for
//!   unknown-block units.
//! - [`reputation`]: fault funnel → two-channel SPICE-internal score
//!   (slow honesty + fast liveness, lazy decay) → network export.
//! - [`messages`]: authorized, unified `SpiceDataRequest`; QoS lanes for
//!   non-validator (RPC) fetches (a priority *attribute*, NOT a separate actor).
//! - contract code is just a third [`fetchable::DataKind`] config.
//!
//! What is NOT here (stays consumer-side, per the state/behavior split): semantic
//! validation — witness state-transition check, receipt-root check, accesses↔witness
//! consistency. Those run in the validator/executor and feed back `Verified`/`Failed`
//! events (see [`messages`]).

// Sketching
#![allow(dead_code)]

mod admission;
mod fetchable;
mod item;
mod messages;
mod reputation;
mod scheduler;
mod serve;

pub(crate) use admission::{AdmissionControl, AdmitError, Budgets, OrphanPool, SizeCaps};
pub(crate) use fetchable::{ContractCodeKind, DataKind, Interest, ReceiptProofKind, WitnessKind};
pub(crate) use item::{
    Assembly, DataAttribution, DataId, FetchItem, FetchState, InFlightRequest, Item, ProduceState,
    TransferUnit,
};
pub(crate) use messages::{
    DataResponse, FailedEvent, Requester, ResponsePayload, SpiceDataRequest, VerifiedEvent,
    WantUnits,
};
pub(crate) use reputation::{Misbehavior, Reputation, ReputationConfig};
pub(crate) use scheduler::{Backoff, DeadlineScheduler, TimingConfig};
pub(crate) use serve::{CachedEncoding, EncodeCache};

use near_primitives::types::BlockHeight;
use std::collections::{BTreeMap, HashMap};

/// QoS lane. An attribute carried by every fetch and every
/// incoming serve-request. `Priority` traffic is drained/served first; `Background`
/// is bounded by its own budget (see [`Budgets`]) and shed first under pressure.
/// The lane attaches to the fetch *cause*, not the data: the same witness can be wanted
/// by our validator duty AND an RPC subscriber — the item then takes the max lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum QosClass {
    /// Consensus-critical: we are an assigned validator / next-block producer for it.
    Priority,
    /// RPC / state-sync driven fetch or serve. Never starves `Priority`.
    Background,
}

/// Owns *all* per-item fetch state, sender attribution, scheduling, admission, and reputation for one node.
/// Holds a map of DataId → Item items;
/// Arriving bytes pass AdmissionControl before touching an item's Assembly;
/// Stuck items get pull requests scheduled by DeadlineScheduler, aimed by Reputation;
/// Every unit's origin is recorded in DataAttribution so faults — immediate or late —
///   land on the right accounts via Misbehavior reports.
/// Consumer actors (validator/executor) subscribe to the
///   `Verified`/`Failed` events it emits and keep their own semantic validation.
pub(crate) struct SpiceDataManager {
    /// per-item lifecycle, consume-side AND produce-side in one map
    /// ([`Item`]) so "have and request the same item" is ruled out.
    /// `Have` items are NOT kept here — the store is the source of truth
    /// for `Have` (kills `recently_decoded_data`).
    items: HashMap<DataId, Item>,

    /// Secondary index for head-driven expiry: `on_heads_advanced` drains the range
    /// at/below the final execution head instead of scanning every item (which would
    /// rebuild the global sweep this design deletes). Heights are captured at seed
    /// time (`FetchItem::height`) — expiry never does per-item store lookups.
    items_by_height: BTreeMap<BlockHeight, Vec<DataId>>,

    /// Fires per-item retry/escalation deadlines. Owns `when` only; retry state lives on items
    scheduler: DeadlineScheduler,

    /// Pre-buffer gate: size caps, distance-to-head, per-class byte budgets, and the
    /// bounded orphan pool for units whose block we haven't processed yet.
    admission: AdmissionControl,

    /// SPICE-internal producer reputation; two channels (slow honesty score + fast
    /// timeout load, lazily decayed) drive `select_sources` and export to the network
    /// peer scorer. The ONLY failure memory — items keep none.
    reputation: Reputation,

    /// Tuning knobs (block-time / RTT relative; all configurable). See [`TimingConfig`].
    timing: TimingConfig,

    /// Encoded-parts cache for the serve side. See [`EncodeCache`] for the policy:
    /// global byte budget, LRU, populated for free at production time, eviction is
    /// correctness-free (miss ⇒ re-encode from the stored artifact).
    encode_cache: EncodeCache,
    // Collaborators injected at construction (epoch manager, chain store, network
    // adapter, consumer-event senders, spawner for heavy decode) omitted in the sketch.
    // Our validator identity is resolved per use via `MutableValidatorSigner` — keys
    // hot-swap at runtime, so a snapshotted `AccountId` would go stale; `None` ⇒ we
    // can still fetch on the `Background` lane.
}

// Illustrative surface — bodies omitted in the sketch.
impl SpiceDataManager {
    /// Seed items this node will need, derived from a processed block + epoch info
    /// (replaces `start_waiting_on_data`). Idempotent; consults `DataKind::is_done`
    /// (a store read) here and on completion only — never per received unit. Also
    /// re-admits any orphans parked for this block.
    pub(crate) fn seed_needs(&mut self, _block_hash: near_primitives::hash::CryptoHash) {}

    /// A unit arrived (push or pull response). Runs admission (unknown block ⇒ orphan
    /// pool), records per-commitment senders, inserts into the item's [`Assembly`],
    /// and clears the matching `in_flight` entry. A `NotAvailable` NAK only clears the
    /// entry (responsive ⇒ no liveness penalty) and lets the item ride its backoff.
    /// On completion runs distribution-level verify (decode + hash) and *delivers* the
    /// assembled data to the consumer, moving the item to `Delivered` (assembly
    /// dropped, attribution retained). The consumer semantically validates, persists the
    /// durable artifact (endorsement / proof), and reports back `Verified`/`Failed` —
    /// the engine persists nothing. The *first* accepted unit is the availability
    /// signal
    pub(crate) fn on_data_received(&mut self, _resp: DataResponse) -> Result<(), AdmitError> {
        Ok(())
    }

    /// A due deadline fired (after the scheduler's lazy validation against
    /// `FetchItem::next_deadline`). First converts `in_flight` entries older than
    /// `request_timeout` into `note_timeout(who)`; then (re)issues the pull: the full
    /// explicit missing-ordinal set to `select_sources`-sampled producers (escalation
    /// fans the SAME set out to several), and re-arms per the backoff.
    pub(crate) fn on_deadline(&mut self, _id: &DataId) {}

    /// Chain heads advanced: drain `items_by_height` at/below the final execution head to
    /// expire items (contract code expires via its highest interested height), and
    /// re-arm items whose existence gate (execution frontier) just opened.
    /// ORDER MATTERS: the certification comparator (endorsed result hash vs certified,
    /// for chunks we endorsed) must run on the newly-certified results BEFORE expiry
    /// drains those heights — expiry is what drops the attribution it needs.
    /// Expiry also eagerly evicts the expired ids from `encode_cache`.
    pub(crate) fn on_heads_advanced(&mut self) {}

    /// Consumer reported the outcome of *semantic* validation (state transition /
    /// receipt root / accesses consistency). `Verified` releases budgets and shrinks
    /// the item to its attribution husk (`FetchState::ProcessedLocally`), kept until expiry —
    /// a later `Failed(CertifiedResultMismatch)` must still attribute. `Failed`
    /// funnels into reputation via the retained [`DataAttribution`] (after expiry: a
    /// tolerated no-op).
    pub(crate) fn on_verified(&mut self, _ev: VerifiedEvent) {}
    pub(crate) fn on_failed(&mut self, _ev: FailedEvent) {}

    /// Serve a pull request. Only `Item::Produce`
    /// entries serve (recovery pool = producers; recipients never serve).
    /// 1. Authorize: the network layer verified the `Validator` signature; here
    ///    `DataKind::is_entitled(id, requester)` — the consumer-side mirror of
    ///    `sources` — must hold, else `AdmitError::Unauthorized`. `NonValidator`
    ///    requesters skip entitlement but are served route-back only, on the
    ///    `Background` lane under its byte budget.
    /// 2. Resolve: `Produced` ⇒ requested ordinals from `encode_cache` (miss ⇒
    ///    re-encode from the stored artifact, warm the cache); blob ⇒ read code from
    ///    store. `Producing` or unknown id ⇒ signed `NotAvailable` NAK — never an
    ///    error: the NAK is the "not yet" signal and later denial evidence.
    /// 3. Respond on the requester's lane; `Priority` drains before `Background`
    ///    (host-actor mailbox ordering).
    pub(crate) fn serve_request(
        &mut self,
        _req: SpiceDataRequest,
    ) -> Result<DataResponse, AdmitError> {
        Err(AdmitError::Irrelevant) // sketch
    }

    /// Epoch switched: GC reputation entries for accounts that left the producer sets.
    pub(crate) fn on_epoch_switch(&mut self) {}
}
