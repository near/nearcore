//! `SpiceDataManager` — the single per-node state machine that is also a generic fetch
//! engine for every kind of SPICE distributed data (witness, receipt proof, contract
//! code): one place owning what each item needs, has, and who it came from.
//!
//! - "Done"/`Have` is a durable artifact (endorsement / persisted receipt proof /
//!   produced data), not the raw data — the engine persists nothing.
//! - availability is *discovered* (first obtained part / speculative pull), not announced.
//! - recovery: per-item deadline scheduler, part-granular, reputation-driven source
//!   selection ([`reputation`]). Requests enumerate explicit ordinals; failure memory is
//!   global (the reputation table), the item keeps only transient `in_flight`.
//! - [`admission`]: one gate before buffering, plus the bounded [`admission::OrphanPool`].
//! - [`reputation`]: fault funnel → two-channel score → network export.
//! - [`messages`]: unified `SpiceDataRequest`; QoS lanes for non-validator (RPC) fetches.
//! - contract code is just a third [`fetchable::DataKind`] config.
//!
//! Semantic validation (state-transition / receipt-root / accesses↔witness consistency)
//! stays consumer-side and feeds back `Verified`/`Failed` (see [`messages`]).
//!
//! The fetch mechanics (assembly, scheduler, source selection, admission) are kept
//! separable from the live lifecycle (seed / expire / produce) so a future deep-sync
//! fetcher could reuse them without running through this manager.

// Sketching
#![allow(dead_code)]
#![allow(unused_imports)]

mod admission;
mod fetchable;
mod item;
mod messages;
mod reputation;
mod scheduler;

pub(crate) use admission::{AdmissionControl, AdmitError, Budgets, OrphanPool, SizeCaps};
pub(crate) use fetchable::{
    ContractCodeKind, DataKind, FetchContext, Interest, ReceiptProofKind, WitnessKind,
};
pub(crate) use item::{
    Assembly, DataAttribution, DataId, FetchItem, FetchState, InFlightRequest, Item, ProduceState,
    TransferUnit,
};
pub(crate) use messages::{
    DataResponse, FailedEvent, Requester, ResponsePayload, SpiceDataRequest, VerifiedEvent,
    WantUnits,
};
pub(crate) use reputation::{Misbehavior, Reputation, ReputationConfig, ReputationPolicy};
pub(crate) use scheduler::{Backoff, DeadlineScheduler, TimingConfig};

use near_primitives::types::{BlockHeight, ShardId};
use std::collections::{BTreeMap, HashMap};

/// QoS lane — a manager-computed attribute, not an ingress-routable label. It attaches to
/// the fetch *cause*, not the data (the same witness can be wanted by our validator duty
/// and an RPC subscriber, so the item takes the max lane), so a pre-manager queue can't
/// classify it.
///
/// Enforcement lives in work scheduling, not queue reordering: cheap classification on the
/// loop, heavy work (re-encode / decode / bulk send) offloaded to a pool that serves
/// `Priority` first, and `Background` byte-budgeted (see [`Budgets`]) and shed by admission
/// before it piles up. Ingress may pre-filter locally-checkable facts (drop unsigned; route
/// self-declared `Background`, which can only downgrade) but never *grants* `Priority`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum QosClass {
    /// Consensus-critical: we are an assigned validator / next-block producer for it.
    Priority,
    /// RPC / state-sync / catch-up fetch or serve — including a validator catching up a
    /// shard for the next epoch (an intermediate lane for that is an open follow-up; two
    /// lanes for now). Never starves `Priority`.
    Background,
}

/// Owns all per-item fetch state, sender attribution, scheduling, admission, and
/// reputation for one node. Arriving bytes pass admission before touching an item's
/// assembly; stuck items get pulls scheduled by the deadline scheduler, aimed by
/// reputation; every unit's origin is recorded (materialized as a `DataAttribution` at
/// delivery) so faults land on the right accounts. Consumer actors subscribe to the
/// `Verified`/`Failed` events and keep their own semantic validation.
///
/// Wiring: owned as plain state by a glue actor (`SpiceDataManagerActor`, successor of the
/// distributor actor); every input is a mailbox message becoming one `&mut` entry-point
/// call, so the manager is a pure, synchronously-testable state machine and races are
/// message interleavings (each entry point documents its tolerated ones). Chain-side
/// events (`seed_block` / `on_certified` / `on_heads_advanced`) come from one upstream
/// pipeline in processing order; heavy work runs on the priority pool with completion
/// returning as a message.
pub(crate) struct SpiceDataManager {
    /// Per-item lifecycle, consume- and produce-side in one map ([`Item`]), so "have and
    /// request the same item" is ruled out. `Have` items are not kept here — the store is
    /// the source of truth for `Have`.
    items: HashMap<DataId, Item>,

    /// Head-driven expiry index: `on_heads_advanced` drains the range at/below the final
    /// execution head instead of scanning every item. Heights are captured at seed time
    /// (`FetchItem::height`). Entries are never removed early — a contract-code anchor
    /// change can leave a stale bucket entry, which the drain validates lazily (item
    /// exists, `FetchItem::height` matches the bucket).
    items_by_height: BTreeMap<BlockHeight, Vec<DataId>>,

    /// The gate-side twin of `items_by_height`: witness items in `FetchState::Need`
    /// behind the existence gate, keyed by the (shard, height) that opens them. A witness
    /// recipient does not apply the shard, so it can't see the shard's execution locally;
    /// the gate opens off the shard's *certified* frontier, which is globally visible. An
    /// item at height `h` opens once the frontier reaches `h - witness_pull_margin`, i.e.
    /// the chunk is likely executed but not yet certified. Drained per shard as the
    /// frontier advances (`Need` → `Collecting`, arm after `push_grace`); same lazy
    /// validation on drain. Frontiers only advance, so there is no re-closing. Receipt
    /// proofs don't gate here — their pull is triggered by the executor's apply attempt.
    gated_by_frontier: BTreeMap<(ShardId, BlockHeight), Vec<DataId>>,

    /// Fires per-item retry/escalation deadlines. Owns `when` only; retry state lives on
    /// items.
    scheduler: DeadlineScheduler,

    /// Pre-buffer gate: size caps, distance-to-head, per-class byte budgets, and the
    /// bounded orphan pool for units whose block we haven't processed yet.
    admission: AdmissionControl,

    /// Injected scoring policy (default [`Reputation`]). The engine depends only on the
    /// four-method trait, so the model is swappable and tests can inject scripted
    /// policies. The only failure memory — items keep none.
    reputation: Box<dyn ReputationPolicy>,

    /// Tuning knobs (block-time / RTT relative; all configurable). See [`TimingConfig`].
    timing: TimingConfig,
    // Collaborators injected at construction (epoch manager, chain store, network adapter,
    // consumer-event senders, the `code_cached(epoch, hash)` handle, and the
    // priority-scheduled pool for heavy work) omitted in the sketch. Our validator identity
    // resolves per use via `MutableValidatorSigner` (keys hot-swap, so a snapshotted
    // `AccountId` would go stale); `None` ⇒ we can still fetch on the `Background` lane.
}

// Illustrative surface — bodies omitted in the sketch.
impl SpiceDataManager {
    /// Seed items for a processed block, both roles, from epoch info (called on every
    /// processed block, and per block of the startup walk over `[final exec head → head]`).
    /// Idempotent; also re-admits orphans parked for this block. An id we author seeds as
    /// `Produce` even when we are also a recipient (consumer duties for self-authored data
    /// are met from our own execution). `is_done` (a store read, consulted here and on
    /// completion only) plays two roles:
    /// - fetch side: gates whether to seed — done ⇒ skip.
    /// - produce side: selects the state — artifact in store ⇒ `ReadyToServe` (witness
    ///   items re-read their accessed-`codes` index from the store), else `Producing`. It
    ///   must never suppress the item: the startup walk re-creating `Produce` entries is
    ///   what makes serving survive a restart.
    pub(crate) fn seed_block(&mut self, _block_hash: near_primitives::hash::CryptoHash) {}

    /// Execution finished for data we author. Flips the item `Producing` → `ReadyToServe`
    /// (for witness items recording the chunk's accessed code hashes, supplied by the
    /// executor) and pushes our own ordinal to each of `DataKind::recipients(..)`.
    pub(crate) fn on_produced(&mut self, _id: &DataId) {}

    /// A unit arrived (push or pull response). Runs admission (unknown block ⇒ orphan pool;
    /// unsolicited coded units: own ordinal only), adds the part via
    /// [`Assembly::tracker_for`] (banned/conflicting commitments are rejected and the
    /// sender reported), and clears the matching `in_flight` entry. A `NotAvailable` NAK
    /// only clears the entry and lets the item ride its backoff — unless the sender earlier
    /// pushed a part of this item, which reports `DeniedHeldData`. On completion,
    /// distribution-level verify runs, then completion forks by kind:
    /// - coded ⇒ move to `Delivered` (winning bytes dropped, non-winning trackers kept as
    ///   `residual`, `DataAttribution` kept); for a witness, seed/re-aim still-missing code
    ///   items from its embedded accesses list; the consumer validates, persists the
    ///   artifact, and reports back `Verified`/`Failed`.
    /// - blob ⇒ verified on arrival, so terminal: hand `(code_hash, bytes)` to the
    ///   validator actor and remove the item. No `Delivered`, no verdict.
    /// The first accepted unit is the availability signal.
    pub(crate) fn on_data_received(&mut self, _resp: DataResponse) -> Result<(), AdmitError> {
        Ok(())
    }

    /// A due deadline fired (after `drain_due`'s validation against `next_deadline`). First
    /// converts `in_flight` entries older than `request_timeout` into `note_timeout(who)`;
    /// then (re)issues the pull to `select_sources`-sampled producers. A first pull sends
    /// the full missing-ordinal set to one producer; escalation stripes the missing set
    /// disjointly across `escalation_fanout` producers (union covers the hole once), so
    /// each ordinal goes to exactly one peer. Then re-arms at `min(backoff next interval,
    /// oldest in_flight + request_timeout)`, so a peer timeout is detected when it elapses,
    /// not when a later backoff deadline fires. A timeout-triggered wake re-requests
    /// without advancing the backoff ladder.
    pub(crate) fn on_deadline(&mut self, _id: &DataId) {}

    /// Certified execution results were processed. Runs the certification comparator: for
    /// each chunk we endorsed, our result hash vs the certified one. A mismatch emits a
    /// retroactive `Failed(CertifiedResultMismatch)`, attributed via the `DataAttribution`
    /// in `ProcessedLocally` (reputation + telemetry only, no refetch). Called upstream of
    /// the head advancement it causes, so the comparator sees the attribution before expiry
    /// can drain it.
    pub(crate) fn on_certified(&mut self) {}

    /// Chain heads / per-shard certified frontiers advanced. The executor coordinator,
    /// which advances the heads, supplies the final execution head and the per-shard
    /// certified frontier (only the shards that moved). Two range drains over the
    /// seed-time indexes, never a full scan or per-item chain query:
    /// - expiry: drain `items_by_height` at/below the final execution head;
    /// - witness gate: for each moved shard, drain `gated_by_frontier` up to
    ///   `new_frontier + witness_pull_margin` — each still-`Need` item flips to
    ///   `Collecting` and arms its speculative pull after `push_grace`.
    /// Runs after `on_certified` by causality (the final head only advances because
    /// certified results were processed).
    pub(crate) fn on_heads_advanced(&mut self) {}

    /// The executor tried to apply a shard's next block and found receipt proofs
    /// missing. This is the pull trigger for `ReceiptProof` items (they seed at
    /// `seed_block` but don't gate on the certified frontier): the reported items flip
    /// `Need` → `Collecting` and arm. Reaching the apply attempt already implies the
    /// source chunk executed, so any residual not-yet-produced skew is covered by backoff.
    pub(crate) fn on_receipt_apply_attempt(&mut self, _missing: &[DataId]) {}

    /// Consumer reported the outcome of *semantic* validation. `Verified` releases budgets
    /// and shrinks the item to its `DataAttribution` (`FetchState::ProcessedLocally`), kept
    /// until expiry (a later `CertifiedResultMismatch` must still attribute). `Failed`
    /// funnels into reputation via the retained attribution — the winning commitment's
    /// senders only (after expiry: a no-op) — then splits by state:
    /// - from `Delivered` (locally detected): ban the winning commitment and resume
    ///   `Collecting` from `residual` (deliver at once if a tracker holds ≥K, else re-arm).
    /// - from `ProcessedLocally` (`CertifiedResultMismatch`): reputation + telemetry only,
    ///   no refetch — a second endorsement would look like equivocation.
    pub(crate) fn on_verified(&mut self, _ev: VerifiedEvent) {}
    pub(crate) fn on_failed(&mut self, _ev: FailedEvent) {}

    /// Serve a pull request. Only `Item::Produce` entries serve (recovery pool =
    /// producers; recipients never serve). Invariant: a `Produce` item exists for every
    /// artifact we authored within `[final exec head, head]` (created by `seed_block`,
    /// restored by the startup walk), so an unknown-id NAK truthfully means "not mine or
    /// expired".
    /// 0. Validate every want-list (dedup, cap the count, ordinals < N) before any work;
    ///    batch entries resolve independently (entitlement/lane per entry).
    /// 1. Authorize + grant the lane: `Priority` iff `Validator` ∧ `who ∈
    ///    DataKind::recipients(..)` ∧ requested — else `Background` (`Anonymous`
    ///    route-back only). For code the recipient rule is the witness rule on the claimed
    ///    chunk plus `hash ∈ codes`.
    /// 2. Resolve: coded ⇒ produce the requested ordinals from the stored artifact (data
    ///    ordinals are byte slices; parity ordinals a measured-ms re-encode — no encode
    ///    cache; persisting parts at production makes this a store read and serves below
    ///    the live window). Blob ⇒ look up our `Witness{claimed chunk}` item, require
    ///    `hash ∈ codes`, read the code from the store. `Producing`, unknown id, or a
    ///    chunk we did not execute ⇒ signed `NotAvailable` NAK, never an error.
    /// 3. Respond on the granted lane; heavy work is offloaded to the priority pool. See
    ///    [`QosClass`].
    pub(crate) fn serve_request(
        &mut self,
        _req: SpiceDataRequest,
    ) -> Result<DataResponse, AdmitError> {
        Err(AdmitError::Irrelevant) // sketch
    }

    /// Epoch switched: GC reputation entries for accounts that left the producer sets.
    pub(crate) fn on_epoch_switch(&mut self) {}
}
