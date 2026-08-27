//! Per-item identity, lifecycle state, assembly buffers, and sender attribution.

use super::AdmitError;
use super::Lane;
use super::scheduler::Backoff;
use crate::spice::data_distributor_actor::SpiceData;
use near_async::time::{Clock, Instant};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::MerklePath;
use near_primitives::reed_solomon::{ReedSolomonEncoder, ReedSolomonPartsTracker};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::{AccountId, BlockHeight, ShardId, SpiceChunkId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Unified content id across all fetchable data types. Goes on the wire inside the
/// versioned `SpiceDataRequest`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DataId {
    /// Coded. The chunk it validates; produced by that chunk's producers, needed by its
    /// validators.
    Witness(SpiceChunkId),
    /// Coded. `source` is the (block, from-shard) chunk whose execution produced the
    /// receipts; `to_shard` is the destination. Produced by `source`'s producers, needed
    /// by next-block producers of `to_shard`.
    ReceiptProof { source: SpiceChunkId, to_shard: ShardId },
    /// Whole blob, content-addressed. Keyed by hash only ⇒ one item (one fetch) per hash
    /// however many blocks/shards need it; extra interests only update
    /// [`FetchItem::anchor`], and delivery unblocks all waiters.
    ContractCode { code_hash: CodeHash },
}

impl DataId {
    /// Anchor block for coded kinds. `None` for contract code — its anchor is on the
    /// [`FetchItem`].
    pub(crate) fn block_hash(&self) -> Option<&CryptoHash> {
        match self {
            DataId::Witness(chunk) => Some(&chunk.block_hash),
            DataId::ReceiptProof { source, .. } => Some(&source.block_hash),
            DataId::ContractCode { .. } => None,
        }
    }

    /// Erasure-coded (K-of-N) vs a single content-addressed blob (K=1).
    pub(crate) fn transfer_unit(&self) -> TransferUnit {
        match self {
            DataId::Witness(_) | DataId::ReceiptProof { .. } => TransferUnit::ErasureCoded,
            DataId::ContractCode { .. } => TransferUnit::Blob,
        }
    }
}

/// How the payload is assembled — the one axis that differs across kinds. The scheduler,
/// source selection and scoring don't care about this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferUnit {
    /// Reassemble from K of N Reed–Solomon parts; decode + hash check run inside the
    /// tracker.
    ErasureCoded,
    /// A single blob whose hash *is* the id — verified on arrival, no decode. (K=1.)
    Blob,
}

/// One tracked piece of data: produced by others and fetched by us, or produced by us
/// and served. A node in both roles for one id (producer-validator; from-shard producer
/// that is a to-shard next-block producer) is `Produce`: consumer duties for
/// self-authored data are met from its own execution/store, not via delivery — mirroring
/// the push side, where producers aren't recipients.
pub(crate) enum Item {
    /// Produced by others; we fetch it.
    Fetch(FetchItem),
    /// Produced by us; we serve it.
    Produce(ProduceState),
}

/// Data we author. Holds no payload bytes (`codes` is an index). Clockless: serving is
/// reactive and expiry head-driven, so there are no produce-side deadlines. Born in
/// `seed_block`, flipped to `ReadyToServe` by `on_produced`.
pub(crate) enum ProduceState {
    /// Assigned to produce it; execution not finished yet.
    Producing,
    /// Artifact in store; serve any requested units. `codes` (witness items only, empty
    /// otherwise) is the set of contract hashes the chunk accessed — the serve-side index
    /// for code pulls claiming this chunk (`WantUnits::Blob { chunk }`). Filled by
    /// `on_produced`, re-read from the store by `seed_block` on restart. `served` counts the
    /// outbound budget per requester (`Budgets::per_item_requester_serve_bytes`).
    ReadyToServe { codes: HashSet<CodeHash>, served: HashMap<AccountId, u64> },
}

/// The consume-side lifecycle. `Have` is absent by construction: the store is the source
/// of truth for done-ness — the consumer persists the verified data (endorsement,
/// receipt proof, produced artifact; see `DataKind::is_done`) and the engine consults
/// that. A `Verified` verdict shrinks the item to its `DataAttribution`
/// (`ProcessedLocally`); removal happens only via head-driven expiry.
pub(crate) enum FetchState {
    /// Wanted (seeded from chain) but the existence gate is closed and no unit has
    /// arrived. On a recent block we just wait for the push here.
    WaitingForPush,
    /// At least one unit obtained (⇒ it exists), or the gate opened and we're
    /// speculatively pulling. A completing coded insert parks the item in `Delivered` in
    /// the same call; a blob is verified on arrival and terminal (delivered to the
    /// validator actor, item removed).
    Collecting(Assembly),
    /// Assembled bytes handed to the consumer; parked until it reports `Verified`/
    /// `Failed`. Coded kinds only. The decoded tracker's part bytes are dropped (and their
    /// budget released) here, leaving a small [`DataAttribution`]; without this state a
    /// re-pushed part would re-deliver.
    /// The incomplete trackers are kept as `residual` until the verdict (empty without
    /// equivocation). A semantic `Failed` blames the decoded commitment's senders only,
    /// bans it, and resumes `Collecting` from `residual` — incomplete by construction, so
    /// the pull re-schedules immediately. `Verified` drops `residual`.
    Delivered { attribution: DataAttribution, residual: Assembly },
    /// Consumer finished local processing and persisted the durable artifact. Only the
    /// [`DataAttribution`] remains, kept until expiry: a witness built on rotten-but-
    /// executable inputs surfaces only when certification lands a different result hash — a
    /// retroactive `Failed(CertifiedResultMismatch)`, reputation + telemetry only (we may
    /// not endorse a second result for the chunk). Terminal until expiry.
    ProcessedLocally { attribution: DataAttribution },
}

/// Everything obtaining one piece of data requires: its lifecycle position
/// ([`FetchState`], holding the in-progress [`Assembly`] or the post-delivery
/// [`DataAttribution`]), relevance bounds (`height`, `anchor`), requests on the wire
/// (`in_flight`), and retry timing. Sender attribution is not a separate field — it lives
/// in the trackers during `Collecting` and materializes as `DataAttribution` at delivery.
/// Identity is the map key, not duplicated here.
pub(crate) struct FetchItem {
    pub(crate) state: FetchState,
    /// Lane, fixed at item creation (max over the causes present then). Drives the
    /// scheduler tie-break and which byte budget the buffers count against. No mid-life
    /// updates: a validator-key hot-swap tolerates a stale lane until expiry.
    pub(crate) lane: Lane,
    /// Lifetime field (all kinds), captured at seed time so expiry and the admission
    /// window read a scalar, never a store lookup. For contract code it's the
    /// denormalized height of `anchor`'s block, kept in sync on anchor bumps.
    pub(crate) height: BlockHeight,
    /// Contract code only (`None` for coded kinds). Producer-derivation context: the
    /// (block, shard) that resolves code's source pool, and the claimed chunk carried in
    /// pulls (`WantUnits::Blob { chunk }`), which a server only honors for chunks it
    /// executed. Set at seeding (only if absent); re-aimed at witness delivery from the
    /// witness's embedded accesses list, latest wins (delivery proves the chunk executed);
    /// unverified accesses never re-aim. A stale accesses-set anchor at worst expires the
    /// item early; re-seeding covers it. `height` re-syncs on change and may decrease
    /// (stale `items_by_height` entries handled by the lazy drain).
    pub(crate) anchor: Option<SpiceChunkId>,
    /// Outstanding pull requests — a snapshot of what's on the wire now, not history
    /// (failure memory is global, in [`super::Reputation`]). An entry is removed on
    /// response/NAK, or, once older than `request_timeout`, converted into
    /// `note_timeout(source)` and removed (its ordinals become requestable again). While it
    /// lives it suppresses duplicate requests.
    pub(crate) in_flight: Vec<InFlightRequest>,
    /// Retry/backoff bookkeeping — the single copy (the scheduler owns only deadlines).
    pub(crate) backoff: Backoff,
    /// When the first unit arrived — starts the `pull_delay_after_first_unit` clock.
    /// Survives a failed verdict (the resumed pull is already due); cleared only when a
    /// ban leaves no parts held, since the only evidence was the banned commitment's own.
    pub(crate) first_unit_at: Option<Instant>,
    /// The currently scheduled deadline. [`super::SpiceDataManager::due_items`] validates
    /// popped heap entries against this and discards stale ones (heap entries can't be
    /// removed).
    pub(crate) next_deadline: Option<Instant>,
}

// Illustrative surface — bodies omitted in the sketch.
impl FetchItem {
    /// Starts a speculative pull: a waiting item begins collecting before any part
    /// arrived. Does nothing unless the item is waiting for the push.
    pub(crate) fn start_pulling(&mut self, _encoder: Arc<ReedSolomonEncoder>) -> bool {
        false // sketch
    }

    /// Opens a waiting item on its first part; a completing part parks the item in
    /// `Delivered` in the same call (decode + hash check run inside the tracker).
    /// `NotCollecting` means the item is parked awaiting a verdict or already processed.
    pub(crate) fn insert_part(
        &mut self,
        _clock: &Clock,
        _encoder: &Arc<ReedSolomonEncoder>,
        _sender: &AccountId,
        _verified: VerifiedCodedPart,
    ) -> Result<PartInsertResult, AdmitError> {
        Err(AdmitError::Irrelevant) // sketch
    }

    /// Consumer verified and persisted the artifact; shrinks the item to its attribution.
    pub(crate) fn mark_verified(&mut self) -> Result<(), AdmitError> {
        Ok(()) // sketch
    }

    /// Consumer rejected the delivered data: bans the decoded commitment on the residual,
    /// resumes `Collecting` from it, and returns the senders to blame.
    pub(crate) fn mark_failed(&mut self) -> Result<HashSet<AccountId>, AdmitError> {
        Ok(HashSet::new()) // sketch
    }
}

/// One outstanding pull request to one peer.
pub(crate) struct InFlightRequest {
    pub(crate) source: AccountId,
    pub(crate) sent_at: Instant,
    /// Requested ordinals; empty ⇒ the whole blob.
    pub(crate) ordinals: Vec<u32>,
}

/// A coded part whose merkle proof was verified against its commitment's root;
/// [`Self::verify`] is the only way to construct one, so a mismatched
/// (commitment, part) insert is unrepresentable.
pub(crate) struct VerifiedCodedPart {
    commitment: SpiceDataCommitment,
    /// Leaf count of the tree the proof was verified against; the assembly accepts a
    /// part only if this equals its encoder's width.
    total_parts: usize,
    ordinal: usize,
    part: Box<[u8]>,
}

impl VerifiedCodedPart {
    pub(crate) fn verify(
        _commitment: &SpiceDataCommitment,
        _total_parts: usize,
        _ordinal: u64,
        _part: Box<[u8]>,
        _merkle_proof: &MerklePath,
    ) -> Result<Self, AdmitError> {
        Err(AdmitError::InvalidMerkleProof) // sketch
    }
}

/// The accumulation buffer, held from first unit to delivery. Delivery drops the decoded
/// tracker's part bytes and carries the incomplete trackers as `Delivered::residual`
/// until the verdict (usually empty); only the small [`DataAttribution`] lingers past it.
///
/// Coded vs blob is an *addressing-model* difference, not a parts-count one (K=N vs K=1),
/// which is why they stay separate variants:
/// - Coded: the commitment is discovered from the parts, and competing ones can arrive at
///   once (equivocation) — the per-commitment machinery exists to disambiguate it.
/// - Blob: the id is the hash, so there's one known commitment; non-matching bytes are
///   rejected on arrival and none of that machinery applies.
pub(crate) enum Assembly {
    Coded {
        encoder: Arc<ReedSolomonEncoder>,
        /// One tracker per commitment, so a fake commitment can't block the honest one;
        /// first to K wins, and a bad decode blames only that commitment's senders.
        /// Unsolicited units are admitted only for the sender's own ordinal, so completing
        /// a tracker unsolicited takes ≥K distinct providers. Each sender may provide only
        /// one commitment, so trackers ≤ sender count — no separate limit; memory is
        /// bounded by the admission byte budgets.
        trackers: HashMap<SpiceDataCommitment, CodedTracker>,
        /// Commitments rejected for this item — a failed consumer verdict or a garbage
        /// decode. Units for them are rejected on arrival, so the same bad data is never
        /// re-delivered. Not counted toward any limit (counting it would let bad entries
        /// crowd out the honest commitment). Usually empty.
        banned: HashSet<SpiceDataCommitment>,
        /// The one commitment each sender provided parts for. Outlives the trackers, so a
        /// sender whose commitment was dropped as garbage cannot loop through fresh
        /// commitments; the conflict check runs against this map.
        commitment_by_sender: HashMap<AccountId, SpiceDataCommitment>,
    },
    /// K=1: nothing accumulates — the first response whose `hash(bytes) == code_hash`
    /// completes and delivers in the same call, so the assembly never holds bytes. A
    /// marker, so `Collecting` has a uniform shape; the expected hash is the `DataId`.
    Blob,
}

impl Assembly {
    /// Where a received part enters the assembly. Rejected (the manager then reports the
    /// sender): a banned commitment, or one that differs from the commitment this sender
    /// already provided (`commitment_by_sender`). A completing insert returns the decoded
    /// data (hash-checked inside the tracker); K parts that yield no data matching the
    /// committed hash come back as `Garbage` — the tracker is dropped and the commitment
    /// banned in the same call. Coded only.
    pub(crate) fn insert_part(
        &mut self,
        _sender: &AccountId,
        _verified: VerifiedCodedPart,
    ) -> Result<PartInsertResult, AdmitError> {
        Err(AdmitError::ConflictingCommitment) // sketch
    }

    /// Whether some tracker holds K parts. Never observed true outside the completing
    /// insert (which parks the item); blob completion is synchronous on arrival.
    pub(crate) fn is_complete(&self) -> bool {
        false // sketch
    }

    /// Ordinals still needed — the explicit request set for the next pull. The union of
    /// gaps across trackers (skip an ordinal only if held under every commitment):
    /// requests are commitment-agnostic, so computing against the part-majority commitment
    /// would let a fake majority starve the honest tracker. Empty for blob.
    pub(crate) fn missing_ordinals(&self) -> Vec<u32> {
        Vec::new() // sketch
    }
}

/// One insert's outcome, surfaced to the manager for budgeting, timing, and reporting.
pub(crate) enum PartInsertResult {
    Accepted,
    Duplicate,
    /// The commitment decoded to this data and it matches the committed hash; the item
    /// parked itself in `Delivered`.
    Complete(SpiceData),
    /// The commitment reached K parts but yielded no data matching its hash — a failed
    /// decode or a hash mismatch. Its tracker is gone and it is banned; the accounts
    /// listed provided it and stay bound to it, so they cannot open another commitment
    /// for this item. Reporting them onward is the manager's.
    Garbage {
        contributors: HashSet<AccountId>,
    },
}

/// Accumulates parts toward decoding under one claimed commitment, and — the same struct,
/// folded in — records who sent each ordinal. On reaching K it decodes and checks the
/// result's hash against `commitment` (which it holds, equal to its map key), so a
/// completion is always deliverable.
///
/// Byte accounting for [`super::AdmissionControl::release`] needs no extra field: parts are
/// fixed-length (`reed_solomon_part_length` = `ceil(encoded_length / K)`), so a sender's
/// charge is its count in `senders` × that length, and the lane total is
/// `parts.total_parts_size()`.
// No `Debug`: `ReedSolomonPartsTracker` has none.
pub(crate) struct CodedTracker {
    /// Holds the part bytes and decodes on K. `SpiceData` (an enum over the two coded
    /// payloads, as today) keeps this monomorphic, so no generic climbs into `Item`. Also
    /// the byte accounting: `total_parts_size()` is what we hold, `encoded_length()` the
    /// expected total.
    pub(crate) parts: ReedSolomonPartsTracker<SpiceData>,
    /// Per-ordinal sender: `Some` ⇒ we hold that ordinal (so `missing_ordinals` reads it
    /// without touching part buffers) and records the sender for attribution. This is the
    /// transport sender — used directly for Merkle faults and collectively (all `Some`)
    /// for a garbage decode. At delivery the decoded tracker's `senders` moves into the
    /// state's [`DataAttribution`]; the part bytes are dropped.
    pub(crate) senders: Vec<Option<AccountId>>,
    /// The commitment the parts are tracked under; equals this tracker's map key.
    pub(crate) commitment: SpiceDataCommitment,
}

/// Who to blame for a late fault on the delivered data. Materializes at delivery from the
/// decoded tracker (before that, per-ordinal senders live in the trackers). Coded kinds
/// only — a blob is verified on arrival and never reaches `Delivered`/`ProcessedLocally`,
/// so `decoded` is a plain commitment, not an `Option`. Kept until expiry; blames the
/// decoded commitment's senders only, never those under other commitments (who may be
/// the honest side of an equivocation race).
#[derive(Debug)]
pub(crate) struct DataAttribution {
    /// The commitment whose tracker reached K and was delivered.
    pub(crate) decoded: SpiceDataCommitment,
    /// The decoded tracker's per-ordinal sender vector, moved out at delivery.
    pub(crate) senders: Vec<Option<AccountId>>,
}

impl DataAttribution {
    /// The distinct accounts that contributed the delivered data — the set to hold
    /// accountable for a fault on it (fed to `reputation.report`); no "everyone who ever
    /// sent a part" set can form.
    pub(crate) fn contributors(&self) -> HashSet<AccountId> {
        HashSet::new() // sketch
    }
}
