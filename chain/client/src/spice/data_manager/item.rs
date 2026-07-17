//! Per-item identity, lifecycle state, assembly buffers, and sender attribution.

use super::AdmitError;
use super::QosClass;
use super::scheduler::Backoff;
use near_async::time::Instant;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::{AccountId, BlockHeight, ShardId, SpiceChunkId};
use std::collections::{HashMap, HashSet};

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
    /// Reassemble from K of N Reed–Solomon parts, then hash-check + decode.
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
    /// `on_produced`, re-read from the store by `seed_block` on restart.
    ReadyToServe { codes: HashSet<CodeHash> },
}

/// The consume-side lifecycle. `Have` is absent by construction: the terminal signal is a
/// durable artifact (see `DataKind::is_done`) — our endorsement, a persisted receipt
/// proof, or produced data — not the raw data. A `Verified` verdict shrinks the item to
/// its `DataAttribution` (`ProcessedLocally`); removal happens only via head-driven
/// expiry.
#[derive(Debug)]
pub(crate) enum FetchState {
    /// Wanted (seeded from chain) but the existence gate is closed and no unit has
    /// arrived. Registered in `gated_by_frontier`; on a recent block we just wait for the
    /// push here.
    Need,
    /// At least one unit obtained (⇒ it exists), or the gate opened and we're
    /// speculatively pulling. On completion, coded kinds move to `Delivered`; a blob is
    /// verified on arrival and terminal (delivered to the validator actor, item removed).
    Collecting(Assembly),
    /// Assembled bytes handed to the consumer; parked until it reports `Verified`/
    /// `Failed`. Coded kinds only. The winning tracker's part bytes are dropped, leaving a
    /// small [`DataAttribution`]; without this state a re-pushed part would re-deliver.
    /// Non-winning trackers are kept as `residual` until the verdict (empty without
    /// equivocation). A semantic `Failed` blames the winning senders only, bans `winning`,
    /// and resumes `Collecting` from `residual` (deliver at once if a tracker already
    /// holds ≥K, else re-arm the pull). `Verified` drops `residual`.
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
    /// QoS lane, fixed at item creation (max over the causes present then). Drives the
    /// scheduler tie-break and which byte budget the buffers count against. No mid-life
    /// updates: a validator-key hot-swap tolerates a stale lane until expiry.
    pub(crate) qos: QosClass,
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
    /// Commitments that were delivered and then failed the consumer's check. Units for
    /// them are rejected on arrival, so we never re-deliver the same bad data. Not counted
    /// toward any limit (counting it would let bad entries crowd out the honest
    /// commitment). Usually empty.
    pub(crate) banned_commitments: HashSet<SpiceDataCommitment>,
    /// Outstanding pull requests — a snapshot of what's on the wire now, not history
    /// (failure memory is global, in [`super::Reputation`]). An entry is removed on
    /// response/NAK, or, once older than `request_timeout`, converted into
    /// `note_timeout(who)` and removed (its ordinals become requestable again). While it
    /// lives it suppresses duplicate requests.
    pub(crate) in_flight: Vec<InFlightRequest>,
    /// Retry/backoff bookkeeping — the single copy (the scheduler owns only deadlines).
    pub(crate) backoff: Backoff,
    /// When the first unit arrived — starts the `first_unit_pull_delay` clock.
    pub(crate) first_unit_at: Option<Instant>,
    /// The currently armed deadline. `drain_due` validates popped heap entries against
    /// this and discards stale ones (heap entries can't be removed).
    pub(crate) next_deadline: Option<Instant>,
}

/// One outstanding pull request to one peer.
pub(crate) struct InFlightRequest {
    pub(crate) who: AccountId,
    pub(crate) sent_at: Instant,
    /// Requested ordinals; empty ⇒ the whole blob.
    pub(crate) ordinals: Vec<u32>,
}

/// The accumulation buffer, held from first unit to delivery. Delivery drops the winning
/// tracker's part bytes and carries the non-winning trackers as `Delivered::residual`
/// until the verdict (usually empty); only the small [`DataAttribution`] lingers past it.
///
/// Coded vs blob is an *addressing-model* difference, not a parts-count one (K=N vs K=1),
/// which is why they stay separate variants:
/// - Coded: the commitment is discovered from the parts, and competing ones can arrive at
///   once (equivocation) — the per-commitment machinery exists to disambiguate it.
/// - Blob: the id is the hash, so there's one known commitment; non-matching bytes are
///   rejected on arrival and none of that machinery applies.
#[derive(Debug)]
pub(crate) enum Assembly {
    /// One tracker per commitment, so a fake commitment can't block the honest one; first
    /// to K wins, and a bad decode blames only that commitment's senders. Unsolicited
    /// units are admitted only for the sender's own ordinal, so completing a tracker
    /// unsolicited takes ≥K distinct backers. Each producer may back only one commitment,
    /// so trackers ≤ producer count — no separate limit; memory is bounded by the
    /// admission byte budgets.
    Coded { trackers: HashMap<SpiceDataCommitment, CodedTracker> },
    /// K=1: nothing accumulates — the first response whose `hash(bytes) == code_hash`
    /// completes and delivers in the same call, so the assembly never holds bytes. A
    /// marker, so `Collecting` has a uniform shape; the expected hash is the `DataId`.
    Blob,
}

/// Accumulates parts toward decoding under one claimed commitment, and — the same struct,
/// folded in — records who sent each ordinal.
#[derive(Debug)]
pub(crate) struct CodedTracker {
    // Holds the part bytes and decodes on K, sized from the commitment (encoded_length)
    // and producer count (N). The decoded payload type is erased here (a generic would
    // climb Assembly → FetchState → FetchItem → Item and break the one-map premise).
    /// Per-ordinal sender: `Some` ⇒ we hold that ordinal (so `missing_ordinals` reads it
    /// without touching part buffers) and records the sender for attribution. This is the
    /// transport sender — used directly for Merkle faults and collectively (all `Some`)
    /// for a garbage decode. At delivery the winning tracker's `senders` moves into the
    /// state's [`DataAttribution`]; the part bytes are dropped.
    pub(crate) senders: Vec<Option<AccountId>>,
}

impl Assembly {
    /// Where a received part's commitment enters the assembly. Rejected (the manager then
    /// reports the sender): a banned commitment, or one that differs from a commitment
    /// this sender already backed. Otherwise returns the tracker, creating it on first
    /// sight. No count limit — one commitment per sender already bounds the trackers.
    /// Coded only.
    pub(crate) fn tracker_for(
        &mut self,
        _commitment: &SpiceDataCommitment,
        _sender: &AccountId,
        _banned: &HashSet<SpiceDataCommitment>,
    ) -> Result<&mut CodedTracker, AdmitError> {
        Err(AdmitError::ConflictingCommitment) // sketch
    }

    /// Whether some tracker holds K parts. Blob completion is synchronous on arrival, so a
    /// `Blob` assembly is never observed complete here.
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

/// Who to blame for a late fault on the delivered data. Materializes at delivery from the
/// winning tracker (before that, per-ordinal senders live in the trackers). Coded kinds
/// only — a blob is verified on arrival and never reaches `Delivered`/`ProcessedLocally`,
/// so `winning` is a plain commitment, not an `Option`. Kept until expiry; blames the
/// winning commitment's senders only, never those under losing commitments (who may be
/// the honest side of an equivocation race).
#[derive(Debug)]
pub(crate) struct DataAttribution {
    /// The commitment whose tracker reached K and was delivered.
    pub(crate) winning: SpiceDataCommitment,
    /// The winning tracker's per-ordinal sender vector, moved out at delivery.
    pub(crate) senders: Vec<Option<AccountId>>,
}

impl DataAttribution {
    /// The distinct accounts that contributed the delivered data — the set to hold
    /// accountable for a fault on it (fed to `reputation.report`). The only accessor by
    /// design, so no "everyone who ever sent a part" set can form.
    pub(crate) fn contributors(&self) -> Vec<AccountId> {
        Vec::new() // sketch
    }
}
