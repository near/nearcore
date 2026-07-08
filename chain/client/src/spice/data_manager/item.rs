//! Per-item identity, lifecycle state, assembly buffers, and sender attribution.

use super::QosClass;
use super::scheduler::Backoff;
use near_async::time::Instant;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use std::collections::HashMap;

/// Unified content id across all fetchable data types. Supersedes
/// `SpiceDataIdentifier` (which today has only the first two variants) incl. `ContractCode` variant.
/// Goes on the wire inside the versioned `SpiceDataRequest` — so this enum is versioned there.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DataId {
    /// Erasure-coded. Produced by chunk producers of `shard`, needed by its validators.
    Witness { block_hash: CryptoHash, shard_id: ShardId },
    /// Erasure-coded. Produced by `from` shard, needed by next-block producers of `to`.
    ReceiptProof { block_hash: CryptoHash, from_shard_id: ShardId, to_shard_id: ShardId },
    /// Whole-blob, content-addressed. Keyed by hash ONLY, so the same code accessed
    /// from several blocks/shards is a single fetch — the latest interested
    /// (block, shard) context lives on the item as [`FetchItem::anchor`]
    /// (it drives sources + expiry).
    ContractCode { code_hash: CodeHash },
}

impl DataId {
    /// Anchor block for coded kinds. `None` for contract code — its anchor lives on
    /// the [`FetchItem`].
    pub(crate) fn block_hash(&self) -> Option<&CryptoHash> {
        match self {
            DataId::Witness { block_hash, .. } | DataId::ReceiptProof { block_hash, .. } => {
                Some(block_hash)
            }
            DataId::ContractCode { .. } => None,
        }
    }

    /// Erasure-coded (K-of-N parts) vs a single content-addressed blob (K=1).
    pub(crate) fn transfer_unit(&self) -> TransferUnit {
        match self {
            DataId::Witness { .. } | DataId::ReceiptProof { .. } => TransferUnit::ErasureCoded,
            DataId::ContractCode { .. } => TransferUnit::Blob,
        }
    }
}

/// How the payload is transferred/assembled — the one axis that differs across kinds.
/// The scheduler, source selection and scoring do not care about this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferUnit {
    /// Reassemble from `K` of `N` Reed–Solomon parts, then hash-check + decode.
    ErasureCoded,
    /// A single blob whose hash *is* the id — verified on arrival, no decode. (K=1.)
    Blob,
}

/// The state of one tracked piece of data. The variant encodes its origin — produced
/// by others and fetched by us, or produced by us and served to others — and is
/// fixed for the item's lifetime.
pub(crate) enum Item {
    /// Produced by others; we fetch it.
    Fetch(FetchItem),
    /// Produced by us; we serve it.
    Produce(ProduceState),
}

/// Data we author. Producers hold the FULL data and can re-encode any/all ordinals on
/// (authorized) request — this is what makes single-source recovery possible.
///
/// Deliberately holds NO data or encoded parts: the artifact lives in the store, and
/// re-serving without re-encoding is the job of the manager's byte-budgeted
/// `EncodeCache` (keyed by `DataId`, populated for free at production time — the
/// parts vector already exists to build the commitment — and LRU-evicted under a
/// GLOBAL byte bound). A per-item cache field could not express a global bound, and
/// cache lifetime ≠ item lifetime in both directions: evictable under pressure long
/// before expiry, and servable after a restart when no item exists at all.
pub(crate) enum ProduceState {
    /// We are assigned to produce it; execution not finished yet.
    Producing,
    /// The produced artifact is in store; serve any requested units.
    Produced,
}

/// The consume-side lifecycle. `Have` is absent by construction: the
/// terminal signal is a role-appropriate *durable artifact* (see `DataKind::is_done`)
/// — our endorsement (a stateless validator never stores the witness itself), a
/// persisted receipt proof, or produced data. On a `Verified` verdict the item
/// shrinks to its attribution husk (`ProcessedLocally`); removal happens ONLY via
/// head-driven expiry (final execution head passes its height — abandoned forks
/// included).
#[derive(Debug)]
pub(crate) enum FetchState {
    /// Wanted, seeded from chain, but the existence gate hasn't opened and no unit has
    /// arrived yet. On a recent block we simply wait for the push here.
    Need,
    /// At least one unit obtained (⇒ it exists) OR the existence gate opened and we are
    /// speculatively pulling. Accumulating toward completion.
    Collecting(Assembly),
    /// Assembled bytes were handed to the consumer; parked until it reports back
    /// `Verified`/`Failed`. The (large) assembly is dropped, the (small) sender map
    /// is retained for attribution. Without this state a re-pushed part would re-enter
    /// `Collecting` and re-deliver — resurrecting the `recently_decoded_data` band-aid.
    Delivered,
    /// Consumer finished its local processing: the semantic check passed and the
    /// durable artifact is persisted (for a witness: executed it and endorsed the
    /// computed result). Only an attribution husk remains — budgets released,
    /// everything but `DataAttribution` dropped — retained until expiry, because
    /// *locally* is the operative word: endorsing = execute-and-sign, so a witness
    /// built on rotten-but-executable inputs (e.g. uncertified prior results) is only
    /// exposed when certification lands with a different result hash — a retroactive
    /// `Failed(CertifiedResultMismatch)` that must still attribute.
    ProcessedLocally,
}

/// The full state of one piece of data we are obtaining: its position in the lifecycle
/// ([`FetchState`], with the in-progress [`Assembly`] inside) plus everything
/// fetching it requires us to remember — relevance bounds (`height`, `anchor`),
/// who sent what (`attribution`), requests on the wire (`in_flight`), and retry
/// timing (`backoff`, `first_unit_at`, `next_deadline`). Identity is not state and
/// is not here: it lives in the map key, never duplicated.
pub(crate) struct FetchItem {
    pub(crate) state: FetchState,
    /// Resolved QoS lane: the max over this item's fetch *causes*, updated as causes
    /// register. Stored, not derived, because the `Background` cause (an RPC /
    /// state-sync request) is extrinsic — no chain state can re-derive it — and the
    /// resolved value is read on every deadline arm and admission check. Drives the
    /// scheduler tie-break and which byte budget the item's buffers count against
    /// (an upgrade mid-collection must re-charge buffered bytes to the new lane).
    pub(crate) qos: QosClass,
    /// Height of the anchor block, captured at seed time, so expiry
    /// (`on_heads_advanced`) and the admission window never do per-item store lookups.
    /// For contract code: the height of the highest interested block, bumped when a
    /// higher anchor replaces the current one.
    pub(crate) height: BlockHeight,
    /// Contract code only (`None` for coded kinds — their id carries the block): the
    /// highest-block (block, shard) context wanting this hash, replaced when a higher
    /// one seeds. The `ShardId` is the essential half — code is *state*, so the
    /// source pool is that shard's producers; the block hash only resolves the epoch
    /// for that derivation. A single anchor suffices: expiry needs only the max
    /// interested height (kept in `height` — all-anchors-passed ≡ max-passed), and
    /// one live anchor yields a complete source pool. HIGHEST, not earliest: the
    /// item must outlive the LAST interest — an earlier anchor would expire it while
    /// later blocks still await the code (a liveness bug), and the freshest block
    /// also names the most current epoch/shard mapping for sources. Forgone:
    /// cross-shard pool union, and expiry can run at-most-late if the kept anchor is
    /// an abandoned fork — both accepted. Engine-internal ONLY: delivery is
    /// content-addressed — completed code is handed downstream as (code_hash, bytes)
    /// and the consumer unblocks ALL chunks waiting on the hash (earliest included);
    /// routing delivery by any single block would starve the other waiters.
    pub(crate) anchor: Option<(CryptoHash, ShardId)>,
    /// Who gave us which unit — retained until the item EXPIRES,
    /// so late discoveries still map back to senders: a receipt proof failing
    /// once execution results land (`Delivered`), or a certified result contradicting
    /// our endorsement (`ProcessedLocally`). Uniform for all kinds.
    pub(crate) attribution: DataAttribution,
    /// Outstanding pull requests — a snapshot of what is on the wire RIGHT NOW, not
    /// history (failure memory is global, in [`super::Reputation`]). Entry lifecycle:
    /// created on send; removed on response/NAK; or, once older than
    /// `request_timeout`, converted into `note_timeout(who)` and removed — its
    /// ordinals thereby become missing-and-requestable again (timeout ⇒ re-request,
    /// from a freshly sampled source). While an entry lives it suppresses duplicates:
    /// its ordinals are not re-requested — relaxed during escalation, which
    /// deliberately over-requests the same missing set across several peers — and its
    /// peer is excluded from `select_sources` (never double-ask a peer still within
    /// its response window; this per-peer rule holds even during escalation).
    pub(crate) in_flight: Vec<InFlightRequest>,
    /// Retry/backoff bookkeeping — the single copy (the scheduler owns only deadlines).
    pub(crate) backoff: Backoff,
    /// When the first unit arrived — starts the `first_unit_pull_delay` clock: if the
    /// remaining pushes don't complete the assembly within ~1–2·T_rtt, pull the rest.
    pub(crate) first_unit_at: Option<Instant>,
    /// The currently armed deadline. `drain_due` validates popped heap entries against
    /// this (heap entries can't be removed, so completed/re-armed items leave stale
    /// entries behind — they are lazily discarded on pop).
    pub(crate) next_deadline: Option<Instant>,
}

/// One outstanding pull request to one peer.
pub(crate) struct InFlightRequest {
    pub(crate) who: AccountId,
    pub(crate) sent_at: Instant,
    /// Requested ordinals; empty ⇒ the whole blob.
    pub(crate) ordinals: Vec<u32>,
}

/// The accumulation buffer; one variant per [`TransferUnit`]. Held only from first unit
/// to *delivered to the consumer*, then dropped — the manager does not retain the
/// (potentially large) witness. Only the small [`DataAttribution`] lingers afterwards
/// (until expiry — see [`FetchState::ProcessedLocally`]).
pub(crate) enum Assembly {
    /// Coded kinds keep parallel trackers PER COMMITMENT (like today's
    /// `waiting_on_data` inner map): a malicious producer pushing parts under a
    /// fabricated commitment must not lock the item out of assembling under the honest
    /// one. Bounded by admission (a handful of commitments per item); whichever
    /// tracker reaches K wins, and garbage attribution targets the losing/garbage
    /// commitment's vouchers only. Parts live in the tracker alone — no shadow copy;
    /// ordinal→sender lives in [`DataAttribution`].
    Coded { trackers: HashMap<SpiceDataCommitment, CodedTracker> },
    /// Content-addressed blob (K=1): nothing accumulates — the first response whose
    /// `hash(bytes) == expected` completes and delivers in the same call, so the
    /// assembly never *holds* bytes. The variant is a waiting-marker, kept so
    /// `Collecting` has a uniform shape across kinds.
    Blob { expected: CodeHash },
}

/// Accumulates parts toward decoding under one claimed commitment.
pub(crate) struct CodedTracker {
    // tracker: ReedSolomonPartsTracker<SpiceData> — holds the part bytes and decodes
    // on K. `SpiceData` is today's wire-level erasure enum
    // (`data_distributor_actor.rs:207`: ReceiptProof | StateWitness — the RS payload
    // IS borsh(SpiceData), so the enum belongs to the wire format).
    // Kept concrete-by-name here but not as a field: a generic
    // `<DecodedT>` would climb Assembly → FetchState → FetchItem → Item and break
    // the one-map premise; the enum is the type-erasure that keeps the map uniform.
    // Constructed from the commitment (encoded_length) + producer count (N).
    /// Ordinals held — a cheap bitset so `missing_ordinals` never touches part buffers.
    pub(crate) have_ordinals: Vec<bool>,
}

impl Assembly {
    /// Whether some tracker holds enough to reconstruct (K parts) / we have the blob.
    pub(crate) fn is_complete(&self) -> bool {
        false // sketch
    }

    /// Ordinals still needed — the explicit request set for the next pull (requests
    /// always enumerate ordinals; the server never decides what to send). Computed as
    /// the UNION of gaps across trackers: an ordinal is skipped only if held under
    /// EVERY commitment. Requests are commitment-agnostic (the server answers under
    /// whatever commitment it holds), so computing against the part-majority
    /// commitment would let an attacker's fake majority starve the honest tracker —
    /// its chosen gap would bound our request set. Single-commitment case unchanged;
    /// under attack the over-request is bounded by the admission cap on commitments.
    /// Empty for blob.
    pub(crate) fn missing_ordinals(&self) -> Vec<u32> {
        Vec::new() // sketch
    }
}

/// Which sender contributed which unit, *per commitment* — senders vouch for the
/// commitment they sent parts under. This is what lets a fault (immediate or late)
/// be pinned on accounts.
#[derive(Default)]
pub(crate) struct DataAttribution {
    /// Coded: commitment → (ordinal → sender). In recovery one sender may serve
    /// ordinals it did not originally push; attribution targets the transport sender
    /// for bad *bytes* (Merkle failure) and the commitment's vouchers for a *garbage
    /// decode* — never everyone who ever sent a part for the item.
    pub(crate) coded: HashMap<SpiceDataCommitment, HashMap<u32, AccountId>>,
    /// Blob: the responder.
    pub(crate) blob_sender: Option<AccountId>,
}

impl DataAttribution {
    /// Vouchers of one commitment — the culprit set if *its* decode yields garbage.
    pub(crate) fn vouchers(&self, _commitment: &SpiceDataCommitment) -> Vec<AccountId> {
        Vec::new() // sketch
    }

    /// Everyone who contributed any unit — the culprit set for a semantic `Failed`
    /// on the delivered (winning-commitment) data.
    pub(crate) fn all(&self) -> Vec<AccountId> {
        Vec::new() // sketch
    }
}
