//! SKETCH. Per-type config for the one fetch engine. Everything generic —
//! scheduling, source selection, admission, scoring, expiry — lives in the engine;
//! only the knobs here differ per kind. Contract code is a third impl, not a new
//! pipeline. Dispatch is a static `match` on `DataId` (three fixed kinds), so
//! `verify_assembled` can return the typed decoded value.

use super::item::{DataId, TransferUnit};
use super::reputation::Misbehavior;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, SpiceChunkId};

/// Seed-time classification of a would-be item, consulted once by `seed_block`.
/// Afterwards the gate is the `FetchState` position (`Need` vs `Collecting`), advanced
/// by events. Tri-state: the two "don't fetch yet" answers differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interest {
    /// Neither needed nor produced here — no item is created.
    NotNeeded,
    /// Needed, but the existence gate is closed: the shard's certified frontier hasn't
    /// reached `height - witness_pull_margin` yet, so the chunk is likely not executed.
    /// Seed as `Need`, register the (shard, height) gate in `gated_by_frontier`, and wait
    /// for the push rather than pulling.
    WaitForPush,
    /// Needed and plausibly produced (catch-up, or contract code). Seed straight into
    /// `Collecting` and arm the speculative pull.
    Fetchable,
}

/// Fetch-side context the id alone can't carry. Today only the contract-code anchor.
pub(crate) struct FetchContext<'a> {
    pub(crate) anchor: Option<&'a SpiceChunkId>,
}

/// One data type's config. Fetch-side questions (`sources`, `classify_at_seed`) may
/// read our fetch state via [`FetchContext`]; serve-side (`recipients`) must be
/// answerable from the request alone (for code, the claimed chunk in `WantUnits::Blob`).
pub(crate) trait DataKind {
    /// The source pool — candidates that can serve this item (all hold the full data);
    /// sampled by `select_sources`. Derived from epoch info. For code: producers of the
    /// anchor's shard.
    fn sources(&self, id: &DataId, ctx: &FetchContext)
    -> Result<Vec<AccountId>, near_chain::Error>;

    /// Who the item is pushed to, and therefore who may pull it on the `Priority` lane.
    /// Witness: the shard's assigned validators; receipt proof: next-block producers of
    /// `to`; code: the witness rule applied to `claimed_chunk` (`None` ⇒ empty), with
    /// `hash ∈ codes` checked separately in `serve_request`. Failing the check demotes
    /// to `Background`, it does not deny.
    fn recipients(
        &self,
        id: &DataId,
        claimed_chunk: Option<&SpiceChunkId>,
    ) -> Result<Vec<AccountId>, near_chain::Error>;

    /// Erasure-coded (K-of-N) vs whole blob (K=1, content-addressed).
    fn transfer_unit(&self) -> TransferUnit;

    /// Does this node need the item, and is its existence gate open? Consulted once, at
    /// seed time (see [`Interest`]); afterwards the gate is event-driven.
    fn classify_at_seed(
        &self,
        id: &DataId,
        ctx: &FetchContext,
    ) -> Result<Interest, near_chain::Error>;

    /// Distribution-level verification on completion: coded ⇒ decode + `hash ==
    /// commitment`; blob ⇒ `hash(bytes) == code_hash`. `Err` names the culprit
    /// misbehavior (attributed to the failing commitment's senders). Semantic
    /// validation is not here — it is consumer-side (`FailedEvent`). The real impl
    /// returns the typed decoded value so the consumer never re-deserializes.
    fn verify_assembled(&self, id: &DataId, bytes: &[u8]) -> Result<(), Misbehavior>;

    /// Is the durable artifact that means "done with this item" present? The real
    /// `Have` — not "raw data stored"; a stateless validator never keeps the witness.
    /// Done = our endorsement exists (or the chunk is certified); receipt = proof
    /// persisted; authored data = artifact in store. Persistence is consumer-side.
    ///
    /// A store read: consulted at seed and completion only, never per part. Gates
    /// seeding on the fetch side; on the produce side it selects the state
    /// (`ReadyToServe` vs `Producing`) and must not suppress the item. Contract code is
    /// a cache lookup, so an eviction may legitimately re-need an item — resurrection
    /// is expected, not a bug.
    fn is_done(&self, id: &DataId) -> Result<bool, near_chain::Error>;
}

/// `Witness(chunk)` — coded; sources = the chunk's producers; recipients = its
/// assigned validators; need = we are one and the chunk isn't certified yet (endorsing
/// is its only purpose); seed = block processed.
pub(crate) struct WitnessKind;

/// `ReceiptProof{source, to_shard}` — coded; sources = producers of the source shard;
/// recipients = next-block producers of `to_shard`; need = we apply `to_shard` next
/// block; existence = the source chunk executed. Seeded at `seed_block`; the executor's
/// apply-attempt missing-set only triggers pulls, it does not create items.
pub(crate) struct ReceiptProofKind;

/// `ContractCode{code_hash}` — whole blob (K=1), content-addressed, so interest from
/// several blocks/shards is one item (the (block, shard) anchor lives on the
/// `FetchItem`). sources = producers of the anchor's shard; pulls carry the anchor as
/// the claimed chunk (`Blob { chunk }`); need = accesses or a delivered witness name a
/// hash missing from the compiled-contract cache; verify = `hash(bytes) == code_hash`;
/// delivery unblocks all chunks waiting on the hash.
pub(crate) struct ContractCodeKind;

// impl DataKind for {WitnessKind, ReceiptProofKind, ContractCodeKind} — bodies omitted.
// Only these impls differ; the engine is unchanged across them.

/// Seed one `ContractCode` item per accessed hash missing from the compiled-contract
/// cache (checked in-engine via the injected `code_cached(epoch, hash)` handle). Anchor
/// set only if absent; no accesses retained. The witness's embedded list is the
/// authoritative re-seed at delivery (see `FetchItem::anchor`); accesses that arrive
/// before the block park in [`super::OrphanPool`] and run here at `seed_block`.
pub(crate) fn seed_contract_code_items(
    _block_hash: CryptoHash,
    _accessed: &[near_primitives::stateless_validation::contract_distribution::CodeHash],
) -> Vec<DataId> {
    Vec::new() // sketch
}
