//! SKETCH. The pluggable per-type surface of the one fetch engine. Everything
//! generic — scheduling, source selection, admission, scoring, expiry — lives in the
//! engine; only the knobs below differ per data kind. Adding contract code
//! is implementing this trait for a third type, not a new pipeline.
//!
//! Dispatch is a static `match` on the `DataId` discriminant — with exactly three
//! fixed kinds, `Box<dyn DataKind>` buys nothing, and static dispatch lets
//! `verify_assembled` hand the *typed* decoded value to the consumer instead of
//! re-borrowing bytes. The trait documents the seam.

use super::item::{DataId, TransferUnit};
use super::reputation::Misbehavior;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;

/// The engine's interest in an item — tri-state because the two "no" answers have
/// different consequences: `NotNeeded` drops the item, `WaitForPush` keeps it parked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interest {
    /// This node neither needs nor produces the item.
    NotNeeded,
    /// Needed, but the existence gate hasn't opened (execution hasn't plausibly
    /// reached the block for this shard) — don't pull, the push is coming. Also
    /// needed for reputation correctness: premature pulls time out against innocent
    /// producers and pollute the liveness channel.
    WaitForPush,
    /// Needed and plausibly produced — arm the pull.
    Fetchable,
}

/// One data type's configuration.
pub(crate) trait DataKind {
    /// Candidate producers that can serve this item (all hold the full data). Derived
    /// from epoch info; identical shape across kinds, different lookup. For contract
    /// code: the producers of the anchor's shard (code is state — that shard's
    /// producers hold it; the anchor block resolves the epoch).
    fn sources(&self, id: &DataId) -> Result<Vec<AccountId>, near_chain::Error>;

    /// Serve-side authorization: may `who` receive this item? The
    /// consumer-side mirror of `sources` — witness/code: assigned chunk validators of
    /// the shard; receipt proof: next-block producers of the `to` shard. Derived from
    /// epoch info, like `sources`. Checked by `serve_request` before serving a
    /// `Validator` requester; `NonValidator` requesters bypass entitlement but are
    /// route-back only, on the `Background` lane.
    fn is_entitled(&self, id: &DataId, who: &AccountId) -> Result<bool, near_chain::Error>;

    /// Erasure-coded (K-of-N) vs whole blob (K=1, content-addressed).
    fn transfer_unit(&self) -> TransferUnit;

    /// Does *this node* need this item, and has its existence gate opened?
    fn interest(&self, id: &DataId) -> Result<Interest, near_chain::Error>;

    /// Distribution-level verification done *inside* the engine on completion:
    /// coded ⇒ decode + `hash == commitment`; blob ⇒ `hash(bytes) == code_hash`.
    /// Returns the culprit misbehavior on failure (fed to reputation, attributed to
    /// the failing commitment's vouchers). On success the engine *delivers* the
    /// assembled data to the consumer and drops the (large) assembly. NOTE: semantic
    /// validation (state transition / receipt root / accesses consistency) is NOT
    /// here — it is consumer-side and reported back via `FailedEvent`; likewise
    /// persistence is consumer-side (see `is_done`).
    ///
    /// (Sketch signature. With static dispatch the real one returns the *typed*
    /// decoded value — `SpiceChunkStateWitness` / receipt proofs / `CodeBytes` — so
    /// the consumer never re-deserializes the bytes.)
    fn verify_assembled(&self, id: &DataId, bytes: &[u8]) -> Result<(), Misbehavior>;

    /// Role-appropriate terminal check: is the *durable artifact* that means "we're done
    /// with this item" present? This is the real `Have` — NOT "the raw data is stored".
    /// A stateless validator never persists the witness: done = **our endorsement**
    /// exists. For a receipt = the **proof is persisted**; for data we author = the
    /// **produced artifact** is in store. Suppresses re-seeding. Persistence itself is
    /// consumer-side (validator endorses; executor saves the verified proof), never the
    /// engine's job. Mirrors today's `is_data_known` (endorsement / receipt-proof checks).
    ///
    /// Consulted at seed time and on completion only — never per received part (it is
    /// a store read). Two corner cases the impls must respect: the receipt-proof check
    /// must match what actually suppresses re-need (persisted vs applied can race the
    /// executor's apply loop); the contract-code check is a *cache* lookup, so an
    /// eviction can legitimately flip an item back to needed — resurrection must be
    /// tolerated, not treated as a dedup bug.
    fn is_done(&self, id: &DataId) -> Result<bool, near_chain::Error>;
}

/// `Witness{block, shard}` — erasure-coded; sources = chunk producers of `shard`;
/// need = we are an assigned chunk validator; seed = block processed.
pub(crate) struct WitnessKind;

/// `ReceiptProof{block, from, to}` — erasure-coded; sources = producers of `from`;
/// need = we will apply `to` in the next block; existence = `from` executed the block;
/// the *seed/trigger* is the executor's apply-attempt missing-set (event-driven).
pub(crate) struct ReceiptProofKind;

/// `ContractCode{code_hash}` — whole blob (K=1), content-addressed, so concurrent
/// interest from several blocks/shards is ONE item (the highest (block, shard) anchor
/// lives on the `FetchItem`); sources = producers of the anchor's shard (they executed ⇒ hold
/// the code); need = the witness/accesses names a hash not in the compiled-contract
/// cache; verify = `hash(bytes) == code_hash` (immediate); delivery = content-
/// addressed, (code_hash, bytes) to the validator actor, which unblocks ALL chunks
/// waiting on the hash — the anchor never routes delivery. Replaces the bespoke
/// `SpiceChunkContractAccesses`/`TrustedAccesses` path.
pub(crate) struct ContractCodeKind;

// impl DataKind for {WitnessKind, ReceiptProofKind, ContractCodeKind} — bodies omitted.
// Only these impls differ; the engine is unchanged across them.

/// Illustrative: contract accesses no longer drive a bespoke flow — they simply *seed*
/// one `ContractCode` fetch item per uncached hash (the manager adds/extends the
/// (block, shard) anchor on the — possibly pre-existing — item). Kept as a free
/// function to show the decomposition (need-seeding = engine input; accesses↔witness
/// consistency = a consumer-side semantic check that emits `Failed` on mismatch).
pub(crate) fn seed_contract_code_items(
    _block_hash: CryptoHash,
    _accessed: &[near_primitives::stateless_validation::contract_distribution::CodeHash],
) -> Vec<DataId> {
    Vec::new() // sketch
}
