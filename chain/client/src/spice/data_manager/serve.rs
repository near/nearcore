//! SKETCH. Serve side: producers answering pull requests from `Item::Produce`
//! entries. Recovery is served by producers ONLY (decided: no peer-assisted
//! recovery — recipients never serve), so everything here keys off data we author.

use super::item::DataId;
use std::collections::HashMap;

/// Byte-budgeted cache of encoded parts, so pull storms never re-encode.
///
/// Populated for free at production time — the full parts vector must exist anyway
/// to build the Merkle commitment — and warmed on a serve miss. Eviction is LRU
/// under a GLOBAL byte budget; eviction is correctness-free (a miss re-encodes from
/// the stored artifact; with persist-before-distribute — parts written to the store
/// at production — a miss is a store read instead). Deliberately a sibling of the
/// item map, not state inside
/// `ProduceState`: the byte bound is global, and cache lifetime ≠ item lifetime
/// (evictable under pressure before expiry; servable after restart when no item
/// exists).
pub(crate) struct EncodeCache {
    budget_bytes: u64,
    used_bytes: u64,
    entries: HashMap<DataId, CachedEncoding>,
    // LRU order over `entries` — concrete structure (intrusive list / generation
    // stamps) left open in the sketch.
}

/// One cached encoding: all N parts + the commitment they hash to.
pub(crate) struct CachedEncoding {
    // parts: Vec<SpiceDataPart>, commitment: SpiceDataCommitment — omitted in the
    // sketch; `bytes` is what counts against the budget.
    pub(crate) bytes: u64,
}

impl EncodeCache {
    /// Insert at production time (or after a miss re-encode), evicting LRU entries
    /// until the budget fits. An entry larger than the whole budget is served
    /// without being cached.
    pub(crate) fn insert(&mut self, _id: DataId, _encoding: CachedEncoding) {}

    /// Cache lookup; `None` ⇒ the caller re-encodes from the stored artifact (and
    /// re-inserts). Touches LRU order.
    pub(crate) fn get(&mut self, _id: &DataId) -> Option<&CachedEncoding> {
        None // sketch
    }

    /// Drop entries for expired items eagerly (expiry already fires an event; no
    /// reason to wait for budget pressure to reclaim dead bytes).
    pub(crate) fn evict(&mut self, _id: &DataId) {}
}
