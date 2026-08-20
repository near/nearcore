use super::{AssembledData, DataId};
use near_primitives::types::{AccountId, SpiceChunkId};

/// Initial fetch state determined while seeding an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interest {
    /// Neither needed nor produced here; no item is created.
    NotNeeded,
    /// Needed, but likely not produced yet; wait for the push.
    WaitForPush,
    /// Needed and plausibly produced; seed collecting and arm the pull.
    Fetchable,
}

/// Per-item context not carried by a content-addressed id.
pub(crate) struct FetchContext<'a> {
    pub(crate) anchor: Option<&'a SpiceChunkId>,
}

/// Supplies the data-specific policy used by the shared fetch engine.
pub(crate) trait DataKind {
    type Error;

    /// Accounts that can serve the whole item.
    fn sources(
        &self,
        id: &DataId,
        context: &FetchContext<'_>,
    ) -> Result<Vec<AccountId>, near_chain::Error>;

    /// Accounts the item is pushed to, and so the only ones granted the priority lane.
    fn recipients(
        &self,
        id: &DataId,
        claimed_chunk: Option<&SpiceChunkId>,
    ) -> Result<Vec<AccountId>, near_chain::Error>;

    /// Whether we need the item and may already pull it. Consulted once, at seed time.
    fn classify_at_seed(
        &self,
        id: &DataId,
        context: &FetchContext<'_>,
    ) -> Result<Interest, near_chain::Error>;

    /// Checks the assembled data against its id: decode plus hash for coded kinds,
    /// `hash(bytes) == code_hash` for a blob. Semantic validation stays with the consumer.
    fn verify_assembled(&self, id: &DataId, data: AssembledData<'_>) -> Result<(), Self::Error>;

    /// Whether the durable artifact that ends this item's life is present.
    fn is_done(&self, id: &DataId) -> Result<bool, near_chain::Error>;
}
