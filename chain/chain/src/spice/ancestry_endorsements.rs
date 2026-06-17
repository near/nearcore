use near_primitives::spice::chunk_endorsement::SpiceStoredVerifiedEndorsement;
use near_primitives::types::{AccountId, SpiceChunkId, SpiceUncertifiedChunkInfo};
use std::collections::{HashMap, HashSet};

/// Endorsement state derived from the uncertified chunks as of the previous block: the context
/// against which a block's core statements are validated. Borrows `prev_uncertified_chunks`.
pub(crate) struct AncestryEndorsements<'a> {
    pending_designated: HashSet<(&'a SpiceChunkId, &'a AccountId)>,
    on_chain: HashMap<&'a SpiceChunkId, HashMap<&'a AccountId, &'a SpiceStoredVerifiedEndorsement>>,
}

impl<'a> AncestryEndorsements<'a> {
    pub(crate) fn collect(prev_uncertified_chunks: &'a [SpiceUncertifiedChunkInfo]) -> Self {
        let mut on_chain: HashMap<_, HashMap<_, _>> = HashMap::new();
        for info in prev_uncertified_chunks {
            for (account_id, endorsement) in info.all_present_endorsements() {
                on_chain.entry(&info.chunk_id).or_default().insert(account_id, endorsement);
            }
        }
        Self {
            pending_designated: prev_uncertified_chunks
                .iter()
                .flat_map(|info| {
                    info.missing_endorsements.iter().map(|account_id| (&info.chunk_id, account_id))
                })
                .collect(),
            on_chain,
        }
    }

    /// Whether `(chunk_id, account_id)` is a designated endorsement still awaited on chain.
    pub(crate) fn is_pending_designated(
        &self,
        chunk_id: &'a SpiceChunkId,
        account_id: &'a AccountId,
    ) -> bool {
        self.pending_designated.contains(&(chunk_id, account_id))
    }

    /// Chunks awaiting a designated endorsement on chain (with repeats per missing validator).
    pub(crate) fn pending_designated_chunks(&self) -> impl Iterator<Item = &'a SpiceChunkId> + '_ {
        self.pending_designated.iter().map(|(chunk_id, _)| *chunk_id)
    }

    /// Endorsements (designated and non-designated) already on chain for `chunk_id`.
    pub(crate) fn on_chain_for(
        &self,
        chunk_id: &SpiceChunkId,
    ) -> impl Iterator<Item = (&'a AccountId, &'a SpiceStoredVerifiedEndorsement)> + '_ {
        self.on_chain
            .get(chunk_id)
            .into_iter()
            .flatten()
            .map(|(account_id, endorsement)| (*account_id, *endorsement))
    }
}
