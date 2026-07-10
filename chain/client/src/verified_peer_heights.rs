use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::num::NonZeroUsize;

/// Highest block height each peer is *verified* to have reached, via a relayed
/// block whose approvals we checked against a known epoch's validators (>2/3
/// stake).
pub struct VerifiedPeerHeights {
    by_peer: HashMap<PeerId, BlockHeight>,
    /// Headers whose approvals already verified; bounds the signature checks
    /// to one pass per distinct header, however many peers relay or replay it.
    verified_header_hashes: LruCache<CryptoHash, ()>,
}

impl Default for VerifiedPeerHeights {
    fn default() -> Self {
        Self {
            by_peer: HashMap::new(),
            verified_header_hashes: LruCache::new(NonZeroUsize::new(32).unwrap()),
        }
    }
}

impl VerifiedPeerHeights {
    /// Record `height` for `peer_id` if the header is verified: by an earlier
    /// call, or established now by `verify_approvals` (invoked at most once
    /// per distinct header). Keeps the highest height per peer.
    pub fn record_if_verified(
        &mut self,
        peer_id: &PeerId,
        header_hash: &CryptoHash,
        height: BlockHeight,
        verify_approvals: impl FnOnce() -> bool,
    ) {
        if self.get(peer_id).is_some_and(|verified| verified >= height) {
            return;
        }
        if !self.verified_header_hashes.contains(header_hash) {
            if !verify_approvals() {
                return;
            }
            self.verified_header_hashes.put(*header_hash, ());
        }
        self.by_peer.insert(peer_id.clone(), height);
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<BlockHeight> {
        self.by_peer.get(peer_id).copied()
    }

    /// Entries at or below our head can't indicate a peer ahead of us; pruning
    /// them bounds the map as the head advances and peers churn.
    pub fn prune_at_or_below(&mut self, height: BlockHeight) {
        self.by_peer.retain(|_, recorded| *recorded > height);
    }
}
