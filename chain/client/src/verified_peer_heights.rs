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

    fn get(&self, peer_id: &PeerId) -> Option<BlockHeight> {
        self.by_peer.get(peer_id).copied()
    }

    /// An entry at or below `head_height` can't show a peer ahead of us, so it
    /// reads as absent however long it stays in the map.
    pub fn get_above(&self, peer_id: &PeerId, head_height: BlockHeight) -> Option<BlockHeight> {
        self.get(peer_id).filter(|height| *height > head_height)
    }

    /// Highest height any peer is verified to have reached. Each recorded height
    /// is backed by a header approved by >2/3 of a known epoch's stake, so it
    /// holds as evidence of chain progress even after that peer leaves.
    pub fn max_height_across_peers(&self) -> Option<BlockHeight> {
        self.by_peer.values().copied().max()
    }

    /// Bounds the map as the head advances and peers churn.
    pub fn prune_at_or_below(&mut self, height: BlockHeight) {
        self.by_peer.retain(|_, recorded| *recorded > height);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{KeyType, SecretKey};

    fn peer(seed: &str) -> PeerId {
        PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed).public_key())
    }

    fn header_hash(seed: &[u8]) -> CryptoHash {
        CryptoHash::hash_bytes(seed)
    }

    #[test]
    fn record_keeps_max_height_per_peer() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.record_if_verified(&p, &header_hash(b"h10"), 10, || true);
        heights.record_if_verified(&p, &header_hash(b"h5"), 5, || true);
        assert_eq!(heights.get(&p), Some(10));
    }

    #[test]
    fn failed_verification_records_nothing() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.record_if_verified(&p, &header_hash(b"h10"), 10, || false);
        assert_eq!(heights.get(&p), None);
    }

    #[test]
    fn known_header_skips_verification() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let hash = header_hash(b"h10");
        heights.record_if_verified(&p1, &hash, 10, || true);
        heights.record_if_verified(&p2, &hash, 10, || panic!("must not re-verify"));
        assert_eq!(heights.get(&p2), Some(10));
    }

    #[test]
    fn already_verified_height_skips_verification() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.record_if_verified(&p, &header_hash(b"h10"), 10, || true);
        heights.record_if_verified(&p, &header_hash(b"other"), 10, || {
            panic!("must not verify below already verified height")
        });
        assert_eq!(heights.get(&p), Some(10));
    }

    #[test]
    fn get_above_hides_entries_at_or_below_head() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.record_if_verified(&p, &header_hash(b"h10"), 10, || true);
        assert_eq!(heights.get_above(&p, 9), Some(10));
        assert_eq!(heights.get_above(&p, 10), None);
        assert_eq!(heights.get_above(&p, 11), None);
    }

    #[test]
    fn prune_at_or_below_drops_caught_up_entries() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        heights.record_if_verified(&p1, &header_hash(b"h10"), 10, || true);
        heights.record_if_verified(&p2, &header_hash(b"h20"), 20, || true);
        heights.prune_at_or_below(10);
        assert_eq!(heights.get(&p1), None);
        assert_eq!(heights.get(&p2), Some(20));
    }
}
