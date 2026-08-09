use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::BlockHeight;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;

const HEADER_VERIFICATION_CACHE_SIZE: usize = 32;
const MAX_PENDING_HEADER_VERIFICATIONS: usize = 4;

struct PendingHeaderVerification {
    height: BlockHeight,
    peers: HashSet<PeerId>,
}

/// Highest block height each peer is *verified* to have reached, via a relayed
/// block whose approvals we checked against a known epoch's validators (>2/3
/// stake).
pub struct VerifiedPeerHeights {
    by_peer: HashMap<PeerId, BlockHeight>,
    /// Positive and negative results, so repeated invalid headers cannot consume worker time.
    header_verification_results: LruCache<CryptoHash, bool>,
    /// Peers waiting on each in-flight header verification. The map has a strict cap so an input
    /// burst cannot create an unbounded computation queue.
    pending_header_verifications: HashMap<CryptoHash, PendingHeaderVerification>,
}

impl Default for VerifiedPeerHeights {
    fn default() -> Self {
        Self {
            by_peer: HashMap::new(),
            header_verification_results: LruCache::new(
                NonZeroUsize::new(HEADER_VERIFICATION_CACHE_SIZE).unwrap(),
            ),
            pending_header_verifications: HashMap::new(),
        }
    }
}

impl VerifiedPeerHeights {
    /// Registers interest in a header and returns whether the caller should start verification.
    /// Repeated relays share one in-flight job. Cached valid results update the peer immediately,
    /// while cached invalid results are ignored.
    pub fn start_verification(
        &mut self,
        peer_id: &PeerId,
        header_hash: &CryptoHash,
        height: BlockHeight,
    ) -> bool {
        if self.get(peer_id).is_some_and(|verified| verified >= height) {
            return false;
        }
        if let Some(is_valid) = self.header_verification_results.get(header_hash).copied() {
            if is_valid {
                self.record_height(peer_id.clone(), height);
            }
            return false;
        }
        if let Some(pending) = self.pending_header_verifications.get_mut(header_hash) {
            debug_assert_eq!(pending.height, height);
            pending.peers.insert(peer_id.clone());
            return false;
        }
        if self.pending_header_verifications.len() >= MAX_PENDING_HEADER_VERIFICATIONS {
            return false;
        }
        self.pending_header_verifications.insert(
            *header_hash,
            PendingHeaderVerification { height, peers: HashSet::from([peer_id.clone()]) },
        );
        true
    }

    /// Completes a pending verification. A result at or below the current head is discarded
    /// because it can no longer prove that a peer is ahead of us.
    pub fn finish_verification(
        &mut self,
        header_hash: &CryptoHash,
        is_valid: bool,
        head_height: BlockHeight,
    ) {
        let Some(pending) = self.pending_header_verifications.remove(header_hash) else {
            return;
        };
        self.header_verification_results.put(*header_hash, is_valid);
        if !is_valid || pending.height <= head_height {
            return;
        }
        for peer_id in pending.peers {
            self.record_height(peer_id, pending.height);
        }
    }

    /// Drops an in-flight entry when the immutable verification inputs are not currently
    /// available. Unlike a failed cryptographic check, this is not cached because epoch data can
    /// become available later.
    pub fn cancel_verification(&mut self, header_hash: &CryptoHash) {
        self.pending_header_verifications.remove(header_hash);
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<BlockHeight> {
        self.by_peer.get(peer_id).copied()
    }

    /// Entries at or below our head can't indicate a peer ahead of us; pruning
    /// them bounds the map as the head advances and peers churn.
    pub fn prune_at_or_below(&mut self, height: BlockHeight) {
        self.by_peer.retain(|_, recorded| *recorded > height);
    }

    fn record_height(&mut self, peer_id: PeerId, height: BlockHeight) {
        self.by_peer
            .entry(peer_id)
            .and_modify(|recorded| *recorded = (*recorded).max(height))
            .or_insert(height);
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
        let h10 = header_hash(b"h10");
        assert!(heights.start_verification(&p, &h10, 10));
        heights.finish_verification(&h10, true, 0);
        let h5 = header_hash(b"h5");
        assert!(!heights.start_verification(&p, &h5, 5));
        assert_eq!(heights.get(&p), Some(10));
    }

    #[test]
    fn failed_verification_records_nothing() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let hash = header_hash(b"h10");
        assert!(heights.start_verification(&p, &hash, 10));
        heights.finish_verification(&hash, false, 0);
        assert_eq!(heights.get(&p), None);
        assert!(!heights.start_verification(&p, &hash, 10));
    }

    #[test]
    fn relays_share_one_verification() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let hash = header_hash(b"h10");
        assert!(heights.start_verification(&p1, &hash, 10));
        assert!(!heights.start_verification(&p2, &hash, 10));
        heights.finish_verification(&hash, true, 0);
        assert_eq!(heights.get(&p1), Some(10));
        assert_eq!(heights.get(&p2), Some(10));
    }

    #[test]
    fn known_header_skips_verification() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let hash = header_hash(b"h10");
        assert!(heights.start_verification(&p1, &hash, 10));
        heights.finish_verification(&hash, true, 0);
        assert!(!heights.start_verification(&p2, &hash, 10));
        assert_eq!(heights.get(&p2), Some(10));
    }

    #[test]
    fn result_at_current_head_is_discarded() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let hash = header_hash(b"h10");
        assert!(heights.start_verification(&p, &hash, 10));
        heights.finish_verification(&hash, true, 10);
        assert_eq!(heights.get(&p), None);
    }

    #[test]
    fn out_of_order_results_keep_highest_height() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let h10 = header_hash(b"h10");
        let h20 = header_hash(b"h20");
        assert!(heights.start_verification(&p, &h10, 10));
        assert!(heights.start_verification(&p, &h20, 20));
        heights.finish_verification(&h20, true, 0);
        heights.finish_verification(&h10, true, 0);
        assert_eq!(heights.get(&p), Some(20));
    }

    #[test]
    fn cancelled_verification_can_be_retried() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let hash = header_hash(b"h10");
        assert!(heights.start_verification(&p, &hash, 10));
        heights.cancel_verification(&hash);
        assert!(heights.start_verification(&p, &hash, 10));
    }

    #[test]
    fn prune_at_or_below_drops_caught_up_entries() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let h10 = header_hash(b"h10");
        assert!(heights.start_verification(&p1, &h10, 10));
        heights.finish_verification(&h10, true, 0);
        let h20 = header_hash(b"h20");
        assert!(heights.start_verification(&p2, &h20, 20));
        heights.finish_verification(&h20, true, 0);
        heights.prune_at_or_below(10);
        assert_eq!(heights.get(&p1), None);
        assert_eq!(heights.get(&p2), Some(20));
    }

    #[test]
    fn pending_verifications_are_bounded() {
        let mut heights = VerifiedPeerHeights::default();
        for i in 0..MAX_PENDING_HEADER_VERIFICATIONS {
            assert!(heights.start_verification(
                &peer(&i.to_string()),
                &header_hash(&[i as u8]),
                10
            ));
        }
        assert!(!heights.start_verification(&peer("overflow"), &header_hash(b"overflow"), 10));
    }
}
