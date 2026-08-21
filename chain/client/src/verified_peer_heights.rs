use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::num::NonZeroUsize;

/// Outcome of checking a header's approvals. Only the block producer for a height
/// can sign a header carrying a weak approval set, and checking one costs a full pass
/// over ~100 signatures, so a failure is remembered rather than retried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalCheckResult {
    Passed,
    Failed,
}

/// Highest block height each peer is *verified* to have reached, via a relayed
/// block whose approvals we checked against a known epoch's validators (>2/3
/// stake).
pub struct VerifiedPeerHeights {
    by_peer: HashMap<PeerId, BlockHeight>,
    /// Outcome per header; bounds the signature checks to one pass per distinct
    /// header, however many peers relay or replay it.
    approval_check_results: LruCache<CryptoHash, ApprovalCheckResult>,
}

impl Default for VerifiedPeerHeights {
    fn default() -> Self {
        Self {
            by_peer: HashMap::new(),
            approval_check_results: LruCache::new(NonZeroUsize::new(32).unwrap()),
        }
    }
}

impl VerifiedPeerHeights {
    /// Record `height` for `peer_id` if the header's approvals check out: by an
    /// earlier call, or by `verify_approvals` now, which runs at most once per
    /// distinct header whichever way it turns out. Keeps the highest height per peer.
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
        let check_result = match self.approval_check_results.get(header_hash).copied() {
            Some(check_result) => check_result,
            None => {
                let check_result = match verify_approvals() {
                    true => ApprovalCheckResult::Passed,
                    false => ApprovalCheckResult::Failed,
                };
                self.approval_check_results.put(*header_hash, check_result);
                check_result
            }
        };
        if check_result == ApprovalCheckResult::Failed {
            return;
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
    fn failed_header_is_not_verified_again() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let hash = header_hash(b"h10");
        heights.record_if_verified(&p1, &hash, 10, || false);
        heights.record_if_verified(&p2, &hash, 10, || panic!("must not re-verify a failed header"));
        assert_eq!(heights.get(&p2), None);
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
