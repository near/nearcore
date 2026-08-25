use lru::LruCache;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Each claim holds a header of several kilobytes. Peers that disconnect while we are behind keep
/// their claim until our head passes it, so the map needs a cap of its own above any real peer count.
const MAX_PEER_CLAIMS: usize = 128;

/// A full approval pass is ~100 signature checks, about 2.8ms. With one claim per peer an
/// unbounded walk could spend a third of a second in one sync step, so stop after a few and
/// let the next step take the rest.
const MAX_CHECKS_PER_STEP: usize = 4;

/// What one peer has shown us: the highest header it relayed that we have not
/// settled yet, and the highest height its approvals verified for.
#[derive(Default)]
struct PeerHeights {
    claim: Option<Arc<BlockHeader>>,
    verified_height: Option<BlockHeight>,
}

impl PeerHeights {
    fn known_height(&self) -> Option<BlockHeight> {
        self.claim.as_ref().map(|claim| claim.height()).max(self.verified_height)
    }

    fn record_verified_height(&mut self, height: BlockHeight) {
        self.verified_height = Some(self.verified_height.map_or(height, |known| known.max(height)));
    }
}

/// Highest block height each peer is *verified* to have reached, via a relayed
/// block whose approvals we checked against a known epoch's validators (>2/3
/// stake). Relayed headers stay unchecked until the sync decision needs one.
pub struct VerifiedPeerHeights {
    by_peer: HashMap<PeerId, PeerHeights>,
    /// Headers whose approvals already verified; bounds the signature checks
    /// to one pass per distinct header, however many peers relay or replay it.
    /// Failures are not recorded: the verifier also reports one when the header's epoch is
    /// merely unknown, so remembering it would reject a valid far-ahead header for good once
    /// sync makes that epoch known. `MAX_CHECKS_PER_STEP` bounds the cost of rechecking.
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
    /// Keeps the highest header per peer, and none at or below what we already know.
    pub fn note_claim(&mut self, peer_id: &PeerId, header: &BlockHeader) {
        let height = header.height();
        if let Some(peer) = self.by_peer.get(peer_id) {
            if peer.known_height().is_some_and(|known| known >= height) {
                return;
            }
        } else if !self.evict_lowest_peer_below(height) {
            return;
        }
        self.by_peer.entry(peer_id.clone()).or_default().claim = Some(Arc::new(header.clone()));
    }

    /// Frees a slot for a peer we have not seen, by dropping the lowest claim when it is lower than
    /// the incoming one. Returns whether a slot is available.
    fn evict_lowest_peer_below(&mut self, height: BlockHeight) -> bool {
        if self.by_peer.len() < MAX_PEER_CLAIMS {
            return true;
        }
        let lowest = self
            .by_peer
            .iter()
            .filter_map(|(peer_id, peer)| Some((peer.known_height()?, peer_id)))
            .min();
        let Some((lowest_height, lowest_peer)) = lowest else {
            return false;
        };
        if lowest_height >= height {
            return false;
        }
        let lowest_peer = lowest_peer.clone();
        self.by_peer.remove(&lowest_peer);
        true
    }

    /// A height at or below `head_height` can't show a peer ahead of us, so it reads
    /// as absent however long it stays in the map.
    pub fn get_above(&self, peer_id: &PeerId, head_height: BlockHeight) -> Option<BlockHeight> {
        self.by_peer.get(peer_id)?.verified_height.filter(|height| *height > head_height)
    }

    #[cfg(test)]
    fn has_claim_above(&self, min_height: BlockHeight) -> bool {
        self.highest_claim_above(min_height).is_some()
    }

    fn highest_claim_above(&self, min_height: BlockHeight) -> Option<Arc<BlockHeader>> {
        self.by_peer
            .values()
            .filter_map(|peer| peer.claim.as_ref())
            .filter(|header| header.height() > min_height)
            .max_by_key(|header| header.height())
            .cloned()
    }

    /// Settles every claim on this header: one outcome answers all of them.
    fn settle_claims(&mut self, header_hash: &CryptoHash, height: BlockHeight, passed: bool) {
        self.by_peer.retain(|_, peer| {
            if peer.claim.as_ref().is_some_and(|claim| claim.hash() == header_hash) {
                peer.claim = None;
                if passed {
                    peer.record_verified_height(height);
                }
            }
            peer.claim.is_some() || peer.verified_height.is_some()
        });
    }

    /// Settles claims above `min_height`, highest first, and stops at the first that
    /// passes. Walking past a failed claim keeps a bogus high claim from hiding an
    /// honest lower one. A header that already passed costs nothing to settle again, so
    /// only fresh checks count against `MAX_CHECKS_PER_STEP`.
    pub fn check_claims_until_verified(
        &mut self,
        min_height: BlockHeight,
        mut check_approvals: impl FnMut(&BlockHeader) -> bool,
    ) {
        let mut checks = 0;
        while let Some(header) = self.highest_claim_above(min_height) {
            let mut passed = self.verified_header_hashes.contains(header.hash());
            if !passed {
                if checks == MAX_CHECKS_PER_STEP {
                    return;
                }
                checks += 1;
                passed = check_approvals(&header);
                if passed {
                    self.verified_header_hashes.put(*header.hash(), ());
                }
            }
            self.settle_claims(header.hash(), header.height(), passed);
            if passed {
                return;
            }
        }
    }

    /// Bounds the map as the head advances and peers churn.
    pub fn prune_at_or_below(&mut self, height: BlockHeight) {
        self.by_peer.retain(|_, peer| {
            if peer.claim.as_ref().is_some_and(|claim| claim.height() <= height) {
                peer.claim = None;
            }
            if peer.verified_height.is_some_and(|verified| verified <= height) {
                peer.verified_height = None;
            }
            peer.claim.is_some() || peer.verified_height.is_some()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{KeyType, SecretKey};
    use near_primitives::block_header::{BlockHeaderInnerLiteV2, BlockHeaderV7};

    fn peer(seed: &str) -> PeerId {
        PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed).public_key())
    }

    /// `seed` only varies the hash, so two claims at one height stay distinct.
    fn header(height: BlockHeight, seed: u64) -> Arc<BlockHeader> {
        let inner_lite = BlockHeaderInnerLiteV2 { height, timestamp: seed, ..Default::default() };
        let mut header = BlockHeaderV7 { inner_lite, ..Default::default() };
        header.init();
        Arc::new(BlockHeader::BlockHeaderV7(header))
    }

    fn check_all(heights: &mut VerifiedPeerHeights, passed: bool) {
        heights.check_claims_until_verified(0, |_| passed);
    }

    #[test]
    fn note_claim_is_accepted_after_every_claim_settled_as_failed() {
        let mut heights = VerifiedPeerHeights::default();
        for index in 0..MAX_PEER_CLAIMS {
            heights.note_claim(&peer(&format!("p{index}")), &header(100 + index as u64, 1));
        }
        for _ in 0..MAX_PEER_CLAIMS {
            check_all(&mut heights, false);
        }
        assert!(heights.by_peer.is_empty());
        let newcomer = peer("newcomer");
        heights.note_claim(&newcomer, &header(50, 2));
        assert!(heights.by_peer.contains_key(&newcomer));
    }

    #[test]
    fn note_claim_evicts_the_lowest_peer_when_the_map_is_full() {
        let mut heights = VerifiedPeerHeights::default();
        for index in 0..MAX_PEER_CLAIMS {
            heights.note_claim(&peer(&format!("p{index}")), &header(100 + index as u64, 1));
        }
        let lowest = peer("p0");
        let newcomer = peer("newcomer");
        heights.note_claim(&newcomer, &header(500, 2));
        assert_eq!(heights.by_peer.len(), MAX_PEER_CLAIMS);
        assert!(!heights.by_peer.contains_key(&lowest));
        assert!(heights.by_peer.contains_key(&newcomer));
    }

    #[test]
    fn note_claim_refuses_a_claim_below_every_peer_when_the_map_is_full() {
        let mut heights = VerifiedPeerHeights::default();
        for index in 0..MAX_PEER_CLAIMS {
            heights.note_claim(&peer(&format!("p{index}")), &header(100 + index as u64, 1));
        }
        let newcomer = peer("newcomer");
        heights.note_claim(&newcomer, &header(50, 2));
        assert_eq!(heights.by_peer.len(), MAX_PEER_CLAIMS);
        assert!(!heights.by_peer.contains_key(&newcomer));
    }

    #[test]
    fn note_claim_keeps_highest_per_peer() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.note_claim(&p, &header(10, 1));
        heights.note_claim(&p, &header(5, 2));
        check_all(&mut heights, true);
        assert_eq!(heights.get_above(&p, 0), Some(10));
    }

    #[test]
    fn highest_claim_above_min_height_is_checked_first() {
        let mut heights = VerifiedPeerHeights::default();
        heights.note_claim(&peer("a"), &header(10, 1));
        heights.note_claim(&peer("b"), &header(30, 2));
        heights.note_claim(&peer("c"), &header(20, 3));
        let mut checked = Vec::new();
        heights.check_claims_until_verified(10, |header| {
            checked.push(header.height());
            false
        });
        assert_eq!(checked, vec![30, 20]);
    }

    #[test]
    fn check_stops_at_the_first_claim_that_passes() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2, p3) = (peer("a"), peer("b"), peer("c"));
        heights.note_claim(&p1, &header(30, 1));
        heights.note_claim(&p2, &header(20, 2));
        heights.note_claim(&p3, &header(10, 3));
        heights.check_claims_until_verified(0, |header| match header.height() {
            20 => true,
            _ => false,
        });
        assert_eq!(heights.get_above(&p2, 0), Some(20));
        assert_eq!(heights.get_above(&p1, 0), None);
        assert!(heights.has_claim_above(0), "the claim at 10 is still unchecked");
    }

    #[test]
    fn one_check_verifies_every_peer_that_relayed_the_header() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let claim = header(10, 1);
        heights.note_claim(&p1, &claim);
        heights.note_claim(&p2, &claim);
        let mut checks = 0;
        heights.check_claims_until_verified(0, |_| {
            checks += 1;
            true
        });
        assert_eq!(checks, 1);
        assert_eq!(heights.get_above(&p1, 0), Some(10));
        assert_eq!(heights.get_above(&p2, 0), Some(10));
    }

    #[test]
    fn failed_check_leaves_the_peer_unverified_and_drops_the_claim() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.note_claim(&p, &header(10, 1));
        check_all(&mut heights, false);
        assert_eq!(heights.get_above(&p, 0), None);
        assert!(!heights.has_claim_above(0));
    }

    #[test]
    fn check_claims_until_verified_rechecks_a_header_that_failed() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let mut checks = 0;
        for _ in 0..2 {
            heights.note_claim(&p, &header(10, 1));
            heights.check_claims_until_verified(0, |_| {
                checks += 1;
                false
            });
        }
        assert_eq!(
            checks, 2,
            "a failure must not be remembered: the epoch may just be unknown yet"
        );
    }

    #[test]
    fn check_claims_until_verified_skips_a_header_that_passed() {
        let mut heights = VerifiedPeerHeights::default();
        let mut checks = 0;
        for seed in 0..2 {
            heights.note_claim(&peer(&format!("p{seed}")), &header(10, 1));
            heights.check_claims_until_verified(0, |_| {
                checks += 1;
                true
            });
        }
        assert_eq!(checks, 1);
    }

    #[test]
    fn higher_claim_keeps_the_height_we_already_verified() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.note_claim(&p, &header(10, 1));
        check_all(&mut heights, true);
        heights.note_claim(&p, &header(11, 2));
        assert_eq!(heights.get_above(&p, 0), Some(10));
        check_all(&mut heights, true);
        assert_eq!(heights.get_above(&p, 0), Some(11));
    }

    #[test]
    fn failed_higher_claim_keeps_the_height_we_already_verified() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.note_claim(&p, &header(10, 1));
        check_all(&mut heights, true);
        heights.note_claim(&p, &header(11, 2));
        check_all(&mut heights, false);
        assert_eq!(heights.get_above(&p, 0), Some(10));
    }

    #[test]
    fn get_above_hides_a_claim_at_or_below_our_head() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        heights.note_claim(&p, &header(10, 1));
        check_all(&mut heights, true);
        assert_eq!(heights.get_above(&p, 9), Some(10));
        assert_eq!(heights.get_above(&p, 10), None);
    }

    #[test]
    fn one_step_checks_at_most_max_checks_per_step_claims() {
        let mut heights = VerifiedPeerHeights::default();
        for index in 0..MAX_CHECKS_PER_STEP + 2 {
            heights.note_claim(&peer(&index.to_string()), &header(10 + index as u64, index as u64));
        }
        let mut checks = 0;
        heights.check_claims_until_verified(0, |_| {
            checks += 1;
            false
        });
        assert_eq!(checks, MAX_CHECKS_PER_STEP);
        assert!(heights.has_claim_above(0), "the rest wait for the next step");
    }

    #[test]
    fn prune_at_or_below_drops_caught_up_claims() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        heights.note_claim(&p1, &header(10, 1));
        check_all(&mut heights, true);
        heights.note_claim(&p2, &header(20, 2));
        check_all(&mut heights, true);
        assert_eq!(heights.get_above(&p1, 0), Some(10));
        heights.prune_at_or_below(10);
        assert_eq!(heights.get_above(&p1, 0), None);
        assert_eq!(heights.get_above(&p2, 0), Some(20));
    }
}
