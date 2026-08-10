use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::BlockHeight;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;

const HEADER_VERIFICATION_CACHE_SIZE: usize = 32;
const MAX_ACTIVE_HEADER_VERIFICATIONS: usize = 4;

/// Limits CPU concurrency while retaining admitted work until a worker becomes available.
/// [`VerifiedPeerHeights`] admits at most one pending job per relaying peer, so the queue is bounded
/// by the network peer set rather than by an arbitrary burst size.
pub(crate) struct BlockApprovalVerificationScheduler<T> {
    active: usize,
    queued: VecDeque<T>,
    deferred_by_peer: HashMap<PeerId, (BlockHeight, T)>,
}

impl<T> Default for BlockApprovalVerificationScheduler<T> {
    fn default() -> Self {
        Self { active: 0, queued: VecDeque::new(), deferred_by_peer: HashMap::new() }
    }
}

impl<T> BlockApprovalVerificationScheduler<T> {
    /// Returns the job when it should run immediately, or queues it when all worker slots are busy.
    pub(crate) fn enqueue(&mut self, job: T) -> Option<T> {
        if self.active < MAX_ACTIVE_HEADER_VERIFICATIONS {
            self.active += 1;
            return Some(job);
        }
        self.queued.push_back(job);
        None
    }

    /// Releases a completed worker slot and returns the oldest queued job to take its place.
    pub(crate) fn complete(&mut self) -> Option<T> {
        debug_assert!(self.active > 0);
        if let Some(job) = self.queued.pop_front() {
            return Some(job);
        }
        self.active = self.active.saturating_sub(1);
        None
    }

    /// Retains only the newest verification received while a peer already has admitted work.
    pub(crate) fn defer(&mut self, peer_id: PeerId, height: BlockHeight, job: T) {
        match self.deferred_by_peer.entry(peer_id) {
            Entry::Occupied(mut entry) if entry.get().0 < height => {
                entry.insert((height, job));
            }
            Entry::Vacant(entry) => {
                entry.insert((height, job));
            }
            Entry::Occupied(_) => {}
        }
    }

    pub(crate) fn deferred_height(&self, peer_id: &PeerId) -> Option<BlockHeight> {
        self.deferred_by_peer.get(peer_id).map(|(height, _)| *height)
    }

    pub(crate) fn take_deferred(&mut self, peer_id: &PeerId) -> Option<T> {
        self.deferred_by_peer.remove(peer_id).map(|(_, job)| job)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum VerificationRegistration {
    /// The caller must prepare and enqueue a new verification job.
    Enqueue,
    /// The peer already has admitted work, so the caller must retain this newer header for later.
    Defer,
    /// The relay was satisfied by verified, cached, or shared pending state.
    Handled,
}

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
    /// Peers waiting on each active or queued header verification.
    pending_header_verifications: HashMap<CryptoHash, PendingHeaderVerification>,
    /// The pending header assigned to each peer. A peer can consume at most one verification slot,
    /// which bounds admitted work by the network peer set without dropping distinct peers merely
    /// because a short block burst exceeds the result cache size.
    pending_header_by_peer: HashMap<PeerId, CryptoHash>,
}

impl Default for VerifiedPeerHeights {
    fn default() -> Self {
        Self {
            by_peer: HashMap::new(),
            header_verification_results: LruCache::new(
                NonZeroUsize::new(HEADER_VERIFICATION_CACHE_SIZE).unwrap(),
            ),
            pending_header_verifications: HashMap::new(),
            pending_header_by_peer: HashMap::new(),
        }
    }
}

impl VerifiedPeerHeights {
    /// Registers interest in a header and tells the caller whether to enqueue, defer, or drop it.
    /// Repeated relays share one pending job. Cached valid results update the peer immediately,
    /// while cached invalid results are ignored. A strictly newer header is deferred when this
    /// peer already has admitted work.
    pub fn register_verification(
        &mut self,
        peer_id: &PeerId,
        header_hash: &CryptoHash,
        height: BlockHeight,
    ) -> VerificationRegistration {
        if self.get(peer_id).is_some_and(|verified| verified >= height) {
            return VerificationRegistration::Handled;
        }
        if let Some(is_valid) = self.header_verification_results.get(header_hash).copied() {
            if is_valid {
                self.record_height(peer_id.clone(), height);
            }
            return VerificationRegistration::Handled;
        }
        if let Some(pending_hash) = self.pending_header_by_peer.get(peer_id) {
            let pending = self
                .pending_header_verifications
                .get(pending_hash)
                .expect("pending header by peer must reference pending verification");
            return if height > pending.height {
                VerificationRegistration::Defer
            } else {
                VerificationRegistration::Handled
            };
        }
        if let Some(pending) = self.pending_header_verifications.get_mut(header_hash) {
            debug_assert_eq!(pending.height, height);
            pending.peers.insert(peer_id.clone());
            self.pending_header_by_peer.insert(peer_id.clone(), *header_hash);
            return VerificationRegistration::Handled;
        }
        self.pending_header_verifications.insert(
            *header_hash,
            PendingHeaderVerification { height, peers: HashSet::from([peer_id.clone()]) },
        );
        self.pending_header_by_peer.insert(peer_id.clone(), *header_hash);
        VerificationRegistration::Enqueue
    }

    /// Completes a pending verification. A result at or below the current head is discarded
    /// because it can no longer prove that a peer is ahead of us.
    pub fn finish_verification(
        &mut self,
        header_hash: &CryptoHash,
        is_valid: bool,
        head_height: BlockHeight,
    ) -> Vec<PeerId> {
        let Some(pending) = self.take_pending_verification(header_hash) else {
            return Vec::new();
        };
        let peer_ids = pending.peers.into_iter().collect::<Vec<_>>();
        self.header_verification_results.put(*header_hash, is_valid);
        if !is_valid || pending.height <= head_height {
            return peer_ids;
        }
        for peer_id in &peer_ids {
            self.record_height(peer_id.clone(), pending.height);
        }
        peer_ids
    }

    /// Drops an in-flight entry when the immutable verification inputs are not currently
    /// available. Unlike a failed cryptographic check, this is not cached because epoch data can
    /// become available later.
    pub fn cancel_verification(&mut self, header_hash: &CryptoHash) -> Vec<PeerId> {
        self.take_pending_verification(header_hash)
            .map(|pending| pending.peers.into_iter().collect())
            .unwrap_or_default()
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

    fn take_pending_verification(
        &mut self,
        header_hash: &CryptoHash,
    ) -> Option<PendingHeaderVerification> {
        let pending = self.pending_header_verifications.remove(header_hash)?;
        for peer_id in &pending.peers {
            self.pending_header_by_peer.remove(peer_id);
        }
        Some(pending)
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
        assert_eq!(heights.register_verification(&p, &h10, 10), VerificationRegistration::Enqueue);
        heights.finish_verification(&h10, true, 0);
        let h5 = header_hash(b"h5");
        assert_eq!(heights.register_verification(&p, &h5, 5), VerificationRegistration::Handled);
        assert_eq!(heights.get(&p), Some(10));
    }

    #[test]
    fn failed_verification_records_nothing() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let hash = header_hash(b"h10");
        assert_eq!(heights.register_verification(&p, &hash, 10), VerificationRegistration::Enqueue);
        heights.finish_verification(&hash, false, 0);
        assert_eq!(heights.get(&p), None);
        assert_eq!(heights.register_verification(&p, &hash, 10), VerificationRegistration::Handled);
    }

    #[test]
    fn relays_share_one_verification() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let hash = header_hash(b"h10");
        assert_eq!(
            heights.register_verification(&p1, &hash, 10),
            VerificationRegistration::Enqueue
        );
        assert_eq!(
            heights.register_verification(&p2, &hash, 10),
            VerificationRegistration::Handled
        );
        heights.finish_verification(&hash, true, 0);
        assert_eq!(heights.get(&p1), Some(10));
        assert_eq!(heights.get(&p2), Some(10));
    }

    #[test]
    fn known_header_skips_verification() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let hash = header_hash(b"h10");
        assert_eq!(
            heights.register_verification(&p1, &hash, 10),
            VerificationRegistration::Enqueue
        );
        heights.finish_verification(&hash, true, 0);
        assert_eq!(
            heights.register_verification(&p2, &hash, 10),
            VerificationRegistration::Handled
        );
        assert_eq!(heights.get(&p2), Some(10));
    }

    #[test]
    fn result_at_current_head_is_discarded() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let hash = header_hash(b"h10");
        assert_eq!(heights.register_verification(&p, &hash, 10), VerificationRegistration::Enqueue);
        heights.finish_verification(&hash, true, 10);
        assert_eq!(heights.get(&p), None);
    }

    #[test]
    fn newer_height_is_deferred_until_the_current_verification_finishes() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let h10 = header_hash(b"h10");
        let h20 = header_hash(b"h20");
        assert_eq!(heights.register_verification(&p, &h10, 10), VerificationRegistration::Enqueue);
        assert_eq!(heights.register_verification(&p, &h20, 20), VerificationRegistration::Defer);
        assert_eq!(heights.finish_verification(&h10, false, 0), vec![p.clone()]);
        assert_eq!(heights.register_verification(&p, &h20, 20), VerificationRegistration::Enqueue);
        heights.finish_verification(&h20, true, 0);
        assert_eq!(heights.get(&p), Some(20));
    }

    #[test]
    fn cancelled_verification_can_be_retried() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let hash = header_hash(b"h10");
        assert_eq!(heights.register_verification(&p, &hash, 10), VerificationRegistration::Enqueue);
        heights.cancel_verification(&hash);
        assert_eq!(heights.register_verification(&p, &hash, 10), VerificationRegistration::Enqueue);
    }

    #[test]
    fn prune_at_or_below_drops_caught_up_entries() {
        let mut heights = VerifiedPeerHeights::default();
        let (p1, p2) = (peer("a"), peer("b"));
        let h10 = header_hash(b"h10");
        assert_eq!(heights.register_verification(&p1, &h10, 10), VerificationRegistration::Enqueue);
        heights.finish_verification(&h10, true, 0);
        let h20 = header_hash(b"h20");
        assert_eq!(heights.register_verification(&p2, &h20, 20), VerificationRegistration::Enqueue);
        heights.finish_verification(&h20, true, 0);
        heights.prune_at_or_below(10);
        assert_eq!(heights.get(&p1), None);
        assert_eq!(heights.get(&p2), Some(20));
    }

    #[test]
    fn pending_verifications_are_bounded_per_peer() {
        let mut heights = VerifiedPeerHeights::default();
        let p = peer("a");
        let first = header_hash(b"first");
        assert_eq!(
            heights.register_verification(&p, &first, 10),
            VerificationRegistration::Enqueue
        );
        assert_eq!(
            heights.register_verification(&p, &header_hash(b"second"), 11),
            VerificationRegistration::Defer
        );
        assert_eq!(heights.pending_header_verifications.len(), 1);

        heights.finish_verification(&first, false, 0);
        assert_eq!(
            heights.register_verification(&p, &header_hash(b"third"), 12),
            VerificationRegistration::Enqueue
        );
    }

    #[test]
    fn distinct_peers_are_retained_beyond_the_result_cache_size() {
        let mut heights = VerifiedPeerHeights::default();
        for i in 0..=HEADER_VERIFICATION_CACHE_SIZE {
            assert_eq!(
                heights.register_verification(&peer(&i.to_string()), &header_hash(&[i as u8]), 10),
                VerificationRegistration::Enqueue
            );
        }

        let overflow_peer = peer(&HEADER_VERIFICATION_CACHE_SIZE.to_string());
        let overflow_hash = header_hash(&[HEADER_VERIFICATION_CACHE_SIZE as u8]);
        heights.finish_verification(&overflow_hash, true, 0);
        assert_eq!(heights.get(&overflow_peer), Some(10));
    }

    #[test]
    fn fifth_verification_waits_for_a_worker_slot() {
        let mut scheduler = BlockApprovalVerificationScheduler::default();
        for job in 0..MAX_ACTIVE_HEADER_VERIFICATIONS {
            assert_eq!(scheduler.enqueue(job), Some(job));
        }

        assert_eq!(scheduler.enqueue(MAX_ACTIVE_HEADER_VERIFICATIONS), None);
        assert_eq!(scheduler.complete(), Some(MAX_ACTIVE_HEADER_VERIFICATIONS));
    }

    #[test]
    fn scheduler_retains_only_the_newest_deferred_height_per_peer() {
        let mut scheduler = BlockApprovalVerificationScheduler::default();
        let p = peer("a");

        scheduler.defer(p.clone(), 11, "h11");
        scheduler.defer(p.clone(), 13, "h13");
        scheduler.defer(p.clone(), 12, "h12");

        assert_eq!(scheduler.deferred_height(&p), Some(13));
        assert_eq!(scheduler.take_deferred(&p), Some("h13"));
        assert_eq!(scheduler.take_deferred(&p), None);
    }

    #[test]
    fn verification_scheduler_retains_a_burst_beyond_the_result_cache_size() {
        let mut scheduler = BlockApprovalVerificationScheduler::default();
        for job in 0..=HEADER_VERIFICATION_CACHE_SIZE {
            let scheduled = scheduler.enqueue(job);
            if job < MAX_ACTIVE_HEADER_VERIFICATIONS {
                assert_eq!(scheduled, Some(job));
            } else {
                assert_eq!(scheduled, None);
            }
        }

        for job in MAX_ACTIVE_HEADER_VERIFICATIONS..=HEADER_VERIFICATION_CACHE_SIZE {
            assert_eq!(scheduler.complete(), Some(job));
        }
    }
}
