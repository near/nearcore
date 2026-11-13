use lru::LruCache;
use near_async::time::{Duration, Utc};
use near_network::types::FullPeerInfo;
use near_network::types::PeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::BlockHeight;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use std::num::NonZeroUsize;

/// A peer's own claim about its chain head, which `FullPeerInfo` holds as an
/// `Option`. Neither the height nor the hash is verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAdvertisedHead {
    pub peer_info: PeerInfo,
    pub highest_block_height: BlockHeight,
    pub highest_block_hash: CryptoHash,
    /// Denote if a node is running in archival mode or not.
    pub archival: bool,
}

impl PeerAdvertisedHead {
    /// `None` when the peer has not told us about any block yet.
    pub fn from_full_peer_info(peer: &FullPeerInfo) -> Option<Self> {
        let last_block = peer.chain_info.last_block?;
        Some(PeerAdvertisedHead {
            peer_info: peer.peer_info.clone(),
            highest_block_height: last_block.height,
            highest_block_hash: last_block.hash,
            archival: peer.chain_info.archival,
        })
    }
}

/// What the client knows about the network for one sync step.
pub struct SyncPeers {
    /// Where we are syncing to: verified when our head is fresh, our own header
    /// head during block sync, an unverified claim otherwise. It picks the sync
    /// phase and the download targets, never anything that discards data.
    pub highest_height: BlockHeight,
    /// Highest height backed by a header with >2/3 stake approvals from an epoch
    /// we know. `None` when nothing is verifiable. Required by any decision that
    /// discards data.
    pub verified_highest_height: Option<BlockHeight>,
    /// Peers we may ask: those advertising a head above ours. `PeerSelector`
    /// decides which of them to ask.
    pub peers_ahead: Vec<PeerAdvertisedHead>,
}

/// A peer that failed to serve what it advertised stays out of the preferred set
/// for this long. An advertised height is worth only as much as the peer's
/// willingness to serve it.
pub const PEER_FAILURE_COOLDOWN_SECONDS: i64 = 60;

/// Bounds the map, so a peer generating fresh ids cannot grow it without limit.
const MAX_TRACKED_PEERS: usize = 256;

/// Peers this far below the median candidate are still worth asking, so requests
/// spread over several peers instead of landing on one.
const PEER_HEIGHT_WINDOW: BlockHeight = 5;

/// Decides which peers to ask, and remembers which ones did not deliver.
///
/// The policy is one place on purpose: ask a peer advertising at least the median
/// height minus `PEER_HEIGHT_WINDOW`, and drop one that fails to serve until its
/// cooldown ends. The median moves only when more than half of the candidates
/// agree, so a few peers advertising a very high height cannot keep the others out
/// of the window. When every peer has failed recently the failure history is
/// ignored, though the window still applies.
pub struct PeerSelector {
    failed_since: LruCache<PeerId, Utc>,
    cooldown: Duration,
    /// Several peers can be worth asking, so this breaks the tie and requests
    /// spread over them. Seeded in tests so selection is repeatable.
    rng: StdRng,
}

impl PeerSelector {
    pub fn new(cooldown: Duration, rng: StdRng) -> Self {
        Self {
            failed_since: LruCache::new(NonZeroUsize::new(MAX_TRACKED_PEERS).unwrap()),
            cooldown,
            rng,
        }
    }

    /// The peer did not serve what it advertised: no answer, or an answer we
    /// could not use.
    pub fn record_failed_to_serve(&mut self, peer_id: &PeerId, now: Utc) {
        self.failed_since.put(peer_id.clone(), now);
    }

    pub fn failed_recently(&self, peer_id: &PeerId, now: Utc) -> bool {
        self.failed_since.peek(peer_id).is_some_and(|since| now - *since < self.cooldown)
    }

    /// The peer to ask now.
    pub fn pick<'a>(
        &mut self,
        candidates: &'a [PeerAdvertisedHead],
        now: Utc,
    ) -> Option<&'a PeerAdvertisedHead> {
        self.pick_matching(candidates, now, |_| true)
    }

    /// The peer to ask now among those matching `is_suitable`.
    pub fn pick_matching<'a>(
        &mut self,
        candidates: &'a [PeerAdvertisedHead],
        now: Utc,
        is_suitable: impl Fn(&PeerAdvertisedHead) -> bool,
    ) -> Option<&'a PeerAdvertisedHead> {
        let suitable = || candidates.iter().filter(|peer| is_suitable(peer));
        // Peers that just failed us are dropped before the median is read, so they
        // do not count toward it until their cooldown ends.
        let mut pool: Vec<&PeerAdvertisedHead> =
            suitable().filter(|peer| !self.failed_recently(&peer.peer_info.id, now)).collect();
        if pool.is_empty() {
            pool = suitable().collect();
        }
        let median = Self::median_advertised_head(pool.iter().copied())?.highest_block_height;
        let worth_asking = median.saturating_sub(PEER_HEIGHT_WINDOW);
        pool.into_iter()
            .filter(|peer| peer.highest_block_height >= worth_asking)
            .choose(&mut self.rng)
    }

    /// The sync target from claimed heights. When peers are split around
    /// `trigger_height`, only those above it count, so the target starts sync.
    /// With none above it, all candidates count.
    pub fn sync_target_from_claimed_peers<'a>(
        &self,
        candidates: &'a [PeerAdvertisedHead],
        trigger_height: BlockHeight,
        now: Utc,
    ) -> Option<&'a PeerAdvertisedHead> {
        let above_trigger: Vec<&PeerAdvertisedHead> =
            candidates.iter().filter(|peer| peer.highest_block_height > trigger_height).collect();
        if above_trigger.is_empty() {
            return self.median_advertised_head_without_recent_failures(candidates, now);
        }
        self.median_advertised_head_without_recent_failures(above_trigger, now)
    }

    /// The median advertised head among peers that did not fail recently, or among all
    /// of them when every one did.
    fn median_advertised_head_without_recent_failures<'a>(
        &self,
        candidates: impl IntoIterator<Item = &'a PeerAdvertisedHead>,
        now: Utc,
    ) -> Option<&'a PeerAdvertisedHead> {
        let candidates: Vec<&PeerAdvertisedHead> = candidates.into_iter().collect();
        let without_recent_failures: Vec<&PeerAdvertisedHead> = candidates
            .iter()
            .copied()
            .filter(|peer| !self.failed_recently(&peer.peer_info.id, now))
            .collect();
        if without_recent_failures.is_empty() {
            return Self::median_advertised_head(candidates);
        }
        Self::median_advertised_head(without_recent_failures)
    }

    /// The peer at the median advertised height.
    ///
    /// For an even count, the lower of the two middle peers, so liars need a
    /// strict majority to set it.
    fn median_advertised_head<'a>(
        candidates: impl IntoIterator<Item = &'a PeerAdvertisedHead>,
    ) -> Option<&'a PeerAdvertisedHead> {
        let mut by_height: Vec<&PeerAdvertisedHead> = candidates.into_iter().collect();
        by_height.sort_unstable_by_key(|peer| peer.highest_block_height);
        by_height.get(by_height.len().checked_sub(1)? / 2).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_async::time::FakeClock;
    use near_crypto::{KeyType, SecretKey};
    use rand::SeedableRng;
    use std::collections::HashSet;

    fn peer_id(seed: &str) -> PeerId {
        PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed).public_key())
    }

    fn advertised(seed: &str, height: BlockHeight) -> PeerAdvertisedHead {
        PeerAdvertisedHead {
            peer_info: PeerInfo { id: peer_id(seed), addr: None, account_id: None },
            highest_block_height: height,
            highest_block_hash: CryptoHash::default(),
            archival: false,
        }
    }

    fn selector() -> (FakeClock, PeerSelector) {
        (FakeClock::default(), PeerSelector::new(Duration::seconds(60), StdRng::seed_from_u64(1)))
    }

    const NUM_PICKS: usize = 50;

    fn pick_peers<'a>(
        clock: &FakeClock,
        selector: &mut PeerSelector,
        peers: &[(&'a str, BlockHeight)],
    ) -> HashSet<&'a str> {
        let candidates: Vec<_> =
            peers.iter().map(|(name, height)| advertised(name, *height)).collect();
        (0..NUM_PICKS)
            .map(|_| {
                let picked = selector.pick(&candidates, clock.now_utc()).unwrap();
                let name = peers.iter().find(|(name, _)| peer_id(name) == picked.peer_info.id);
                name.unwrap().0
            })
            .collect()
    }

    #[test]
    fn only_peers_inside_the_window_get_asked() {
        let (clock, mut selector) = selector();
        let peers = [
            ("top", 101),
            ("median", 100),
            ("also_median", 100),
            ("edge", 100 - PEER_HEIGHT_WINDOW),
            ("below", 100 - PEER_HEIGHT_WINDOW - 1),
        ];
        let asked = pick_peers(&clock, &mut selector, &peers);
        assert_eq!(asked, HashSet::from(["top", "median", "also_median", "edge"]));
    }

    #[test]
    fn peer_that_failed_is_skipped() {
        let (clock, mut selector) = selector();
        let candidates = vec![advertised("low", 50), advertised("high", 100)];
        selector.record_failed_to_serve(&peer_id("high"), clock.now_utc());
        let picked = selector.pick(&candidates, clock.now_utc()).unwrap();
        assert_eq!(picked.peer_info.id, peer_id("low"));
    }

    #[test]
    fn failed_peer_returns_after_the_cooldown() {
        let (clock, mut selector) = selector();
        let peers = [("other", 100), ("failed", 100)];
        selector.record_failed_to_serve(&peer_id("failed"), clock.now_utc());
        clock.advance(Duration::seconds(61));
        let asked = pick_peers(&clock, &mut selector, &peers);
        assert!(asked.contains("failed"));
    }

    #[test]
    fn later_failure_restarts_the_cooldown() {
        let (clock, mut selector) = selector();
        let candidates = vec![advertised("low", 50), advertised("high", 100)];
        selector.record_failed_to_serve(&peer_id("high"), clock.now_utc());
        clock.advance(Duration::seconds(59));
        selector.record_failed_to_serve(&peer_id("high"), clock.now_utc());
        clock.advance(Duration::seconds(2));
        let picked = selector.pick(&candidates, clock.now_utc()).unwrap();
        assert_eq!(picked.peer_info.id, peer_id("low"));
    }

    #[test]
    fn every_peer_failed_ignores_the_failure_history() {
        let (clock, mut selector) = selector();
        let peers = [("first", 100), ("second", 100)];
        for (name, _) in peers {
            selector.record_failed_to_serve(&peer_id(name), clock.now_utc());
        }
        let asked = pick_peers(&clock, &mut selector, &peers);
        assert_eq!(asked, HashSet::from(["first", "second"]));
    }

    #[test]
    fn pick_matching_reads_the_median_among_suitable_peers_only() {
        let (clock, mut selector) = selector();
        let mut archival = advertised("archival", 50);
        archival.archival = true;
        let candidates = vec![archival, advertised("plain", 100), advertised("plain_two", 100)];
        let picked =
            selector.pick_matching(&candidates, clock.now_utc(), |peer| peer.archival).unwrap();
        assert_eq!(picked.peer_info.id, peer_id("archival"));
    }

    #[test]
    fn far_ahead_minority_does_not_push_honest_peers_out_of_window() {
        let (clock, mut selector) = selector();
        let honest_height = 100;
        let fake_height = 1_000_000;
        let peers = [
            ("honest_one", honest_height),
            ("honest_two", honest_height),
            ("honest_three", honest_height),
            ("liar_one", fake_height),
            ("liar_two", fake_height),
        ];
        let asked = pick_peers(&clock, &mut selector, &peers);
        assert!(HashSet::from(["honest_one", "honest_two", "honest_three"]).is_subset(&asked));
    }

    #[test]
    fn far_ahead_majority_pushes_honest_peers_out_of_window() {
        let (clock, mut selector) = selector();
        let honest_height = 100;
        let fake_height = 1_000_000;
        let peers = [
            ("honest_one", honest_height),
            ("honest_two", honest_height),
            ("liar_one", fake_height),
            ("liar_two", fake_height),
            ("liar_three", fake_height),
        ];
        let asked = pick_peers(&clock, &mut selector, &peers);
        assert_eq!(asked, HashSet::from(["liar_one", "liar_two", "liar_three"]));
    }

    #[test]
    fn median_advertised_head_takes_lower_middle_for_even_count() {
        let candidates = vec![
            advertised("high", 40),
            advertised("low", 10),
            advertised("upper_middle", 30),
            advertised("lower_middle", 20),
        ];
        let median = PeerSelector::median_advertised_head(&candidates).unwrap();
        assert_eq!(median.peer_info.id, peer_id("lower_middle"));
    }

    #[test]
    fn median_advertised_head_ignores_far_ahead_minority() {
        let honest_height = 100;
        let candidates = vec![
            advertised("liar", 1_000_000),
            advertised("honest_one", honest_height),
            advertised("honest_two", honest_height),
        ];
        let median = PeerSelector::median_advertised_head(&candidates).unwrap();
        assert_eq!(median.highest_block_height, honest_height);
    }

    #[test]
    fn median_without_recent_failures_stops_following_majority_once_it_fails() {
        let (clock, mut selector) = selector();
        let honest_height = 100;
        let fake_height = 1_000_000;
        let honest = ["honest_one", "honest_two", "honest_three", "honest_four"];
        let liars = ["liar_one", "liar_two", "liar_three", "liar_four", "liar_five"];
        let mut candidates: Vec<_> =
            honest.iter().map(|name| advertised(name, honest_height)).collect();
        candidates.extend(liars.iter().map(|name| advertised(name, fake_height)));

        let before_failures =
            selector.median_advertised_head_without_recent_failures(&candidates, clock.now_utc());
        assert_eq!(before_failures.unwrap().highest_block_height, fake_height);

        for liar in liars {
            selector.record_failed_to_serve(&peer_id(liar), clock.now_utc());
        }
        let after_failures =
            selector.median_advertised_head_without_recent_failures(&candidates, clock.now_utc());
        assert_eq!(after_failures.unwrap().highest_block_height, honest_height);
    }

    #[test]
    fn sync_target_comes_from_peers_above_trigger() {
        let (clock, selector) = selector();
        let trigger_height = 101;
        let tip_height = 1_000;
        let candidates = vec![
            advertised("lagging_one", trigger_height),
            advertised("lagging_two", trigger_height),
            advertised("tip", tip_height),
        ];
        let target =
            selector.sync_target_from_claimed_peers(&candidates, trigger_height, clock.now_utc());
        assert_eq!(target.unwrap().peer_info.id, peer_id("tip"));
    }

    #[test]
    fn no_candidates_gives_none() {
        let (clock, mut selector) = selector();
        assert!(selector.pick(&[], clock.now_utc()).is_none());
        assert!(PeerSelector::median_advertised_head(&[]).is_none());
    }
}
