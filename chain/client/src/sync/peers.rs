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

/// Peers this far below the best candidate are still worth asking, so requests
/// spread over several peers instead of landing on one.
const PEER_HEIGHT_WINDOW: BlockHeight = 5;

/// Decides which peers to ask, and remembers which ones did not deliver.
///
/// The policy is one place on purpose: ask a peer near the highest advertised
/// head, and drop one that fails to serve it until its cooldown ends. A peer
/// advertising a very high height is asked first and, if it does not deliver,
/// stops setting the mark for everyone else. When every peer has failed recently
/// the failure history is ignored, though the window still applies.
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
        // Peers that just failed us are dropped before the best height is read, so
        // a peer advertising a very high height cannot keep the others out of the
        // window by advertising it again.
        let mut pool: Vec<&PeerAdvertisedHead> =
            suitable().filter(|peer| !self.failed_recently(&peer.peer_info.id, now)).collect();
        if pool.is_empty() {
            pool = suitable().collect();
        }
        let best = pool.iter().map(|peer| peer.highest_block_height).max()?;
        let worth_asking = best.saturating_sub(PEER_HEIGHT_WINDOW);
        pool.into_iter()
            .filter(|peer| peer.highest_block_height >= worth_asking)
            .choose(&mut self.rng)
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

    #[test]
    fn only_peers_inside_the_window_get_asked() {
        let (clock, mut selector) = selector();
        let names = ["top", "edge", "below"];
        let candidates = vec![
            advertised("top", 100),
            advertised("edge", 100 - PEER_HEIGHT_WINDOW),
            advertised("below", 100 - PEER_HEIGHT_WINDOW - 1),
        ];
        let mut asked = HashSet::new();
        for _ in 0..50 {
            let picked = selector.pick(&candidates, clock.now_utc()).unwrap();
            let name = names.into_iter().find(|name| peer_id(name) == picked.peer_info.id);
            asked.insert(name.unwrap());
        }
        assert_eq!(asked, HashSet::from(["top", "edge"]));
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
        let candidates = vec![advertised("low", 50), advertised("high", 100)];
        selector.record_failed_to_serve(&peer_id("high"), clock.now_utc());
        clock.advance(Duration::seconds(61));
        let picked = selector.pick(&candidates, clock.now_utc()).unwrap();
        assert_eq!(picked.peer_info.id, peer_id("high"));
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
        let candidates = vec![advertised("low", 50), advertised("high", 100)];
        for seed in ["low", "high"] {
            selector.record_failed_to_serve(&peer_id(seed), clock.now_utc());
        }
        let picked = selector.pick(&candidates, clock.now_utc()).unwrap();
        assert_eq!(picked.peer_info.id, peer_id("high"));
    }

    #[test]
    fn pick_matching_reads_the_best_height_among_suitable_peers_only() {
        let (clock, mut selector) = selector();
        let mut archival = advertised("archival", 50);
        archival.archival = true;
        let candidates = vec![archival, advertised("plain", 100)];
        let picked =
            selector.pick_matching(&candidates, clock.now_utc(), |peer| peer.archival).unwrap();
        assert_eq!(picked.peer_info.id, peer_id("archival"));
    }

    #[test]
    fn no_candidates_gives_none() {
        let (clock, mut selector) = selector();
        assert!(selector.pick(&[], clock.now_utc()).is_none());
    }
}
