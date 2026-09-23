//! Per-peer business-logic metadata tracker.
//!
//! Owns the `PeerId → ConnectedPeerState` maps (split per tier) and
//! the derived TIER1 `account_key → peer_id` index. Source of truth
//! for "who is connected and on which tier" — consumed by routing
//! decisions, PMA background loops, and the `send_message_to_account`
//! T1 fast path.
//!
//! All read access returns owned data. No borrow-level iterator or
//! `MutexGuard` escapes this module. Every method takes the lock,
//! reads/writes, and returns — so it's impossible to hold a lock
//! across external calls.
//!
//! Transport-layer state (bandwidth, `last_time_received_message`)
//! lives on `Connection` and is surfaced through `TransportInfo`. This
//! module is only routing metadata.

use crate::network_protocol::PeerInfo;
use crate::tcp;
use crate::types::{BlockInfo, PeerType};
use near_async::time;
use near_crypto::PublicKey;
use near_primitives::network::PeerId;
use near_primitives::types::ShardId;
use parking_lot::Mutex;
use std::collections::HashMap;

/// Per-peer routing metadata. Populated by register
/// (`on_peer_connected`), updated incrementally by message handlers
/// (Block updates `block_info`), removed on unregister.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct ConnectedPeerState {
    pub peer_info: PeerInfo,
    /// Highest block we've heard of from this peer. `None` until the
    /// first `Block` message arrives. Distinct from `Connection::last_block`:
    /// this is the business-logic view (updated by `handle_peer_message`),
    /// independent of the TCP-transport layer. Allows routing decisions
    /// to work without touching `Connection`.
    pub block_info: Option<BlockInfo>,
    pub tier: tcp::Tier,
    pub archival: bool,
    pub tracked_shards: Vec<ShardId>,
    /// Account key this peer proved ownership of during handshake
    /// (T1 validators only).
    pub owned_account_key: Option<PublicKey>,
    pub peer_type: PeerType,
    pub established_time: time::Instant,
}

/// Canonical record of connected peers, split per tier.
///
/// Invariant: `tier1_by_account_key[k] = p` iff there is a T1 entry
/// at `p` with `owned_account_key == Some(k)`. Maintained by `insert`
/// and `remove`.
pub(crate) struct ConnectedPeers {
    tier1_peers: Mutex<HashMap<PeerId, ConnectedPeerState>>,
    tier2_peers: Mutex<HashMap<PeerId, ConnectedPeerState>>,
    tier3_peers: Mutex<HashMap<PeerId, ConnectedPeerState>>,
    tier1_by_account_key: Mutex<HashMap<PublicKey, PeerId>>,
}

#[allow(dead_code)]
impl ConnectedPeers {
    pub fn new() -> Self {
        Self {
            tier1_peers: Mutex::new(HashMap::new()),
            tier2_peers: Mutex::new(HashMap::new()),
            tier3_peers: Mutex::new(HashMap::new()),
            tier1_by_account_key: Mutex::new(HashMap::new()),
        }
    }

    fn peers_for(&self, tier: tcp::Tier) -> &Mutex<HashMap<PeerId, ConnectedPeerState>> {
        match tier {
            tcp::Tier::T1 => &self.tier1_peers,
            tcp::Tier::T2 => &self.tier2_peers,
            tcp::Tier::T3 => &self.tier3_peers,
        }
    }

    /// Insert a peer on registration. If the peer is T1 and has an
    /// `owned_account_key`, also populates the T1 secondary index.
    ///
    /// When the T1 secondary index must be updated, lock order is
    /// `peers_for(tier)` then `tier1_by_account_key`, held atomically
    /// across both writes. Matches `remove` for a consistent acquisition
    /// order. Holding both is required on that path: if we wrote the
    /// tier map first, dropped, then took the index lock, a concurrent
    /// `remove` could pop the tier entry and skip the index cleanup
    /// (because the index hasn't been written yet) — the index would
    /// then be left dangling when this insert completes step 2.
    ///
    /// For peers that don't contribute to the T1 secondary index
    /// (non-T1, or T1 without an `owned_account_key`), only the tier
    /// map is locked — avoids unnecessary contention with T1 index
    /// readers like `tier1_peer_for_account`.
    pub fn insert(&self, peer_id: PeerId, state: ConnectedPeerState) {
        if state.tier == tcp::Tier::T1 {
            if let Some(account_key) = state.owned_account_key.clone() {
                let mut peers = self.peers_for(state.tier).lock();
                let mut index = self.tier1_by_account_key.lock();
                peers.insert(peer_id.clone(), state);
                index.insert(account_key, peer_id);
                return;
            }
        }
        self.peers_for(state.tier).lock().insert(peer_id, state);
    }

    /// Remove a peer on disconnection. Returns the prior state so the
    /// caller can drive follow-up cleanup (peer_store ban, edge
    /// removal broadcast, etc.).
    ///
    /// Mirrors `insert`: when `tier == T1`, holds both
    /// `peers_for(tier)` and the T1 index atomically (lock order: tier
    /// map first, then index — same as insert). The T1 index is touched
    /// only when `tier == T1`, and cleared only if it still points to
    /// *this* peer_id — protects against a later peer with the same
    /// `account_key` registering before this remove drains.
    pub fn remove(&self, tier: tcp::Tier, peer_id: &PeerId) -> Option<ConnectedPeerState> {
        let mut peers = self.peers_for(tier).lock();
        if tier == tcp::Tier::T1 {
            let mut index = self.tier1_by_account_key.lock();
            let removed = peers.remove(peer_id)?;
            if let Some(key) = &removed.owned_account_key {
                if index.get(key) == Some(peer_id) {
                    index.remove(key);
                }
            }
            Some(removed)
        } else {
            peers.remove(peer_id)
        }
    }

    /// Non-decreasing `block_info` update: applies if `new.height` is
    /// at least the peer's current recorded height. Same-height updates
    /// ARE applied — a peer can switch to a different fork at the same
    /// height and we want to record the new hash. Strict regressions
    /// (lower height) are dropped. Updates the entry on every tier the
    /// peer is connected on; no-op if the peer isn't connected on any
    /// tier.
    pub fn update_block_info(&self, peer_id: &PeerId, new: BlockInfo) {
        for tier_map in [&self.tier1_peers, &self.tier2_peers, &self.tier3_peers] {
            let mut peers = tier_map.lock();
            if let Some(s) = peers.get_mut(peer_id) {
                if s.block_info.as_ref().map_or(true, |bi| bi.height <= new.height) {
                    s.block_info = Some(new);
                }
            }
        }
    }

    /// Is `peer_id` in our connected set on any tier?
    pub fn is_connected(&self, peer_id: &PeerId) -> bool {
        self.tier1_peers.lock().contains_key(peer_id)
            || self.tier2_peers.lock().contains_key(peer_id)
            || self.tier3_peers.lock().contains_key(peer_id)
    }

    /// Is `peer_id` connected specifically on `tier`?
    pub fn is_connected_on_tier(&self, peer_id: &PeerId, tier: tcp::Tier) -> bool {
        self.peers_for(tier).lock().contains_key(peer_id)
    }

    /// Owned clone of a peer's state, if connected on any tier.
    pub fn get(&self, peer_id: &PeerId) -> Option<ConnectedPeerState> {
        for tier_map in [&self.tier1_peers, &self.tier2_peers, &self.tier3_peers] {
            if let Some(s) = tier_map.lock().get(peer_id) {
                return Some(s.clone());
            }
        }
        None
    }

    /// Owned snapshot of every peer on TIER1.
    pub fn tier1(&self) -> HashMap<PeerId, ConnectedPeerState> {
        Self::snapshot(&self.tier1_peers)
    }

    /// Owned snapshot of every peer on TIER2.
    pub fn tier2(&self) -> HashMap<PeerId, ConnectedPeerState> {
        Self::snapshot(&self.tier2_peers)
    }

    /// Owned snapshot of every peer on TIER3.
    pub fn tier3(&self) -> HashMap<PeerId, ConnectedPeerState> {
        Self::snapshot(&self.tier3_peers)
    }

    fn snapshot(
        map: &Mutex<HashMap<PeerId, ConnectedPeerState>>,
    ) -> HashMap<PeerId, ConnectedPeerState> {
        map.lock().clone()
    }

    /// O(1) T1 peer lookup by account key. Used by the
    /// `send_message_to_account` T1 fast path and by `get_tier1_proxy`.
    pub fn tier1_peer_for_account(&self, key: &PublicKey) -> Option<PeerId> {
        self.tier1_by_account_key.lock().get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network_protocol::testonly as data;
    use crate::testonly::make_rng;
    use near_primitives::hash::CryptoHash;

    fn test_state(
        peer_id: &PeerId,
        tier: tcp::Tier,
        peer_type: PeerType,
        owned_account_key: Option<PublicKey>,
    ) -> ConnectedPeerState {
        let mut rng = make_rng(12345);
        let mut peer_info = data::make_peer_info(&mut rng);
        peer_info.id = peer_id.clone();
        ConnectedPeerState {
            peer_info,
            block_info: None,
            tier,
            archival: false,
            tracked_shards: vec![],
            owned_account_key,
            peer_type,
            established_time: near_async::time::Clock::real().now(),
        }
    }

    fn key(seed: u64) -> PublicKey {
        near_crypto::SecretKey::from_seed(near_crypto::KeyType::ED25519, &seed.to_string())
            .public_key()
    }

    fn peer(seed: u64) -> PeerId {
        PeerId::new(key(seed))
    }

    #[test]
    fn insert_tier1_populates_account_index() {
        let peers = ConnectedPeers::new();
        let p = peer(1);
        let k = key(100);
        peers.insert(p.clone(), test_state(&p, tcp::Tier::T1, PeerType::Outbound, Some(k.clone())));

        assert_eq!(peers.tier1_peer_for_account(&k), Some(p.clone()));
        assert!(peers.is_connected_on_tier(&p, tcp::Tier::T1));
    }

    #[test]
    fn insert_tier2_skips_account_index() {
        // A T2 peer with an owned_account_key set (hypothetical) must
        // NOT be inserted into the T1 secondary index.
        let peers = ConnectedPeers::new();
        let p = peer(2);
        let k = key(101);
        peers.insert(p.clone(), test_state(&p, tcp::Tier::T2, PeerType::Outbound, Some(k.clone())));

        assert_eq!(peers.tier1_peer_for_account(&k), None);
        assert!(peers.is_connected_on_tier(&p, tcp::Tier::T2));
    }

    #[test]
    fn remove_clears_account_index_on_t1() {
        let peers = ConnectedPeers::new();
        let p = peer(3);
        let k = key(102);
        peers.insert(p.clone(), test_state(&p, tcp::Tier::T1, PeerType::Outbound, Some(k.clone())));
        let removed = peers.remove(tcp::Tier::T1, &p);
        assert!(removed.is_some());
        assert_eq!(peers.tier1_peer_for_account(&k), None);
        assert!(!peers.is_connected(&p));
    }

    #[test]
    fn remove_non_t1_does_not_touch_index() {
        // Two peers: A on T1 with key K (populates index), B on T2 also
        // with key K (not in index). Removing B must leave K → A intact.
        let peers = ConnectedPeers::new();
        let a = peer(4);
        let b = peer(5);
        let k = key(103);
        peers.insert(a.clone(), test_state(&a, tcp::Tier::T1, PeerType::Outbound, Some(k.clone())));
        peers.insert(b.clone(), test_state(&b, tcp::Tier::T2, PeerType::Outbound, Some(k.clone())));

        assert_eq!(peers.tier1_peer_for_account(&k), Some(a.clone()));
        peers.remove(tcp::Tier::T2, &b);
        assert_eq!(peers.tier1_peer_for_account(&k), Some(a));
    }

    #[test]
    fn remove_race_preserves_index() {
        // A registers (T1, key K). B registers (T1, key K) — index now
        // points K → B. A unregisters (T1, peer A). The defensive check
        // inside `remove` must leave the index pointing to B.
        let peers = ConnectedPeers::new();
        let a = peer(6);
        let b = peer(7);
        let k = key(104);
        peers.insert(a.clone(), test_state(&a, tcp::Tier::T1, PeerType::Outbound, Some(k.clone())));
        peers.insert(b.clone(), test_state(&b, tcp::Tier::T1, PeerType::Outbound, Some(k.clone())));
        assert_eq!(peers.tier1_peer_for_account(&k), Some(b.clone()));

        peers.remove(tcp::Tier::T1, &a);
        assert_eq!(peers.tier1_peer_for_account(&k), Some(b));
    }

    #[test]
    fn update_block_info_monotonic() {
        let peers = ConnectedPeers::new();
        let p = peer(8);
        peers.insert(p.clone(), test_state(&p, tcp::Tier::T2, PeerType::Outbound, None));

        let hash10 = CryptoHash::hash_bytes(b"b10");
        let hash5 = CryptoHash::hash_bytes(b"b5");
        let hash20 = CryptoHash::hash_bytes(b"b20");

        peers.update_block_info(&p, BlockInfo { height: 10, hash: hash10 });
        assert_eq!(peers.get(&p).unwrap().block_info, Some(BlockInfo { height: 10, hash: hash10 }));

        // Regression (lower height) is ignored.
        peers.update_block_info(&p, BlockInfo { height: 5, hash: hash5 });
        assert_eq!(peers.get(&p).unwrap().block_info, Some(BlockInfo { height: 10, hash: hash10 }));

        // Higher height advances.
        peers.update_block_info(&p, BlockInfo { height: 20, hash: hash20 });
        assert_eq!(peers.get(&p).unwrap().block_info, Some(BlockInfo { height: 20, hash: hash20 }));
    }

    #[test]
    fn update_block_info_same_height_updates_hash() {
        // Same-height updates ARE applied — a peer can switch to a
        // different fork at the same height.
        let peers = ConnectedPeers::new();
        let p = peer(11);
        peers.insert(p.clone(), test_state(&p, tcp::Tier::T2, PeerType::Outbound, None));

        let hash_a = CryptoHash::hash_bytes(b"fork_a");
        let hash_b = CryptoHash::hash_bytes(b"fork_b");

        peers.update_block_info(&p, BlockInfo { height: 10, hash: hash_a });
        assert_eq!(peers.get(&p).unwrap().block_info, Some(BlockInfo { height: 10, hash: hash_a }));

        // Same height, different hash — should update.
        peers.update_block_info(&p, BlockInfo { height: 10, hash: hash_b });
        assert_eq!(peers.get(&p).unwrap().block_info, Some(BlockInfo { height: 10, hash: hash_b }));
    }

    #[test]
    fn update_block_info_unknown_peer_is_noop() {
        let peers = ConnectedPeers::new();
        let p = peer(9);
        // No insert. Should not panic.
        peers.update_block_info(&p, BlockInfo { height: 1, hash: CryptoHash::default() });
        assert!(!peers.is_connected(&p));
    }

    #[test]
    fn tier_snapshots_return_owned_clones() {
        let peers = ConnectedPeers::new();
        let p = peer(10);
        peers.insert(p.clone(), test_state(&p, tcp::Tier::T2, PeerType::Outbound, None));

        let mut snapshot = peers.tier2();
        snapshot.clear();
        // Original state unaffected.
        assert!(peers.is_connected_on_tier(&p, tcp::Tier::T2));
        assert_eq!(peers.tier2().len(), 1);
    }

    #[test]
    fn tier3_insert_remove_no_account_index() {
        // T3 peers use `ConnectedPeers::insert`/`remove` exactly like
        // T1/T2, but MUST NOT touch the T1 account-key index even if
        // `owned_account_key` is set (T3 is an ad-hoc request tier).
        let peers = ConnectedPeers::new();
        let p = peer(12);
        let k = key(105);
        peers.insert(p.clone(), test_state(&p, tcp::Tier::T3, PeerType::Outbound, Some(k.clone())));

        assert!(peers.is_connected_on_tier(&p, tcp::Tier::T3));
        assert!(!peers.is_connected_on_tier(&p, tcp::Tier::T2));
        assert_eq!(peers.tier3().len(), 1);
        assert_eq!(peers.tier1_peer_for_account(&k), None);

        peers.remove(tcp::Tier::T3, &p);
        assert!(!peers.is_connected(&p));
        assert_eq!(peers.tier3().len(), 0);
        assert_eq!(peers.tier1_peer_for_account(&k), None);
    }
}
