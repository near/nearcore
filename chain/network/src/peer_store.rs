use std::collections::{hash_map::Iter, HashMap};
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;

use borsh::BorshSerialize;
use chrono::Utc;
use log::{debug, error};
use rand::seq::SliceRandom;
use rand::thread_rng;

use near_primitives::network::PeerId;
use near_primitives::utils::to_timestamp;
use near_store::{ColPeers, Store};

use crate::types::{KnownPeerState, KnownPeerStatus, NetworkConfig, PeerInfo, ReasonForBan};

/// Level of trust we have about a new (PeerId, Addr) pair.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum TrustLevel {
    /// We learn about it from other peers.
    Indirect,
    /// Responding node at addr claims to possess PeerId.
    Direct,
    /// Responding peer proved to have SecretKey associated with this PeerID.
    Signed,
}

#[derive(Debug, Clone)]
struct VerifiedPeer {
    peer_id: PeerId,
    trust_level: TrustLevel,
}

impl VerifiedPeer {
    fn new(peer_id: PeerId) -> Self {
        Self { peer_id, trust_level: TrustLevel::Indirect }
    }
    fn signed(peer_id: PeerId) -> Self {
        Self { peer_id, trust_level: TrustLevel::Signed }
    }
}

/// Known peers store, maintaining cache of known peers and connection to storage to save/load them.
pub struct PeerStore {
    store: Arc<Store>,
    peer_states: HashMap<PeerId, KnownPeerState>,
    // This is a reverse index, from physical address to peer_id
    // It can happens that some peers don't have known address, so
    // they will not be present in this list, otherwise they will be present.
    addr_peers: HashMap<SocketAddr, VerifiedPeer>,
}

impl PeerStore {
    pub fn new(
        store: Arc<Store>,
        boot_nodes: &[PeerInfo],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut peer_states = HashMap::default();
        let mut addr_peers = HashMap::default();

        for peer_info in boot_nodes.iter().cloned() {
            if !peer_states.contains_key(&peer_info.id) {
                if let Some(peer_addr) = peer_info.addr.clone() {
                    if addr_peers.contains_key(&peer_addr) {
                        // There is already a different peer_id with this address.
                        error!(target: "network", "Two boot nodes have the same address {:?}", peer_addr);
                    } else {
                        addr_peers.insert(peer_addr, VerifiedPeer::signed(peer_info.id.clone()));
                        peer_states
                            .insert(peer_info.id.clone(), KnownPeerState::new(peer_info.clone()));
                    }
                }
            }
        }

        for (key, value) in store.iter(ColPeers) {
            let key: Vec<u8> = key.into();
            let value: Vec<u8> = value.into();
            let peer_id: PeerId = key.try_into()?;
            let mut peer_state: KnownPeerState = value.try_into()?;
            // Mark loaded node last seen to now, to avoid deleting them as soon as they are loaded.
            peer_state.last_seen = to_timestamp(Utc::now());
            match peer_state.status {
                KnownPeerStatus::Banned(_, _) => {}
                _ => peer_state.status = KnownPeerStatus::NotConnected,
            };

            if let Some(current_peer_state) = peer_states.get_mut(&peer_id) {
                // This peer is a boot node and was already added so skip.
                if peer_state.status.is_banned() {
                    current_peer_state.status = peer_state.status;
                }
                continue;
            }

            if let Some(peer_addr) = peer_state.peer_info.addr.clone() {
                if !addr_peers.contains_key(&peer_addr) {
                    addr_peers
                        .insert(peer_addr, VerifiedPeer::new(peer_state.peer_info.id.clone()));
                    peer_states.insert(peer_id, peer_state);
                }
            }
        }
        Ok(PeerStore { store, peer_states, addr_peers })
    }

    pub fn len(&self) -> usize {
        self.peer_states.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peer_states.is_empty()
    }

    pub fn is_banned(&self, peer_id: &PeerId) -> bool {
        self.peer_states
            .get(&peer_id)
            .map_or(false, |known_peer_state| known_peer_state.status.is_banned())
    }

    pub fn peer_connected(
        &mut self,
        peer_info: &PeerInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.add_trusted_peer(peer_info.clone(), TrustLevel::Signed)?;
        let entry = self.peer_states.get_mut(&peer_info.id).unwrap();
        entry.last_seen = to_timestamp(Utc::now());
        entry.status = KnownPeerStatus::Connected;
        let mut store_update = self.store.store_update();
        store_update.set_ser(ColPeers, &peer_info.id.try_to_vec()?, entry)?;
        store_update.commit().map_err(|err| err.into())
    }

    pub fn peer_disconnected(
        &mut self,
        peer_id: &PeerId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.last_seen = to_timestamp(Utc::now());
            peer_state.status = KnownPeerStatus::NotConnected;
            let mut store_update = self.store.store_update();
            store_update.set_ser(ColPeers, &peer_id.try_to_vec()?, peer_state)?;
            store_update.commit().map_err(|err| err.into())
        } else {
            Err(format!("Peer {} is missing in the peer store", peer_id).into())
        }
    }

    pub fn peer_ban(
        &mut self,
        peer_id: &PeerId,
        ban_reason: ReasonForBan,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.last_seen = to_timestamp(Utc::now());
            peer_state.status = KnownPeerStatus::Banned(ban_reason, to_timestamp(Utc::now()));
            let mut store_update = self.store.store_update();
            store_update.set_ser(ColPeers, &peer_id.try_to_vec()?, peer_state)?;
            store_update.commit().map_err(|err| err.into())
        } else {
            Err(format!("Peer {} is missing in the peer store", peer_id).into())
        }
    }

    pub fn peer_unban(&mut self, peer_id: &PeerId) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.status = KnownPeerStatus::NotConnected;
            let mut store_update = self.store.store_update();
            store_update.set_ser(ColPeers, &peer_id.try_to_vec()?, peer_state)?;
            store_update.commit().map_err(|err| err.into())
        } else {
            Err(format!("Peer {} is missing in the peer store", peer_id).into())
        }
    }

    fn find_peers<F>(&self, mut filter: F, count: u32) -> Vec<PeerInfo>
    where
        F: FnMut(&KnownPeerState) -> bool,
    {
        let mut peers = self
            .peer_states
            .values()
            .filter_map(|p| if filter(p) { Some(p.peer_info.clone()) } else { None })
            .collect::<Vec<_>>();
        if count == 0 {
            return peers;
        }
        peers.shuffle(&mut thread_rng());
        peers.iter().take(count as usize).cloned().collect::<Vec<_>>()
    }

    /// Return unconnected or peers with unknown status that we can try to connect to.
    /// Peers with unknown addresses are filtered out.
    pub fn unconnected_peers(&self, ignore_fn: impl Fn(&KnownPeerState) -> bool) -> Vec<PeerInfo> {
        self.find_peers(
            |p| {
                (p.status == KnownPeerStatus::NotConnected || p.status == KnownPeerStatus::Unknown)
                    && !ignore_fn(p)
                    && p.peer_info.addr.is_some()
            },
            0,
        )
    }

    /// Return healthy known peers up to given amount.
    pub fn healthy_peers(&self, max_count: u32) -> Vec<PeerInfo> {
        self.find_peers(
            |p| match p.status {
                KnownPeerStatus::Banned(_, _) => false,
                _ => true,
            },
            max_count,
        )
    }

    /// Return iterator over all known peers.
    pub fn iter(&self) -> Iter<'_, PeerId, KnownPeerState> {
        self.peer_states.iter()
    }

    /// Removes peers that are not responding for expiration period.
    pub fn remove_expired(
        &mut self,
        config: &NetworkConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let mut to_remove = vec![];
        for (peer_id, peer_status) in self.peer_states.iter() {
            let diff = (now - peer_status.last_seen()).to_std()?;
            if peer_status.status != KnownPeerStatus::Connected
                && diff > config.peer_expiration_duration
            {
                debug!(target: "network", "Removing peer: last seen {:?}", diff);
                to_remove.push(peer_id.clone());
            }
        }
        let mut store_update = self.store.store_update();
        for peer_id in to_remove {
            self.peer_states.remove(&peer_id);
            store_update.delete(ColPeers, &peer_id.try_to_vec()?);
        }
        store_update.commit().map_err(|err| err.into())
    }

    fn touch(&mut self, peer_id: &PeerId) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get(&peer_id) {
            let mut store_update = self.store.store_update();
            store_update.set_ser(ColPeers, &peer_id.try_to_vec()?, peer_state)?;
            store_update.commit().map_err(|err| err.into())
        } else {
            Ok(())
        }
    }

    /// Create new pair between peer_info.id and peer_addr removing
    /// old pairs if necessary.
    fn update_peer_info(
        &mut self,
        peer_info: PeerInfo,
        peer_addr: SocketAddr,
        trust_level: TrustLevel,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut touch_other = None;

        // If there is a peer associated with current address remove the address from it.
        if let Some(verified_peer) = self.addr_peers.remove(&peer_addr) {
            self.peer_states.entry(verified_peer.peer_id).and_modify(|peer_state| {
                peer_state.peer_info.addr = None;
                touch_other = Some(peer_state.peer_info.id.clone());
            });
        }

        // If this peer already has an address, remove that pair from the index.
        if let Some(peer_state) = self.peer_states.get_mut(&peer_info.id) {
            if let Some(cur_addr) = peer_state.peer_info.addr.take() {
                self.addr_peers.remove(&cur_addr);
            }
        }

        // Add new address
        self.addr_peers
            .insert(peer_addr.clone(), VerifiedPeer { peer_id: peer_info.id.clone(), trust_level });

        // Update peer_id addr
        self.peer_states
            .entry(peer_info.id.clone())
            .and_modify(|peer_state| peer_state.peer_info.addr = Some(peer_addr))
            .or_insert_with(|| KnownPeerState::new(peer_info.clone()));

        self.touch(&peer_info.id)?;
        if let Some(touch_other) = touch_other {
            self.touch(&touch_other)?;
        }
        Ok(())
    }

    /// Add list of peers into store.
    /// When verified is true is because we establish direct connection with such peer and know
    /// for sure its identity. If we receive a list of peers from another node in the network
    /// by default all of them are unverified.
    fn add_peer(
        &mut self,
        peer_info: PeerInfo,
        trust_level: TrustLevel,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_addr) = peer_info.addr {
            match trust_level {
                TrustLevel::Signed => {
                    self.update_peer_info(peer_info, peer_addr, TrustLevel::Signed)?;
                }
                TrustLevel::Direct => {
                    // If this peer already exists with a signed connection ignore this update.
                    // Warning: This is a problem for nodes that changes its address without changing peer_id.
                    //          It is recommended to change peer_id if address is changed.
                    if self.peer_states.get(&peer_info.id).map_or(false, |peer_state| {
                        peer_state.peer_info.addr.map_or(false, |current_addr| {
                            self.addr_peers.get(&current_addr).map_or(false, |verified_peer| {
                                verified_peer.trust_level == TrustLevel::Signed
                            })
                        })
                    }) {
                        return Ok(());
                    }

                    self.update_peer_info(peer_info, peer_addr, TrustLevel::Direct)?;
                }
                TrustLevel::Indirect => {
                    // We should only update an Indirect connection if we don't know anything about the peer
                    // or about the address.
                    if !self.peer_states.contains_key(&peer_info.id)
                        && !self.addr_peers.contains_key(&peer_addr)
                    {
                        self.update_peer_info(peer_info, peer_addr, TrustLevel::Indirect)?;
                    }
                }
            }
        } else {
            // If doesn't have the address attached it is not verified and we add it
            // only if it is unknown to us.
            if !self.peer_states.contains_key(&peer_info.id) {
                self.peer_states.insert(peer_info.id.clone(), KnownPeerState::new(peer_info));
            }
        }
        Ok(())
    }

    pub fn add_indirect_peers(
        &mut self,
        peers: Vec<PeerInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for peer_info in peers {
            self.add_peer(peer_info, TrustLevel::Indirect)?;
        }
        Ok(())
    }

    pub fn add_trusted_peer(
        &mut self,
        peer_info: PeerInfo,
        trust_level: TrustLevel,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.add_peer(peer_info, trust_level)
    }
}

#[cfg(test)]
mod test {
    use near_crypto::{KeyType, SecretKey};
    use near_store::create_store;
    use near_store::test_utils::create_test_store;

    use super::*;

    fn get_peer_id(seed: String) -> PeerId {
        SecretKey::from_seed(KeyType::ED25519, seed.as_str()).public_key().into()
    }

    fn get_addr(port: u8) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    fn get_peer_info(peer_id: PeerId, addr: Option<SocketAddr>) -> PeerInfo {
        PeerInfo { id: peer_id, addr, account_id: None }
    }

    fn gen_peer_info(port: u8) -> PeerInfo {
        PeerInfo {
            id: PeerId::from(SecretKey::from_random(KeyType::ED25519).public_key()),
            addr: Some(get_addr(port)),
            account_id: None,
        }
    }

    #[test]
    fn ban_store() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_store_ban").tempdir().unwrap();
        let peer_info_a = gen_peer_info(0);
        let peer_info_to_ban = gen_peer_info(1);
        let boot_nodes = vec![peer_info_a.clone(), peer_info_to_ban.clone()];
        {
            let store = create_store(tmp_dir.path().to_str().unwrap());
            let mut peer_store = PeerStore::new(store, &boot_nodes).unwrap();
            assert_eq!(peer_store.healthy_peers(3).iter().count(), 2);
            peer_store.peer_ban(&peer_info_to_ban.id, ReasonForBan::Abusive).unwrap();
            assert_eq!(peer_store.healthy_peers(3).iter().count(), 1);
        }
        {
            let store_new = create_store(tmp_dir.path().to_str().unwrap());
            let peer_store_new = PeerStore::new(store_new, &boot_nodes).unwrap();
            assert_eq!(peer_store_new.healthy_peers(3).iter().count(), 1);
        }
    }

    fn check_exist(
        peer_store: &PeerStore,
        peer_id: &PeerId,
        addr_level: Option<(SocketAddr, TrustLevel)>,
    ) -> bool {
        if let Some(peer_info) = peer_store.peer_states.get(&peer_id) {
            let peer_info = &peer_info.peer_info;
            if let Some((addr, level)) = addr_level {
                peer_info.addr.map_or(false, |cur_addr| cur_addr == addr)
                    && peer_store
                        .addr_peers
                        .get(&addr)
                        .map_or(false, |verified| verified.trust_level == level)
            } else {
                peer_info.addr.is_none()
            }
        } else {
            false
        }
    }

    fn check_integrity(peer_store: &PeerStore) -> bool {
        peer_store.peer_states.clone().iter().all(|(k, v)| {
            if let Some(addr) = v.peer_info.addr {
                if peer_store.addr_peers.get(&addr).map_or(true, |value| value.peer_id != *k) {
                    return false;
                }
            }
            true
        }) && peer_store.addr_peers.clone().iter().all(|(k, v)| {
            !peer_store
                .peer_states
                .get(&v.peer_id)
                .map_or(true, |value| value.peer_info.addr.map_or(true, |addr| addr != *k))
        })
    }

    /// If we know there is a peer_id A at address #A, and after some time
    /// we learn that there is a new peer B at address #A, we discard address of A
    #[test]
    fn handle_peer_id_change() {
        let store = create_test_store();
        let mut peer_store = PeerStore::new(store, &[]).unwrap();

        let peers_id = (0..2).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
        let addr = get_addr(0);

        let peer_aa = get_peer_info(peers_id[0].clone(), Some(addr));
        peer_store.peer_connected(&peer_aa).unwrap();
        assert!(check_exist(&peer_store, &peers_id[0], Some((addr, TrustLevel::Signed))));

        let peer_ba = get_peer_info(peers_id[1].clone(), Some(addr));
        peer_store.add_peer(peer_ba, TrustLevel::Direct).unwrap();

        assert!(check_exist(&peer_store, &peers_id[0], None));
        assert!(check_exist(&peer_store, &peers_id[1], Some((addr, TrustLevel::Direct))));
        assert!(check_integrity(&peer_store));
    }

    /// If we know there is a peer_id A at address #A, and then we learn about
    /// the same peer_id A at address #B, if that connection wasn't signed it is not updated,
    /// to avoid malicious actor making us forget about known peers.
    #[test]
    fn dont_handle_address_change() {
        let store = create_test_store();
        let mut peer_store = PeerStore::new(store, &[]).unwrap();

        let peers_id = (0..1).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
        let addrs = (0..2).map(|ix| get_addr(ix)).collect::<Vec<_>>();

        let peer_aa = get_peer_info(peers_id[0].clone(), Some(addrs[0]));
        peer_store.peer_connected(&peer_aa).unwrap();
        assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));

        let peer_ba = get_peer_info(peers_id[0].clone(), Some(addrs[1]));
        peer_store.add_peer(peer_ba, TrustLevel::Direct).unwrap();
        assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
        assert!(check_integrity(&peer_store));
    }

    #[test]
    fn check_add_peers_overriding() {
        let store = create_test_store();
        let mut peer_store = PeerStore::new(store.clone(), &[]).unwrap();

        // Five peers: A, B, C, D, X, T
        let peers_id = (0..6).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
        // Five addresses: #A, #B, #C, #D, #X, #T
        let addrs = (0..6).map(|ix| get_addr(ix)).collect::<Vec<_>>();

        // Create signed connection A - #A
        let peer_00 = get_peer_info(peers_id[0].clone(), Some(addrs[0]));
        peer_store.peer_connected(&peer_00).unwrap();
        assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
        assert!(check_integrity(&peer_store));

        // Create direct connection B - #B
        let peer_11 = get_peer_info(peers_id[1].clone(), Some(addrs[1]));
        peer_store.add_peer(peer_11.clone(), TrustLevel::Direct).unwrap();
        assert!(check_exist(&peer_store, &peers_id[1], Some((addrs[1], TrustLevel::Direct))));
        assert!(check_integrity(&peer_store));

        // Create signed connection B - #B
        peer_store.peer_connected(&peer_11).unwrap();
        assert!(check_exist(&peer_store, &peers_id[1], Some((addrs[1], TrustLevel::Signed))));
        assert!(check_integrity(&peer_store));

        // Create indirect connection C - #C
        let peer_22 = get_peer_info(peers_id[2].clone(), Some(addrs[2]));
        peer_store.add_peer(peer_22.clone(), TrustLevel::Indirect).unwrap();
        assert!(check_exist(&peer_store, &peers_id[2], Some((addrs[2], TrustLevel::Indirect))));
        assert!(check_integrity(&peer_store));

        // Create signed connection C - #C
        peer_store.peer_connected(&peer_22).unwrap();
        assert!(check_exist(&peer_store, &peers_id[2], Some((addrs[2], TrustLevel::Signed))));
        assert!(check_integrity(&peer_store));

        // Create signed connection C - #B
        // This overrides C - #C and B - #B
        let peer_21 = get_peer_info(peers_id[2].clone(), Some(addrs[1]));
        peer_store.peer_connected(&peer_21).unwrap();
        assert!(check_exist(&peer_store, &peers_id[1], None));
        assert!(check_exist(&peer_store, &peers_id[2], Some((addrs[1], TrustLevel::Signed))));
        assert!(check_integrity(&peer_store));

        // Create indirect connection D - #D
        let peer_33 = get_peer_info(peers_id[3].clone(), Some(addrs[3]));
        peer_store.add_peer(peer_33, TrustLevel::Indirect).unwrap();
        assert!(check_exist(&peer_store, &peers_id[3], Some((addrs[3], TrustLevel::Indirect))));
        assert!(check_integrity(&peer_store));

        // Try to create indirect connection A - #X but fails since A - #A exists
        let peer_04 = get_peer_info(peers_id[0].clone(), Some(addrs[4]));
        peer_store.add_peer(peer_04, TrustLevel::Indirect).unwrap();
        assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
        assert!(check_integrity(&peer_store));

        // Try to create indirect connection X - #D but fails since D - #D exists
        let peer_43 = get_peer_info(peers_id[4].clone(), Some(addrs[3]));
        peer_store.add_peer(peer_43.clone(), TrustLevel::Indirect).unwrap();
        assert!(check_exist(&peer_store, &peers_id[3], Some((addrs[3], TrustLevel::Indirect))));
        assert!(check_integrity(&peer_store));

        // Create Direct connection X - #D and succeed removing connection D - #D
        peer_store.add_peer(peer_43, TrustLevel::Direct).unwrap();
        assert!(check_exist(&peer_store, &peers_id[4], Some((addrs[3], TrustLevel::Direct))));
        // D should still exist, but without any addr
        assert!(check_exist(&peer_store, &peers_id[3], None));
        assert!(check_integrity(&peer_store));

        // Try to create indirect connection A - #T but fails since A - #A (signed) exists
        let peer_05 = get_peer_info(peers_id[0].clone(), Some(addrs[5]));
        peer_store.add_peer(peer_05, TrustLevel::Direct).unwrap();
        assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
        assert!(check_integrity(&peer_store));

        // Check we are able to recover from store previous signed connection
        let peer_store_2 = PeerStore::new(store, &[]).unwrap();
        assert!(check_exist(&peer_store_2, &peers_id[0], Some((addrs[0], TrustLevel::Indirect))));
        assert!(check_integrity(&peer_store_2));
    }
}
