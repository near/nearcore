use borsh::{BorshDeserialize, BorshSerialize};
use near_network_primitives::types::{
    Blacklist, KnownPeerState, KnownPeerStatus, NetworkConfig, PeerInfo, ReasonForBan,
};
use near_primitives::network::PeerId;
use near_primitives::time::{Clock, Utc};
use near_primitives::utils::to_timestamp;
use near_store::{DBCol, Store};
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::collections::hash_map::{Entry, Iter};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::ops::Not;
use tracing::{debug, error, info};

/// Level of trust we have about a new (PeerId, Addr) pair.
#[derive(Eq, PartialEq, Debug, Clone)]
enum TrustLevel {
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
    store: Store,
    peer_states: HashMap<PeerId, KnownPeerState>,
    // This is a reverse index, from physical address to peer_id
    // It can happens that some peers don't have known address, so
    // they will not be present in this list, otherwise they will be present.
    addr_peers: HashMap<SocketAddr, VerifiedPeer>,
    blacklist: Blacklist,
}

impl PeerStore {
    pub(crate) fn new(
        store: Store,
        boot_nodes: &[PeerInfo],
        blacklist: Blacklist,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // A mapping from `PeerId` to `KnownPeerState`.
        let mut peerid_2_state = HashMap::default();
        // Stores mapping from `SocketAddr` to `VerifiedPeer`, which contains `PeerId`.
        // Only one peer can exist with given `PeerId` or `SocketAddr`.
        // In case of collision, we will choose the first one.
        let mut addr_2_peer = HashMap::default();

        let now = Clock::utc();
        boot_nodes.iter().for_each(|peer_info|{
            if !peerid_2_state.contains_key(&peer_info.id) {
                if let Some(peer_addr) = peer_info.addr {
                    match addr_2_peer.entry(peer_addr) {
                        Entry::Occupied(entry) => {
                            // There is already a different peer_id with this address.
                            error!(target: "network", "Two boot nodes have the same address {:?}", entry.key());
                            // TODO: add panic!, `boot_nodes` list is wrong
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(VerifiedPeer::signed(peer_info.id.clone()));
                            peerid_2_state.insert(
                                peer_info.id.clone(),
                                KnownPeerState::new(peer_info.clone(), now),
                            );
                        }
                    }
                }
            }
            else {
                error!(id = ?peer_info.id, "There is a duplicated peer in boot_nodes")
            }
        });

        let now = to_timestamp(Utc::now());
        for (key, value) in store.iter(DBCol::Peers) {
            let peer_id: PeerId = PeerId::try_from_slice(key.as_ref())?;
            let peer_state: KnownPeerState = KnownPeerState::try_from_slice(value.as_ref())?;

            // Mark loaded node last seen to now, to avoid deleting them as soon as they are loaded.
            let last_seen = now;

            // If it’s already banned, keep it banned.  If it’s blacklisted, ban
            // it.  Otherwise, it’s not connected.
            let status = if peer_state.status.is_banned() {
                peer_state.status
            } else {
                let is_blacklisted = peer_state
                    .peer_info
                    .addr
                    .as_ref()
                    .map_or(false, |addr| blacklist.contains(addr));
                if is_blacklisted {
                    info!(target: "network", "Banning {:?} because address is blacklisted",
                          peer_state.peer_info);
                    KnownPeerStatus::Banned(ReasonForBan::Blacklisted, now)
                } else {
                    KnownPeerStatus::NotConnected
                }
            };

            let peer_state = KnownPeerState {
                peer_info: peer_state.peer_info,
                first_seen: peer_state.first_seen,
                last_seen,
                status,
            };

            match peerid_2_state.entry(peer_id) {
                // Peer is a boot node
                Entry::Occupied(mut current_peer_state) => {
                    if peer_state.status.is_banned() {
                        // If it says in database, that peer should be banned, ban the peer.
                        current_peer_state.get_mut().status = peer_state.status;
                    }
                }
                // Peer is not a boot node
                Entry::Vacant(entry) => {
                    if let Some(peer_addr) = peer_state.peer_info.addr {
                        if let Entry::Vacant(entry2) = addr_2_peer.entry(peer_addr) {
                            // Default case, add new entry.
                            entry2.insert(VerifiedPeer::new(peer_state.peer_info.id.clone()));
                            entry.insert(peer_state);
                        }
                        // else: There already exists a peer with a same addr, that's a boot node.
                        // Note: We don't load this entry into the memory, but it still stays on disk.
                    }
                }
            }
        }
        Ok(PeerStore { store, peer_states: peerid_2_state, addr_peers: addr_2_peer, blacklist })
    }

    pub fn is_blacklisted(&self, addr: &SocketAddr) -> bool {
        self.blacklist.contains(addr)
    }

    pub(crate) fn len(&self) -> usize {
        self.peer_states.len()
    }

    pub(crate) fn is_banned(&self, peer_id: &PeerId) -> bool {
        self.peer_states
            .get(peer_id)
            .map_or(false, |known_peer_state| known_peer_state.status.is_banned())
    }

    pub(crate) fn peer_connected(
        &mut self,
        peer_info: &PeerInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.add_signed_peer(peer_info.clone())?;
        let entry = self.peer_states.get_mut(&peer_info.id).unwrap();
        entry.last_seen = to_timestamp(Utc::now());
        entry.status = KnownPeerStatus::Connected;
        Self::save_to_db(&self.store, peer_info.id.try_to_vec()?.as_slice(), entry)
    }

    pub(crate) fn peer_disconnected(
        &mut self,
        peer_id: &PeerId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.last_seen = to_timestamp(Utc::now());
            peer_state.status = KnownPeerStatus::NotConnected;
            Self::save_to_db(&self.store, peer_id.try_to_vec()?.as_slice(), peer_state)
        } else {
            Err(format!("Peer {} is missing in the peer store", peer_id).into())
        }
    }

    pub(crate) fn peer_ban(
        &mut self,
        peer_id: &PeerId,
        ban_reason: ReasonForBan,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.last_seen = to_timestamp(Utc::now());
            peer_state.status = KnownPeerStatus::Banned(ban_reason, to_timestamp(Utc::now()));
            Self::save_to_db(&self.store, peer_id.try_to_vec()?.as_slice(), peer_state)
        } else {
            Err(format!("Peer {} is missing in the peer store", peer_id).into())
        }
    }

    fn save_to_db(
        store: &Store,
        peer_id: &[u8],
        peer_state: &KnownPeerState,
    ) -> Result<(), Box<dyn Error>> {
        let mut store_update = store.store_update();
        store_update.set_ser(DBCol::Peers, peer_id, peer_state)?;
        store_update.commit().map_err(|err| err.into())
    }

    fn delete_from_db(
        store: &Store,
        peer_ids: &[PeerId],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut store_update = store.store_update();
        for peer_id in peer_ids {
            store_update.delete(DBCol::Peers, &peer_id.try_to_vec()?);
        }
        store_update.commit().map_err(|err| err.into())
    }

    pub(crate) fn peer_unban(
        &mut self,
        peer_id: &PeerId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.status = KnownPeerStatus::NotConnected;
            Self::save_to_db(&self.store, peer_id.try_to_vec()?.as_slice(), peer_state)
        } else {
            Err(format!("Peer {} is missing in the peer store", peer_id).into())
        }
    }

    /// Find a random subset of peers based on filter.
    fn find_peers<F>(&self, filter: F, count: usize) -> Vec<PeerInfo>
    where
        F: FnMut(&&KnownPeerState) -> bool,
    {
        (self.peer_states.values())
            .filter(filter)
            .choose_multiple(&mut thread_rng(), count)
            .into_iter()
            .map(|kps| kps.peer_info.clone())
            .collect()
    }

    /// Return unconnected or peers with unknown status that we can try to connect to.
    /// Peers with unknown addresses are filtered out.
    pub(crate) fn unconnected_peer(
        &self,
        ignore_fn: impl Fn(&KnownPeerState) -> bool,
    ) -> Option<PeerInfo> {
        self.find_peers(
            |p| {
                (p.status == KnownPeerStatus::NotConnected || p.status == KnownPeerStatus::Unknown)
                    && !ignore_fn(p)
                    && p.peer_info.addr.is_some()
            },
            1,
        )
        .get(0)
        .cloned()
    }

    /// Return healthy known peers up to given amount.
    pub(crate) fn healthy_peers(&self, max_count: usize) -> Vec<PeerInfo> {
        self.find_peers(|p| matches!(p.status, KnownPeerStatus::Banned(_, _)).not(), max_count)
    }

    /// Return iterator over all known peers.
    pub(crate) fn iter(&self) -> Iter<'_, PeerId, KnownPeerState> {
        self.peer_states.iter()
    }

    /// Removes peers that are not responding for expiration period.
    pub(crate) fn remove_expired(
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
        for peer_id in &to_remove {
            self.peer_states.remove(peer_id);
        }
        Self::delete_from_db(&self.store, &to_remove)
    }

    fn touch(&self, peer_id: &PeerId) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get(peer_id) {
            Self::save_to_db(&self.store, peer_id.try_to_vec()?.as_slice(), peer_state)
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
            .insert(peer_addr, VerifiedPeer { peer_id: peer_info.id.clone(), trust_level });

        // Update peer_id addr
        self.peer_states
            .entry(peer_info.id.clone())
            .and_modify(|peer_state| peer_state.peer_info.addr = Some(peer_addr))
            .or_insert_with(|| KnownPeerState::new(peer_info.clone(), Clock::utc()));

        self.touch(&peer_info.id)?;
        if let Some(touch_other) = touch_other {
            self.touch(&touch_other)?;
        }
        Ok(())
    }

    /// Adds a peer into the store with given trust level.
    #[inline(always)]
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
                    let is_peer_trusted =
                        self.peer_states.get(&peer_info.id).map_or(false, |peer_state| {
                            peer_state.peer_info.addr.map_or(false, |current_addr| {
                                self.addr_peers.get(&current_addr).map_or(false, |verified_peer| {
                                    verified_peer.trust_level == TrustLevel::Signed
                                })
                            })
                        });
                    if is_peer_trusted {
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
                self.peer_states
                    .insert(peer_info.id.clone(), KnownPeerState::new(peer_info, Clock::utc()));
            }
        }
        Ok(())
    }

    /// Adds peers we’ve learned about from other peers.
    ///
    /// Identities of the nodes hasn’t been verified in any way.  We don’t even
    /// know if there is anything running at given addresses and even if there
    /// are nodes there we haven’t received signatures of their peer ID.
    ///
    /// See also [`Self::add_direct_peer`] and [`Self::add_signed_peer`].
    pub(crate) fn add_indirect_peers(
        &mut self,
        peers: impl Iterator<Item = PeerInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut total: usize = 0;
        let mut blacklisted: usize = 0;
        for peer_info in peers {
            total += 1;
            let is_blacklisted =
                peer_info.addr.as_ref().map_or(false, |addr| self.blacklist.contains(addr));
            if is_blacklisted {
                blacklisted += 1;
            } else {
                self.add_peer(peer_info, TrustLevel::Indirect)?;
            }
        }
        if blacklisted != 0 {
            info!(target: "network", "Ignored {} blacklisted peers out of {} indirect peer(s)",
                  blacklisted, total);
        }
        Ok(())
    }

    /// Adds a peer we’ve connected to but haven’t verified ID yet.
    ///
    /// We've connected to the host (thus know that the address is correct) and
    /// they claim they control given peer ID but we haven’t received signature
    /// confirming that identity yet.
    ///
    /// See also [`Self::add_indirect_peers`] and [`Self::add_signed_peer`].
    pub(crate) fn add_direct_peer(
        &mut self,
        peer_info: PeerInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.add_peer(peer_info, TrustLevel::Direct)
    }

    /// Adds a peer which proved to have secret key associated with the ID.
    ///
    /// The host have sent us a message signed with a secret key corresponding
    /// to the peer ID thus we can be sure that they control the secret key.
    ///
    /// See also [`Self::add_indirect_peers`] and [`Self::add_direct_peer`].
    pub(crate) fn add_signed_peer(
        &mut self,
        peer_info: PeerInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.add_peer(peer_info, TrustLevel::Signed)
    }
}

/// Public method used to iterate through all peers stored in the database.
pub fn iter_peers_from_store<F>(store: Store, f: F)
where
    F: Fn((&PeerId, &KnownPeerState)),
{
    let peer_store = PeerStore::new(store, &[], Default::default()).unwrap();
    peer_store.iter().for_each(f);
}

#[cfg(test)]
mod test {
    use near_crypto::{KeyType, SecretKey};
    use near_store::create_store;
    use near_store::test_utils::create_test_store;
    use std::collections::HashSet;
    use std::net::{Ipv4Addr, SocketAddrV4};

    use super::*;

    fn get_peer_id(seed: String) -> PeerId {
        PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed.as_str()).public_key())
    }

    fn get_addr(port: u16) -> SocketAddr {
        SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port).into()
    }

    fn get_peer_info(peer_id: PeerId, addr: Option<SocketAddr>) -> PeerInfo {
        PeerInfo { id: peer_id, addr, account_id: None }
    }

    fn gen_peer_info(port: u16) -> PeerInfo {
        PeerInfo {
            id: PeerId::new(SecretKey::from_random(KeyType::ED25519).public_key()),
            addr: Some(get_addr(port)),
            account_id: None,
        }
    }

    #[test]
    fn ban_store() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_store_ban").tempdir().unwrap();
        let peer_info_a = gen_peer_info(0);
        let peer_info_to_ban = gen_peer_info(1);
        let boot_nodes = vec![peer_info_a, peer_info_to_ban.clone()];
        {
            let store = create_store(tmp_dir.path());
            let mut peer_store = PeerStore::new(store, &boot_nodes, Default::default()).unwrap();
            assert_eq!(peer_store.healthy_peers(3).len(), 2);
            peer_store.peer_ban(&peer_info_to_ban.id, ReasonForBan::Abusive).unwrap();
            assert_eq!(peer_store.healthy_peers(3).len(), 1);
        }
        {
            let store_new = create_store(tmp_dir.path());
            let peer_store_new =
                PeerStore::new(store_new, &boot_nodes, Default::default()).unwrap();
            assert_eq!(peer_store_new.healthy_peers(3).len(), 1);
        }
    }

    #[test]
    fn test_unconnected_peer() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_store_ban").tempdir().unwrap();
        let peer_info_a = gen_peer_info(0);
        let peer_info_to_ban = gen_peer_info(1);
        let boot_nodes = vec![peer_info_a, peer_info_to_ban];
        {
            let store = create_store(tmp_dir.path());
            let peer_store = PeerStore::new(store, &boot_nodes, Default::default()).unwrap();
            assert!(peer_store.unconnected_peer(|_| false).is_some());
            assert!(peer_store.unconnected_peer(|_| true).is_none());
        }
    }

    fn check_exist(
        peer_store: &PeerStore,
        peer_id: &PeerId,
        addr_level: Option<(SocketAddr, TrustLevel)>,
    ) -> bool {
        if let Some(peer_info) = peer_store.peer_states.get(peer_id) {
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
        let mut peer_store = PeerStore::new(store, &[], Default::default()).unwrap();

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
        let mut peer_store = PeerStore::new(store, &[], Default::default()).unwrap();

        let peers_id = (0..1).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
        let addrs = (0..2).map(get_addr).collect::<Vec<_>>();

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
        let mut peer_store = PeerStore::new(store.clone(), &[], Default::default()).unwrap();

        // Five peers: A, B, C, D, X, T
        let peers_id = (0..6).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
        // Five addresses: #A, #B, #C, #D, #X, #T
        let addrs = (0..6).map(get_addr).collect::<Vec<_>>();

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
        let peer_store_2 = PeerStore::new(store, &[], Default::default()).unwrap();
        assert!(check_exist(&peer_store_2, &peers_id[0], Some((addrs[0], TrustLevel::Indirect))));
        assert!(check_integrity(&peer_store_2));
    }

    #[test]
    fn check_ignore_blacklisted_peers() {
        fn assert_peers(peer_store: &PeerStore, expected: &[&PeerId], banned: &[&PeerId]) {
            let expected: HashSet<&PeerId> = HashSet::from_iter(expected.iter().cloned());
            let got = HashSet::from_iter(peer_store.peer_states.keys());
            assert_eq!(expected, got);

            let expected: HashSet<&PeerId> = HashSet::from_iter(banned.iter().cloned());
            let got =
                HashSet::from_iter(peer_store.peer_states.iter().filter_map(|(key, value)| {
                    if value.status.is_banned() {
                        Some(key)
                    } else {
                        None
                    }
                }));
            assert_eq!(expected, got);
        }

        let ids = (0..6).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
        let store = create_test_store();

        // Populate store with three peers.
        {
            let mut peer_store = PeerStore::new(store.clone(), &[], Default::default()).unwrap();
            peer_store
                .add_indirect_peers(
                    [
                        get_peer_info(ids[0].clone(), None),
                        get_peer_info(ids[1].clone(), Some(get_addr(1))),
                        get_peer_info(ids[2].clone(), Some(get_addr(2))),
                    ]
                    .into_iter(),
                )
                .unwrap();
            assert_peers(&peer_store, &[&ids[0], &ids[1], &ids[2]], &[]);
        }

        // Peers without address aren’t saved but make sure the rest are read
        // correctly.
        {
            let peer_store = PeerStore::new(store.clone(), &[], Default::default()).unwrap();
            assert_peers(&peer_store, &[&ids[1], &ids[2]], &[]);
        }

        // Blacklist one of the existing peers and one new peer.
        {
            let blacklist = Blacklist::from_iter(
                ["127.0.0.1:2".to_string(), "127.0.0.1:5".to_string()].into_iter(),
            );
            let mut peer_store = PeerStore::new(store, &[], blacklist).unwrap();
            // Peer 127.0.0.1:2 is there but is banned.
            assert_peers(&peer_store, &[&ids[1], &ids[2]], &[&ids[2]]);

            peer_store
                .add_indirect_peers(
                    [
                        get_peer_info(ids[3].clone(), None),
                        get_peer_info(ids[4].clone(), Some(get_addr(4))),
                        get_peer_info(ids[5].clone(), Some(get_addr(5))),
                    ]
                    .into_iter(),
                )
                .unwrap();
            // Peer 127.0.0.1:5 is ignored and never added.
            assert_peers(&peer_store, &[&ids[1], &ids[2], &ids[3], &ids[4]], &[&ids[2]]);
        }
    }
}
