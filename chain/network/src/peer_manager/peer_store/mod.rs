use crate::blacklist;
use crate::network_protocol::PeerInfo;
use crate::types::{KnownPeerState, KnownPeerStatus, ReasonForBan};
use anyhow::bail;
use im::hashmap::Entry;
use im::{HashMap, HashSet};
use lru::LruCache;
use near_async::time;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::net::SocketAddr;
use std::ops::Not;

#[cfg(test)]
mod testonly;
#[cfg(test)]
mod tests;

/// The PeerStore is an in-memory cache of known peer states. It is used to:
///     - Store information about known peers in the network. Peers may be discovered
///       by connecting to them directly or by learning about them from other peers.
///     - Respond to requests from other peers for known peers (see PeerStore::healthy_peers).
///     - Select peers to which we may try to connect directly (see PeerStore::unconnected_peer).
///
/// Contents of the PeerStore are not persisted to the database. Upon starting a node,
/// the PeerStore is initialized from the boot nodes in its config.

/// Level of trust we have about a new (PeerId, Addr) pair.
#[derive(Eq, PartialEq, Debug, Clone, Copy)]
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
    fn signed(peer_id: PeerId) -> Self {
        Self { peer_id, trust_level: TrustLevel::Signed }
    }
}

#[derive(Clone)]
pub struct Config {
    /// A list of nodes to connect to on the first run of the neard server.
    /// Once it connects to some of them, the server will learn about other
    /// nodes in the network and will try to connect to them as well.
    ///
    /// The recommended boot nodes are distributed together with the config.json
    /// file, but you can modify the boot_nodes field to contain any nodes that
    /// you trust.
    pub boot_nodes: Vec<PeerInfo>,
    /// Nodes will not accept or try to establish connection to such peers.
    pub blacklist: blacklist::Blacklist,
    /// If true - connect only to the bootnodes.
    pub connect_only_to_boot_nodes: bool,
    /// The maximum number of peers to store. If capacity is exceeded, the peers
    /// with the earliest last_seen value will be evicted.
    pub peer_states_cache_size: u32,
    /// Remove expired peers.
    pub peer_expiration_duration: time::Duration,
    /// Duration of the ban for misbehaving peers.
    pub ban_window: time::Duration,
}

/// Known peers store, maintaining cache of known peers
struct Inner {
    config: Config,
    boot_nodes: HashSet<PeerId>,
    // LruCache of the known peer states. Be sure to use peek/peek_mut to access information.
    // Using the get/put methods modifies the cache order.
    peer_states: LruCache<PeerId, KnownPeerState>,
    // This is a reverse index, from physical address to peer_id
    // It can happens that some peers don't have known address, so
    // they will not be present in this list, otherwise they will be present.
    addr_peers: HashMap<SocketAddr, VerifiedPeer>,
}

impl Inner {
    /// Adds a peer which proved to have secret key associated with the ID.
    ///
    /// The host have sent us a message signed with a secret key corresponding
    /// to the peer ID thus we can be sure that they control the secret key.
    ///
    /// See also [`Self::add_indirect_peers`] and [`Self::add_direct_peer`].
    fn add_signed_peer(&mut self, clock: &time::Clock, peer_info: PeerInfo) {
        self.add_peer(clock, peer_info, TrustLevel::Signed)
    }

    /// Adds a peer into the store with given trust level.
    fn add_peer(&mut self, clock: &time::Clock, peer_info: PeerInfo, trust_level: TrustLevel) {
        if let Some(peer_addr) = peer_info.addr {
            match trust_level {
                TrustLevel::Signed => {
                    self.update_peer_info(clock, peer_info, peer_addr, TrustLevel::Signed);
                }
                TrustLevel::Direct => {
                    // If this peer already exists with a signed connection ignore this update.
                    // Warning: This is a problem for nodes that changes its address without changing peer_id.
                    //          It is recommended to change peer_id if address is changed.
                    let trust_level = (|| {
                        let state = self.peer_states.get(&peer_info.id)?;
                        let addr = state.peer_info.addr?;
                        let verified_peer = self.addr_peers.get(&addr)?;
                        Some(verified_peer.trust_level)
                    })();
                    if trust_level == Some(TrustLevel::Signed) {
                        return;
                    }
                    self.update_peer_info(clock, peer_info, peer_addr, TrustLevel::Direct);
                }
                TrustLevel::Indirect => {
                    // We should only update an Indirect connection if we don't know anything about the peer
                    // or about the address.
                    if !self.peer_states.contains(&peer_info.id)
                        && !self.addr_peers.contains_key(&peer_addr)
                    {
                        self.update_peer_info(clock, peer_info, peer_addr, TrustLevel::Indirect);
                    }
                }
            }
        } else {
            // If doesn't have the address attached it is not verified and we add it
            // only if it is unknown to us.
            if !self.peer_states.contains(&peer_info.id) {
                if let Some((_, popped_peer_state)) = self
                    .peer_states
                    .push(peer_info.id.clone(), KnownPeerState::new(peer_info, clock.now_utc()))
                {
                    // If a peer was evicted from peer_states due to the bounded cache size
                    // and it has an address, remove the corresponding entry from addr_peers
                    if let Some(popped_peer_addr) = popped_peer_state.peer_info.addr {
                        self.addr_peers.remove(&popped_peer_addr);
                    }
                }
            }
        }
    }

    fn peer_unban(&mut self, peer_id: &PeerId) -> anyhow::Result<()> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.status = KnownPeerStatus::NotConnected;
        } else {
            bail!("Peer {} is missing in the peer store", peer_id);
        }
        Ok(())
    }

    /// Deletes peers from the internal cache
    fn delete_peers(&mut self, peer_ids: &[PeerId]) {
        for peer_id in peer_ids {
            if let Some(peer_state) = self.peer_states.pop(peer_id) {
                if let Some(addr) = peer_state.peer_info.addr {
                    self.addr_peers.remove(&addr);
                }
            }
        }
    }

    /// Find a random subset of peers based on filter.
    fn find_peers<F>(&self, filter: F, count: usize) -> Vec<PeerInfo>
    where
        F: FnMut(&&KnownPeerState) -> bool,
    {
        (self.peer_states.iter().map(|(_, v)| v))
            .filter(filter)
            .choose_multiple(&mut thread_rng(), count)
            .into_iter()
            .map(|kps| kps.peer_info.clone())
            .collect()
    }

    /// Create new pair between peer_info.id and peer_addr removing
    /// old pairs if necessary.
    fn update_peer_info(
        &mut self,
        clock: &time::Clock,
        peer_info: PeerInfo,
        peer_addr: SocketAddr,
        trust_level: TrustLevel,
    ) {
        // If there is a peer associated with current address remove the address from it.
        if let Some(verified_peer) = self.addr_peers.remove(&peer_addr) {
            self.peer_states.peek_mut(&verified_peer.peer_id).unwrap().peer_info.addr = None;
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

        // Update or insert peer_id addr
        if let Some(peer_state) = self.peer_states.peek_mut(&peer_info.id) {
            peer_state.peer_info.addr = Some(peer_addr);
        } else {
            let now = clock.now_utc();
            if let Some((_, popped_peer_state)) = self
                .peer_states
                .push(peer_info.id.clone(), KnownPeerState::new(peer_info.clone(), now))
            {
                // If a peer was evicted from peer_states due to the bounded cache size
                // and it has an address, remove the corresponding entry from addr_peers
                if let Some(popped_peer_addr) = popped_peer_state.peer_info.addr {
                    self.addr_peers.remove(&popped_peer_addr);
                }
            }
        }
    }

    /// Removes peers that are not responding for expiration period.
    fn remove_expired(&mut self, now: time::Utc) {
        let mut to_remove = vec![];
        for (peer_id, peer_status) in self.peer_states.iter() {
            if peer_status.status != KnownPeerStatus::Connected
                && now > peer_status.last_seen + self.config.peer_expiration_duration
            {
                tracing::debug!(target: "network", "Removing peer: last seen {:?} ago", now-peer_status.last_seen);
                to_remove.push(peer_id.clone());
            }
        }
        self.delete_peers(&to_remove);
    }

    fn unban(&mut self, now: time::Utc) {
        let mut to_unban = vec![];
        for (peer_id, peer_state) in &self.peer_states {
            if let KnownPeerStatus::Banned(_, ban_time) = peer_state.status {
                if now < ban_time + self.config.ban_window {
                    continue;
                }
                tracing::info!(target: "network", unbanned = ?peer_id, ?ban_time, "unbanning a peer");
                to_unban.push(peer_id.clone());
            }
        }
        for peer_id in &to_unban {
            if let Err(err) = self.peer_unban(&peer_id) {
                tracing::error!(target: "network", ?peer_id, ?err, "Failed to unban a peer");
            }
        }
    }

    /// Update the 'last_seen' time for all the peers that we're currently connected to.
    fn update_last_seen(&mut self, now: time::Utc) {
        let mut connected_peer_ids = vec![];
        for (peer_id, peer_state) in self.peer_states.iter_mut() {
            if peer_state.status == KnownPeerStatus::Connected {
                connected_peer_ids.push(peer_id.clone());
                peer_state.last_seen = now;
            }
        }

        // Access the connected peers to move them to the head of the LruCache
        for peer_id in connected_peer_ids {
            self.peer_states.get(&peer_id);
        }
    }

    /// Cleans up the state of the PeerStore, due to passing time.
    /// * it unbans a peer if config.ban_window has passed
    /// * it updates KnownPeerStatus.last_seen of the connected peers
    /// * it removes peers which were not seen for config.peer_expiration_duration
    /// This function should be called periodically.
    pub fn update(&mut self, clock: &time::Clock) {
        let now = clock.now_utc();
        self.unban(now);
        self.update_last_seen(now);
        self.remove_expired(now);
    }
}

pub(crate) struct PeerStore(Mutex<Inner>);

impl PeerStore {
    pub fn new(clock: &time::Clock, config: Config) -> anyhow::Result<Self> {
        let boot_nodes: HashSet<_> = config.boot_nodes.iter().map(|p| p.id.clone()).collect();
        // A mapping from `PeerId` to `KnownPeerState`.
        let mut peerid_2_state = LruCache::new(config.peer_states_cache_size as usize);
        // Stores mapping from `SocketAddr` to `VerifiedPeer`, which contains `PeerId`.
        // Only one peer can exist with given `PeerId` or `SocketAddr`.
        // In case of collision, we will choose the first one.
        let mut addr_2_peer = HashMap::default();

        if boot_nodes.len() > (config.peer_states_cache_size as usize) {
            tracing::error!(
                "Num boot nodes is {} but the size of the peer store is limited to {}.",
                boot_nodes.len(),
                config.peer_states_cache_size
            );
        }

        let now = clock.now_utc();
        for peer_info in &config.boot_nodes {
            if peerid_2_state.contains(&peer_info.id) {
                tracing::error!(id = ?peer_info.id, "There is a duplicated peer in boot_nodes");
                continue;
            }
            let peer_addr = match peer_info.addr {
                None => continue,
                Some(addr) => addr,
            };
            let entry = match addr_2_peer.entry(peer_addr) {
                Entry::Occupied(entry) => {
                    // There is already a different peer_id with this address.
                    anyhow::bail!("Two boot nodes have the same address {:?}", entry.key());
                }
                Entry::Vacant(entry) => entry,
            };
            entry.insert(VerifiedPeer::signed(peer_info.id.clone()));

            if let Some((_, popped_peer_state)) = peerid_2_state
                .push(peer_info.id.clone(), KnownPeerState::new(peer_info.clone(), now))
            {
                // If a peer was evicted from peer_states due to the bounded cache size
                // and it has an address, remove the corresponding entry from addr_peers
                if let Some(popped_peer_addr) = popped_peer_state.peer_info.addr {
                    addr_2_peer.remove(&popped_peer_addr);
                }
            }
        }

        let inner =
            Inner { config, boot_nodes, peer_states: peerid_2_state, addr_peers: addr_2_peer };
        Ok(PeerStore(Mutex::new(inner)))
    }

    pub fn is_blacklisted(&self, addr: &SocketAddr) -> bool {
        self.0.lock().config.blacklist.contains(*addr)
    }

    pub fn len(&self) -> usize {
        self.0.lock().peer_states.len()
    }

    pub fn is_banned(&self, peer_id: &PeerId) -> bool {
        self.0.lock().peer_states.get(peer_id).map_or(false, |s| s.status.is_banned())
    }

    pub fn count_banned(&self) -> usize {
        self.0.lock().peer_states.iter().filter(|(_, st)| st.status.is_banned()).count()
    }

    pub fn update(&self, clock: &time::Clock) {
        self.0.lock().update(clock)
    }

    #[allow(dead_code)]
    /// Returns the state of the current peer in memory.
    pub fn get_peer_state(&self, peer_id: &PeerId) -> Option<KnownPeerState> {
        self.0.lock().peer_states.get(peer_id).cloned()
    }

    pub fn peer_connected(&self, clock: &time::Clock, peer_info: &PeerInfo) {
        let mut inner = self.0.lock();
        inner.add_signed_peer(clock, peer_info.clone());
        let entry = inner.peer_states.get_mut(&peer_info.id).unwrap();
        entry.last_seen = clock.now_utc();
        entry.status = KnownPeerStatus::Connected;
    }

    pub fn peer_disconnected(&self, clock: &time::Clock, peer_id: &PeerId) -> anyhow::Result<()> {
        let mut inner = self.0.lock();
        if let Some(peer_state) = inner.peer_states.get_mut(peer_id) {
            peer_state.last_seen = clock.now_utc();
            peer_state.status = KnownPeerStatus::NotConnected;
        } else {
            bail!("Peer {} is missing in the peer store", peer_id);
        }
        Ok(())
    }

    /// Records the last attempt to connect to peer.
    pub fn peer_connection_attempt(
        &self,
        clock: &time::Clock,
        peer_id: &PeerId,
        result: Result<(), anyhow::Error>,
    ) -> anyhow::Result<()> {
        let mut inner = self.0.lock();

        if let Some(peer_state) = inner.peer_states.get_mut(peer_id) {
            if result.is_err() {
                // Marks the peer status as Unknown (as we failed to connect to it).
                peer_state.status = KnownPeerStatus::Unknown;
            }
            peer_state.last_outbound_attempt =
                Some((clock.now_utc(), result.map_err(|err| err.to_string())));
            peer_state.last_seen = clock.now_utc();
        } else {
            bail!("Peer {} is missing in the peer store", peer_id);
        }

        Ok(())
    }

    pub fn peer_ban(
        &self,
        clock: &time::Clock,
        peer_id: &PeerId,
        ban_reason: ReasonForBan,
    ) -> anyhow::Result<()> {
        tracing::warn!(target: "network", "Banning peer {} for {:?}", peer_id, ban_reason);
        let mut inner = self.0.lock();
        if let Some(peer_state) = inner.peer_states.get_mut(peer_id) {
            let now = clock.now_utc();
            peer_state.last_seen = now;
            peer_state.status = KnownPeerStatus::Banned(ban_reason, now);
        } else {
            bail!("Peer {} is missing in the peer store", peer_id);
        }
        Ok(())
    }

    /// Return unconnected or peers with unknown status that we can try to connect to.
    /// Peers with unknown addresses are filtered out.
    pub fn unconnected_peer(
        &self,
        ignore_fn: impl Fn(&KnownPeerState) -> bool,
        prefer_previously_connected_peer: bool,
    ) -> Option<PeerInfo> {
        let inner = self.0.lock();
        if prefer_previously_connected_peer {
            let preferred_peer = inner.find_peers(
                |p| {
                    (p.status == KnownPeerStatus::NotConnected)
                        && !ignore_fn(p)
                        && p.peer_info.addr.is_some()
                        // if we're connecting only to the boot nodes - filter out the nodes that are not bootnodes.
                        && (!inner.config.connect_only_to_boot_nodes || inner.boot_nodes.contains(&p.peer_info.id))
                },
                1,
            )
            .get(0)
            .cloned();
            // If we found a preferred peer - return it.
            if preferred_peer.is_some() {
                return preferred_peer;
            };
            // otherwise, pick a peer from the wider pool below.
        }
        inner.find_peers(
            |p| {
                (p.status == KnownPeerStatus::NotConnected || p.status == KnownPeerStatus::Unknown)
                    && !ignore_fn(p)
                    && p.peer_info.addr.is_some()
                    // If we're connecting only to the boot nodes - filter out the nodes that are not boot nodes.
                    && (!inner.config.connect_only_to_boot_nodes || inner.boot_nodes.contains(&p.peer_info.id))
            },
            1,
        )
        .get(0)
        .cloned()
    }

    /// Return healthy known peers up to given amount.
    pub fn healthy_peers(&self, max_count: usize) -> Vec<PeerInfo> {
        self.0
            .lock()
            .find_peers(|p| matches!(p.status, KnownPeerStatus::Banned(_, _)).not(), max_count)
    }

    /// Adds peers we’ve learned about from other peers.
    ///
    /// Identities of the nodes hasn’t been verified in any way.  We don’t even
    /// know if there is anything running at given addresses and even if there
    /// are nodes there we haven’t received signatures of their peer ID.
    ///
    /// See also [`Self::add_direct_peer`] and [`Self::add_signed_peer`].
    pub fn add_indirect_peers(&self, clock: &time::Clock, peers: impl Iterator<Item = PeerInfo>) {
        let mut inner = self.0.lock();
        let mut total: usize = 0;
        let mut blacklisted: usize = 0;
        for peer_info in peers {
            total += 1;
            let is_blacklisted =
                peer_info.addr.map_or(false, |addr| inner.config.blacklist.contains(addr));
            if is_blacklisted {
                blacklisted += 1;
            } else {
                inner.add_peer(clock, peer_info, TrustLevel::Indirect);
            }
        }
        if blacklisted != 0 {
            tracing::info!(target: "network", "Ignored {} blacklisted peers out of {} indirect peer(s)",
                  blacklisted, total);
        }
    }

    /// Adds a peer we’ve connected to but haven’t verified ID yet.
    ///
    /// We've connected to the host (thus know that the address is correct) and
    /// they claim they control given peer ID but we haven’t received signature
    /// confirming that identity yet.
    ///
    /// See also [`Self::add_indirect_peers`] and [`Self::add_signed_peer`].
    pub fn add_direct_peer(&self, clock: &time::Clock, peer_info: PeerInfo) {
        self.0.lock().add_peer(clock, peer_info, TrustLevel::Direct)
    }

    pub fn load(&self) -> HashMap<PeerId, KnownPeerState> {
        self.0.lock().peer_states.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}
