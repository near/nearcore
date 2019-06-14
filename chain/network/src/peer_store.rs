use std::collections::{hash_map::Iter, HashMap};
use std::convert::TryInto;
use std::sync::Arc;

use chrono::Utc;
use log::debug;
use rand::seq::SliceRandom;
use rand::thread_rng;

use near_store::{Store, COL_PEERS};

use crate::types::{
    FullPeerInfo, KnownPeerState, KnownPeerStatus, NetworkConfig, PeerId, PeerInfo, ReasonForBan,
};

/// Known peers store, maintaining cache of known peers and connection to storage to save/load them.
pub struct PeerStore {
    store: Arc<Store>,
    peer_states: HashMap<PeerId, KnownPeerState>,
}

impl PeerStore {
    pub fn new(
        store: Arc<Store>,
        boot_nodes: &[PeerInfo],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut peer_states = HashMap::default();
        for (key, value) in store.iter(COL_PEERS) {
            let key: Vec<u8> = key.into();
            let value: Vec<u8> = value.into();
            let peer_id: PeerId = key.try_into()?;
            let mut peer_state: KnownPeerState = value.try_into()?;
            peer_state.status = KnownPeerStatus::NotConnected;
            peer_states.insert(peer_id, peer_state);
        }
        for peer_info in boot_nodes.iter() {
            if !peer_states.contains_key(&peer_info.id) {
                peer_states.insert(peer_info.id, KnownPeerState::new(peer_info.clone()));
            }
        }
        Ok(PeerStore { store, peer_states })
    }

    pub fn len(&self) -> usize {
        self.peer_states.len()
    }

    pub fn peer_connected(
        &mut self,
        peer_info: &FullPeerInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let entry = self
            .peer_states
            .entry(peer_info.peer_info.id)
            .or_insert(KnownPeerState::new(peer_info.peer_info.clone()));
        entry.last_seen = Utc::now();
        entry.status = KnownPeerStatus::Connected;
        let mut store_update = self.store.store_update();
        store_update.set_ser(COL_PEERS, peer_info.peer_info.id.as_ref(), entry)?;
        store_update.commit().map_err(|err| err.into())
    }

    pub fn peer_disconnected(
        &mut self,
        peer_id: &PeerId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.last_seen = Utc::now();
            peer_state.status = KnownPeerStatus::NotConnected;
            let mut store_update = self.store.store_update();
            store_update.set_ser(COL_PEERS, peer_id.as_ref(), peer_state)?;
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
            peer_state.last_seen = Utc::now();
            peer_state.status = KnownPeerStatus::Banned(ban_reason, Utc::now());
            let mut store_update = self.store.store_update();
            store_update.set_ser(COL_PEERS, peer_id.as_ref(), peer_state)?;
            store_update.commit().map_err(|err| err.into())
        } else {
            Err(format!("Peer {} is missing in the peer store", peer_id).into())
        }
    }

    pub fn peer_unban(&mut self, peer_id: &PeerId) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(peer_state) = self.peer_states.get_mut(peer_id) {
            peer_state.status = KnownPeerStatus::NotConnected;
            let mut store_update = self.store.store_update();
            store_update.set_ser(COL_PEERS, peer_id.as_ref(), peer_state)?;
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
    pub fn unconnected_peers(&self) -> Vec<PeerInfo> {
        self.find_peers(
            |p| p.status == KnownPeerStatus::NotConnected || p.status == KnownPeerStatus::Unknown,
            0,
        )
    }

    /// Return healthy known peers up to given amount.
    pub fn healthy_peers(&self, max_count: u32) -> Vec<PeerInfo> {
        // TODO: better healthy peer definition here.
        self.find_peers(
            |p| match p.status {
                KnownPeerStatus::Banned(_, _) => false,
                _ => true,
            },
            max_count,
        )
    }

    /// Return iterator over all known peers.
    pub fn iter(&self) -> Iter<PeerId, KnownPeerState> {
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
            let diff = (now - peer_status.last_seen).to_std()?;
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
            store_update.delete(COL_PEERS, peer_id.as_ref());
        }
        store_update.commit().map_err(|err| err.into())
    }

    pub fn add_peers(&mut self, mut peers: Vec<PeerInfo>) {
        for peer_info in peers.drain(..) {
            if !self.peer_states.contains_key(&peer_info.id) {
                self.peer_states.insert(peer_info.id, KnownPeerState::new(peer_info));
            }
        }
    }
}
