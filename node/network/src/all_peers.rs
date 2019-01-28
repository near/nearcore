//! Container for managing information about all peers.

use crate::peer::PeerInfo;
use crate::peer::PeerMessage;
use futures::future::Future;
use futures::sink::Sink;
use futures::sync::mpsc::Sender;
use log::{error, warn};
use primitives::types::PeerId;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use rand::{thread_rng, seq::IteratorRandom};

/// Information on all peers, including this one. The relationship between these collections:
/// `peers_info` \supseteq `locked_peers` \supseteq `ready_peers`
/// Each container has a different usage:
/// * `ready_peers`. Messages can be only sent and received with `ready_peers`. Because until the
///   handshake is complete we do not trust the peer and they do not trust us. This also facilitates
///   the peer info gossip, because it is included in the handshake;
/// * `locked_peers`. Peers that we have started connecting to but are not ready yet. We want to know
///   them because we do not want to concurrently re-connect to them while another connection is
///   pending;
/// * `peers_info`. All peers that we know some information of. This is the information that we
///   gossip. Also when we decide whom we should connect we iterate over `peers_info`\`locked_peers`.
pub struct AllPeers {
    /// Information on all peers, even those that we have not connected yet.
    peers_info: HashMap<SocketAddr, PeerInfo>,
    /// List of peer addresses to make sure we do not initialize or drop them concurrently.
    locked_peers: HashSet<SocketAddr>,
    /// List of channels in which we can put messages to be sent over the network.
    ready_peers: HashMap<PeerId, (PeerInfo, Sender<PeerMessage>)>,
    /// Channel that notifies there are new peers that we can try connecting to.
    check_new_peers_tx: Sender<()>,
}

impl AllPeers {
    pub fn new(boot_nodes: &Vec<SocketAddr>, check_new_peers_tx: Sender<()>) -> Self {
        let mut res = Self {
            peers_info: HashMap::new(),
            locked_peers: HashSet::new(),
            ready_peers: HashMap::new(),
            check_new_peers_tx: check_new_peers_tx.clone(),
        };
        res.peers_info.extend(boot_nodes.iter().map(|addr| {
            (addr.clone(), PeerInfo { id: None, addr: addr.clone(), account_id: None })
        }));

        // Immediately let know that there are new peers that we need connecting to.
        tokio::spawn(
            check_new_peers_tx
                .send(())
                .map(|_| ())
                .map_err(|_| warn!(target: "network", "Error notifying about boot nodes.")),
        );
        res
    }

    /// Take peer that was locked and make it ready.
    pub fn promote_to_ready(&mut self, info: &PeerInfo, out_msg_tx: &Sender<PeerMessage>) {
        let id = info.id.as_ref().expect("To make peer ready we need its id").clone();
        let addr = info.addr;
        if self.ready_peers.insert(id, (info.clone(), out_msg_tx.clone())).is_some() {
            error!(target: "network", "Trying to make peer ready that was already ready {}. ", addr);
        }
    }

    /// Drop the peer that was locked.
    pub fn drop_lock(&mut self, addr: &SocketAddr) {
        if self.peers_info.remove(addr).is_none() {
            error!(target: "network", "Removing {} from peers info, but it was not there.", addr);
        }
        if self.locked_peers.remove(addr) {
            error!(target: "network", "Removing {} from locked peers, but it was not there.", addr);
        }
    }

    /// Drop the peer that was ready.
    pub fn drop_ready(&mut self, info: &PeerInfo) {
        self.drop_lock(&info.addr);
        let id = info.id.as_ref().expect("To drop ready peer we need its id");
        if self.ready_peers.remove(id).is_none() {
            error!(target: "network", "Trying to remove ready peer {}, but it was not there. ", &info.addr);
        }
    }

    /// Merges info on all newly (re)discovered peers. Returns ref for chaining.
    pub fn merge_peers_info(&mut self, peers_info: Vec<PeerInfo>) -> &mut Self {
        let peers_to_add: Vec<_> = peers_info
            .iter()
            .filter_map(|info| {
                if self.peers_info.contains_key(&info.addr) {
                    None
                } else {
                    Some((info.addr.clone(), info.clone()))
                }
            })
            .collect();
        if !peers_to_add.is_empty() {
            tokio::spawn(self.check_new_peers_tx.clone().send(()).map(|_| ()).map_err(
                |_| warn!(target: "network", "Error while notifying about new peers.")));
            self.peers_info.extend(peers_to_add);
        }
        self
    }

    pub fn peers_info(&self) -> Vec<PeerInfo> {
        self.peers_info.values().cloned().collect()
    }

    /// Sample peers that are ready.
    pub fn sample_ready_peers(&self, sample_size: usize) -> Vec<Sender<PeerMessage>> {
        let mut rng = thread_rng();
        self.ready_peers.values()
            .choose_multiple(&mut rng, sample_size)
            .iter()
            .map(|(_, ch)| ch.clone()).collect()
    }

    /// Locks new peers and returns them.
    pub fn lock_new_peers(&mut self) -> Vec<PeerInfo> {
        let mut res = vec![];
        for (addr, info) in &self.peers_info {
            if !self.locked_peers.contains(addr) {
                res.push(info.clone());
                self.locked_peers.insert(addr.clone());
            }
        }
        res
    }

    /// Get channel of the peer. If peer is not ready returns `None`.
    pub fn get_ready_peer_by_id(&self, id: &PeerId) -> Option<Sender<PeerMessage>> {
        self.ready_peers.get(id).map(|(_, ch)| ch.clone())
    }

    /// Try locking incoming peer. If peer was already locked return `false`.
    pub fn add_incoming_peer(&mut self, info: &PeerInfo) -> bool {
        self.peers_info.insert(info.addr.clone(), info.clone());
        if self.locked_peers.contains(&info.addr) {
            false
        } else {
            self.locked_peers.insert(info.addr.clone());
            true
        }
    }
}
