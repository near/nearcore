use crate::network_protocol::Encoding;
use crate::peer::peer_actor::PeerActor;
use crate::types::{FullPeerInfo, PeerStatsResult};
use arc_swap::ArcSwap;
use near_network_primitives::time;
use near_network_primitives::types::PeerType;
use near_primitives::network::PeerId;
use near_rate_limiter::ThrottleController;
use std::sync::Arc;

/// Contains information relevant to a connected peer.
#[derive(Clone)]
pub(crate) struct ConnectedPeer {
    pub addr: actix::Addr<PeerActor>,
    pub full_peer_info: FullPeerInfo,
    /// Number of bytes we've received from the peer.
    pub received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    pub sent_bytes_per_sec: u64,
    /// Last time requested peers.
    pub last_time_peer_requested: time::Instant,
    /// Last time we received a message from this peer.
    pub last_time_received_message: time::Instant,
    /// Time where the connection was established.
    pub connection_established_time: time::Instant,
    /// Who started connection. Inbound (other) or Outbound (us).
    pub peer_type: PeerType,
    /// A helper data structure for limiting reading, reporting stats.
    pub throttle_controller: ThrottleController,
    /// Encoding used for communication.
    pub encoding: Option<Encoding>,
}

#[derive(Default)]
pub(crate) struct ConnectedPeers(ArcSwap<im::HashMap<PeerId, ConnectedPeer>>);

impl ConnectedPeers {
    pub fn read(&self) -> Arc<im::HashMap<PeerId, ConnectedPeer>> {
        self.0.load_full()
    }

    fn update(&self, mut f: impl FnMut(&mut im::HashMap<PeerId, ConnectedPeer>)) {
        self.0.rcu(|peers| {
            let mut peers: im::HashMap<PeerId, ConnectedPeer> = (**peers).clone();
            f(&mut peers);
            Arc::new(peers)
        });
    }

    pub fn insert(&self, peer: ConnectedPeer) {
        self.update(|peers| {
            peers.insert(peer.full_peer_info.peer_info.id.clone(), peer.clone());
        });
    }

    pub fn remove(&self, peer_id: &PeerId) {
        self.update(|peers| {
            peers.remove(peer_id);
        });
    }

    pub fn set_last_time_peer_requested(&self, peer_id: &PeerId, t: time::Instant) {
        self.update(|peers| {
            if let Some(p) = peers.get_mut(peer_id) {
                p.last_time_peer_requested = t;
            }
        });
    }

    pub fn set_last_time_received_message(&self, peer_id: &PeerId, t: time::Instant) {
        self.update(|peers| {
            if let Some(p) = peers.get_mut(peer_id) {
                p.last_time_received_message = t;
            }
        });
    }

    pub fn set_peer_stats(&self, peer_id: &PeerId, stats: PeerStatsResult) {
        self.update(|peers| {
            if let Some(p) = peers.get_mut(peer_id) {
                p.full_peer_info.chain_info = stats.chain_info.clone();
                p.sent_bytes_per_sec = stats.sent_bytes_per_sec;
                p.received_bytes_per_sec = stats.received_bytes_per_sec;
                p.encoding = stats.encoding;
            }
        });
    }
}
