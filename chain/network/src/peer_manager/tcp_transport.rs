use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::network_transport::NetworkTransport;
use crate::tcp;
use crate::types::{PeerMessage, ReasonForBan};
use near_primitives::network::PeerId;
use std::sync::Arc;

/// Production implementation of NetworkTransport.
/// Transitional: wraps NetworkState's Pools. In PR 9, Pools move to
/// TcpTransport directly.
pub(crate) struct TcpTransport {
    pub state: Arc<NetworkState>,
}

impl TcpTransport {
    pub fn new(state: Arc<NetworkState>) -> Arc<Self> {
        Arc::new(Self { state })
    }
}

impl NetworkTransport for TcpTransport {
    fn send_message(&self, tier: tcp::Tier, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        match tier {
            tcp::Tier::T1 => self.state.tier1.send_message(peer_id, msg),
            tcp::Tier::T2 => self.state.tier2.send_message(peer_id, msg),
            tcp::Tier::T3 => self.state.tier3.send_message(peer_id, msg),
        }
    }

    fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        self.state.tier2.broadcast_message(msg);
    }

    fn disconnect_peer(&self, peer_id: &PeerId, ban_reason: Option<ReasonForBan>) {
        // A peer may be connected on multiple tiers simultaneously
        // (e.g. T1 + T2 for a validator). Stop the connection on every
        // tier we find it on — partial disconnect would leave a stale
        // entry on the other tier.
        for pool in [&self.state.tier1, &self.state.tier2, &self.state.tier3] {
            let snapshot = pool.load();
            if let Some(conn) = snapshot.ready.get(peer_id) {
                conn.stop(ban_reason);
            }
        }
    }
}
