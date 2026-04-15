use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::network_transport::{NetworkTransport, PeerTransportStats, TransportInfo};
use crate::tcp;
use crate::types::{PeerMessage, ReasonForBan};
use near_primitives::network::PeerId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Production implementation of NetworkTransport.
/// Transitional: wraps NetworkState's Pools. In PR 10, Pools move to
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

    fn transport_info(&self) -> TransportInfo {
        let tier2 = self.state.tier2.load();
        // Include T3 peer stats so PMA's idle-connection cleanup can read
        // last_time_received_message via transport_info instead of touching
        // the Pool directly.
        let tier3 = self.state.tier3.load();

        // T2 comes first; if the same peer_id appears in T3, the T2 entry
        // is kept (via `or_insert_with`). Keying by peer_id collapses
        // duplicates naturally.
        let mut peer_stats: HashMap<PeerId, PeerTransportStats> = HashMap::new();
        for (peer_id, conn) in tier2.ready.iter().chain(tier3.ready.iter()) {
            peer_stats.entry(peer_id.clone()).or_insert_with(|| {
                let s = &conn.stats;
                PeerTransportStats {
                    last_time_received_message: conn.last_time_received_message.load(),
                    last_time_peer_requested: conn.last_time_peer_requested.load(),
                    received_bytes_per_sec: s.received_bytes_per_sec.load(Ordering::Relaxed),
                    received_messages_per_sec: s.received_messages_per_sec.load(Ordering::Relaxed),
                    sent_bytes_per_sec: s.sent_bytes_per_sec.load(Ordering::Relaxed),
                }
            });
        }
        TransportInfo {
            pending_outbound: tier2.outbound_handshakes.iter().cloned().collect(),
            peer_stats,
        }
    }
}
