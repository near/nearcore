use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager::network_state::NetworkState;
use crate::store;
use crate::time;
use crate::types::{ConnectionInfo, PeerInfo, PeerType};
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Size of the LRU cache of recent outbound connections.
/// The longest-disconnected elements are evicted first.
pub const OUTBOUND_CONNECTIONS_CACHE_SIZE: usize = 40;
/// How long a connection should survive before being stored.
pub(crate) const STORED_CONNECTIONS_MIN_DURATION: time::Duration = time::Duration::minutes(10);

struct Inner {
    store: store::Store,
    outbound: Vec<ConnectionInfo>,
    /// List of peers to which we should re-establish a connection (not persisted to storage)
    pending_reconnect: Vec<PeerInfo>,
}

impl Inner {
    fn contains_outbound(&self, peer_id: &PeerId) -> bool {
        for stored_info in &self.outbound {
            if stored_info.peer_info.id == *peer_id {
                return true;
            }
        }
        return false;
    }

    fn remove_outbound(&mut self, peer_id: &PeerId) {
        self.outbound.retain(|c| c.peer_info.id != *peer_id);
        if let Err(err) = self.store.set_recent_outbound_connections(&self.outbound) {
            tracing::error!(target: "network", ?err, "Failed to save recent outbound connections");
        }
    }

    fn update(&mut self, clock: &time::Clock, state: Arc<NetworkState>) {
        let now = clock.now();
        let now_utc = clock.now_utc();

        // Start with the connections already in storage
        let mut updated_outbound: HashMap<PeerId, ConnectionInfo> =
            self.outbound.iter().map(|c| (c.peer_info.id.clone(), c.clone())).collect();

        // Add information about live outbound connections which have lasted long enough
        for c in state.tier2.load().ready.values() {
            if c.peer_type != PeerType::Outbound {
                continue;
            }

            let connected_duration: time::Duration = now - c.established_time;
            if connected_duration < STORED_CONNECTIONS_MIN_DURATION {
                continue;
            }

            // If there was already an entry for this peer (from storage) we'll overwite it,
            // always keeping the most recent long-enough outbound connection.
            let first_connected: time::Utc = now_utc - connected_duration;
            updated_outbound.insert(
                c.peer_info.id.clone(),
                ConnectionInfo {
                    peer_info: c.peer_info.clone(),
                    first_connected: first_connected,
                    last_connected: now_utc,
                },
            );
        }

        // Order by last_connected, with longer-disconnected nodes appearing later
        // Break ties by first_connected for convenience when testing
        let mut updated_outbound: Vec<ConnectionInfo> =
            updated_outbound.values().cloned().collect();
        updated_outbound.sort_by_key(|c| (c.last_connected, c.first_connected));
        updated_outbound.reverse();

        // Evict the longest-disconnected connections, if needed
        updated_outbound.truncate(OUTBOUND_CONNECTIONS_CACHE_SIZE);

        if let Err(err) = self.store.set_recent_outbound_connections(&updated_outbound) {
            tracing::error!(target: "network", ?err, "Failed to save recent outbound connections");
        }
        self.outbound = updated_outbound;
    }

    pub fn connection_closed(
        &mut self,
        peer_info: &PeerInfo,
        peer_type: &PeerType,
        reason: &ClosingReason,
    ) {
        if *peer_type == PeerType::Outbound {
            // If the connection is not in the store, do nothing.
            if !self.contains_outbound(&peer_info.id) {
                return;
            }

            if reason.remove_from_recent_outbound_connections() {
                // If the outbound connection closed for a reason which indicates we should not
                // re-establish the connection, remove it from the connection store.
                self.remove_outbound(&peer_info.id);
            } else {
                // Otherwise, attempt to re-establish the connection.
                self.pending_reconnect.push(peer_info.clone());
            }
        }
    }

    fn poll_pending_reconnect(&mut self) -> Vec<PeerInfo> {
        let result = self.pending_reconnect.clone();
        self.pending_reconnect.clear();
        return result;
    }
}

pub(crate) struct ConnectionStore(Mutex<Inner>);

impl ConnectionStore {
    pub fn new(store: store::Store) -> anyhow::Result<Self> {
        let outbound = store.get_recent_outbound_connections();
        let inner = Inner { store, outbound, pending_reconnect: vec![] };
        Ok(ConnectionStore(Mutex::new(inner)))
    }

    pub fn get_recent_outbound_connections(&self) -> Vec<ConnectionInfo> {
        return self.0.lock().outbound.clone();
    }

    pub fn remove_from_recent_outbound_connections(&self, peer_id: &PeerId) {
        self.0.lock().remove_outbound(peer_id);
    }

    pub fn connection_closed(
        &self,
        peer_info: &PeerInfo,
        peer_type: &PeerType,
        reason: &ClosingReason,
    ) {
        self.0.lock().connection_closed(peer_info, peer_type, reason);
    }

    pub fn poll_pending_reconnect(&self) -> Vec<PeerInfo> {
        return self.0.lock().poll_pending_reconnect();
    }

    pub fn update(&self, clock: &time::Clock, state: Arc<NetworkState>) {
        self.0.lock().update(clock, state);
    }
}
