use crate::peer_manager::network_state::NetworkState;
use crate::store;
use crate::time;
use crate::types::{ConnectionInfo, PeerType};
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
}

impl Inner {
    fn remove_outbound_connection(&mut self, peer_id: &PeerId) {
        self.outbound.retain(|c| c.peer_info.id != *peer_id);
        if let Err(err) = self.store.set_recent_outbound_connections(&self.outbound) {
            tracing::error!(target: "network", ?err, "Failed to save recent outbound connections");
        }
    }

    fn contains_outbound(&self, peer_id: &PeerId) -> bool {
        for stored_info in &self.outbound {
            if stored_info.peer_info.id == *peer_id {
                return true;
            }
        }
        return false;
    }

    fn update(&mut self, clock: &time::Clock, state: Arc<NetworkState>) {
        let now = clock.now();
        let now_utc = clock.now_utc();

        // Start with the connections already in storage
        let mut updated_outbound: HashMap<PeerId, ConnectionInfo> =
            self.outbound.iter().map(|c| (c.peer_info.id.clone(), c.clone())).collect();

        // Add information about live outbound connections
        for c in state.tier2.load().ready.values() {
            if c.peer_type != PeerType::Outbound {
                continue;
            }

            // If this connection is already in storage, update its last_connected time
            if let Some(stored) = updated_outbound.get_mut(&c.peer_info.id) {
                stored.last_connected = now_utc;
                continue;
            }

            // If this connection is not in storage and has lasted long enough, add it as a new entry
            let connected_duration: time::Duration = now - c.established_time;
            if connected_duration > STORED_CONNECTIONS_MIN_DURATION {
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
}

pub(crate) struct ConnectionStore(Mutex<Inner>);

impl ConnectionStore {
    pub fn new(store: store::Store) -> anyhow::Result<Self> {
        let outbound = store.get_recent_outbound_connections();
        let connection_store = Inner { store, outbound };
        Ok(ConnectionStore(Mutex::new(connection_store)))
    }

    pub fn get_recent_outbound_connections(&self) -> Vec<ConnectionInfo> {
        return self.0.lock().outbound.clone();
    }

    pub fn has_recent_outbound_connection(&self, peer_id: &PeerId) -> bool {
        return self.0.lock().contains_outbound(peer_id);
    }

    pub fn remove_from_recent_outbound_connections(&self, peer_id: &PeerId) {
        self.0.lock().remove_outbound_connection(peer_id);
    }

    pub fn update(&self, clock: &time::Clock, state: Arc<NetworkState>) {
        self.0.lock().update(clock, state);
    }
}
