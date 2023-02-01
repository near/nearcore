use crate::concurrency::arc_mutex::ArcMutex;
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager::connection;
use crate::store;
use crate::time;
use crate::types::{ConnectionInfo, PeerInfo, PeerType};
use near_primitives::network::PeerId;
use std::collections::HashMap;

/// Size of the LRU cache of recent outbound connections.
pub const OUTBOUND_CONNECTIONS_CACHE_SIZE: usize = 40;
/// How long a connection should survive before being stored.
pub(crate) const STORED_CONNECTIONS_MIN_DURATION: time::Duration = time::Duration::minutes(10);

#[derive(Clone)]
struct Inner {
    store: store::Store,
    outbound: Vec<ConnectionInfo>,
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

    /// Takes a list of ConnectionInfos and inserts them to the outbound store
    fn insert_outbound(&mut self, conns: Vec<ConnectionInfo>) {
        // Start with the connections already in storage
        let mut updated_outbound: HashMap<PeerId, ConnectionInfo> =
            self.outbound.iter().map(|c| (c.peer_info.id.clone(), c.clone())).collect();

        // Insert the new connections
        for conn_info in conns {
            updated_outbound.insert(conn_info.peer_info.id.clone(), conn_info);
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

pub(crate) struct ConnectionStore(ArcMutex<Inner>);

impl ConnectionStore {
    pub fn new(store: store::Store) -> anyhow::Result<Self> {
        let outbound = store.get_recent_outbound_connections();
        let inner = Inner { store, outbound };
        Ok(ConnectionStore(ArcMutex::new(inner)))
    }

    pub fn get_recent_outbound_connections(&self) -> Vec<ConnectionInfo> {
        return self.0.load().outbound.clone();
    }

    pub fn remove_from_recent_outbound_connections(&self, peer_id: &PeerId) {
        self.0.update(|mut inner| {
            inner.remove_outbound(peer_id);
            ((), inner)
        });
    }

    /// Called upon closing a connection. Updates the connection store and returns a boolean
    /// indicating whether the connection should be re-established.
    pub fn connection_closed(
        &self,
        peer_info: &PeerInfo,
        peer_type: &PeerType,
        reason: &ClosingReason,
    ) -> bool {
        if *peer_type != PeerType::Outbound {
            return false;
        }

        if reason.remove_from_recent_outbound_connections() {
            // If the outbound connection closed for a reason which indicates we should not
            // re-establish the connection, remove it from the connection store.
            self.0.update(|mut inner| {
                inner.remove_outbound(&peer_info.id);
                ((), inner)
            });
        }

        return self.0.load().contains_outbound(&peer_info.id);
    }

    /// Inserts information about live connections to the connection store.
    pub fn update(&self, clock: &time::Clock, tier2: connection::Pool) {
        let now = clock.now();
        let now_utc = clock.now_utc();

        // Gather information about outbound connections which have lasted long enough
        let mut outbound = vec![];
        for c in tier2.load().ready.values() {
            if c.peer_type != PeerType::Outbound {
                continue;
            }

            let connected_duration: time::Duration = now - c.established_time;
            if connected_duration < STORED_CONNECTIONS_MIN_DURATION {
                continue;
            }

            outbound.push(ConnectionInfo {
                peer_info: c.peer_info.clone(),
                first_connected: now_utc - connected_duration,
                last_connected: now_utc,
            });
        }

        self.0.update(|mut inner| {
            inner.insert_outbound(outbound);
            ((), inner)
        });
    }
}
