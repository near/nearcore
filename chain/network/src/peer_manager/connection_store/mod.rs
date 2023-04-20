use crate::concurrency::arc_mutex::ArcMutex;
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager::connection;
use crate::store;
use crate::types::{ConnectionInfo, PeerInfo, PeerType};
use near_async::time;
use near_primitives::network::PeerId;
use std::collections::HashSet;

#[cfg(test)]
mod testonly;
#[cfg(test)]
mod tests;

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
    /// Returns whether the store contains an outbound connection to the given peer
    fn contains_outbound(&self, peer_id: &PeerId) -> bool {
        for stored_info in &self.outbound {
            if stored_info.peer_info.id == *peer_id {
                return true;
            }
        }
        return false;
    }

    /// If there is an outbound connection to the given peer in storage, removes it
    fn remove_outbound(&mut self, peer_id: &PeerId) {
        self.outbound.retain(|c| c.peer_info.id != *peer_id);
        if let Err(err) = self.store.set_recent_outbound_connections(&self.outbound) {
            tracing::error!(target: "network", ?err, "Failed to save recent outbound connections");
        }
    }

    /// Takes a list of ConnectionInfos and inserts them to the front of the outbound store.
    /// Any existing entry having the same PeerId as a newly inserted entry is dropped.
    /// Evicts from the back if OUTBOUND_CONNECTIONS_CACHE_SIZE is reached.
    /// Timestamps are stored for debugging purposes, but not otherwise used.
    fn push_front_outbound(&mut self, mut conns: Vec<ConnectionInfo>) {
        // Collect the PeerIds of the newly inserted connections
        let updated_peer_ids: HashSet<PeerId> =
            conns.iter().map(|c| c.peer_info.id.clone()).collect();

        // Append entries from storage for disconnected peers, preserving order
        for stored in &self.outbound {
            if !updated_peer_ids.contains(&stored.peer_info.id) {
                conns.push(stored.clone());
            }
        }

        // Evict the longest-disconnected connections, if needed
        conns.truncate(OUTBOUND_CONNECTIONS_CACHE_SIZE);

        if let Err(err) = self.store.set_recent_outbound_connections(&conns) {
            tracing::error!(target: "network", ?err, "Failed to save recent outbound connections");
        }
        self.outbound = conns;
    }
}

/// ConnectionStore is cheap to clone, so we can use ArcMutex and avoid blocking on read
pub(crate) struct ConnectionStore(ArcMutex<Inner>);

impl ConnectionStore {
    pub fn new(store: store::Store) -> anyhow::Result<Self> {
        let outbound = store.get_recent_outbound_connections();
        let inner = Inner { store, outbound };
        Ok(ConnectionStore(ArcMutex::new(inner)))
    }

    /// Returns all stored outbound connections
    pub fn get_recent_outbound_connections(&self) -> Vec<ConnectionInfo> {
        return self.0.load().outbound.clone();
    }

    /// If there is an outbound connection to the given peer in storage, removes it
    pub fn remove_from_connection_store(&self, peer_id: &PeerId) {
        self.0.update(|mut inner| {
            inner.remove_outbound(peer_id);
            ((), inner)
        });
    }

    /// Called upon closing a connection. If closed for a reason indicating we should
    /// not reconnect (for example, a ban), removes the connection from storage.
    /// Returns a boolean indicating whether the connection should be re-established.
    pub fn connection_closed(
        &self,
        peer_info: &PeerInfo,
        peer_type: &PeerType,
        reason: &ClosingReason,
    ) -> bool {
        if *peer_type != PeerType::Outbound {
            return false;
        }

        if reason.remove_from_connection_store() {
            // If the outbound connection closed for a reason which indicates we should not
            // re-establish the connection, remove it from the connection store.
            self.0.update(|mut inner| {
                inner.remove_outbound(&peer_info.id);
                ((), inner)
            });
        }

        return self.0.load().contains_outbound(&peer_info.id);
    }

    /// Given a snapshot of the TIER2 connection pool, updates the connections in storage.
    pub fn update(&self, clock: &time::Clock, tier2: &connection::PoolSnapshot) {
        let now = clock.now();
        let now_utc = clock.now_utc();

        // Gather information about active outbound connections which have lasted long enough
        let mut outbound = vec![];
        for c in tier2.ready.values() {
            if c.peer_type != PeerType::Outbound {
                continue;
            }

            let connected_duration: time::Duration = now - c.established_time;
            if connected_duration < STORED_CONNECTIONS_MIN_DURATION {
                continue;
            }

            outbound.push(ConnectionInfo {
                peer_info: c.peer_info.clone(),
                time_established: now_utc - connected_duration,
                time_connected_until: now_utc,
            });
        }

        // Push the information about the active connections to the front of the store
        self.0.update(|mut inner| {
            inner.push_front_outbound(outbound);
            ((), inner)
        });
    }
}
