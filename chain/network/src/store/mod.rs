/// Store module defines atomic DB operations on top of schema module.
/// All transactions should be implemented within this module,
/// in particular schema::StoreUpdate is not exported.
use crate::network_protocol::Edge;
use crate::types::ConnectionInfo;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::debug;

mod schema;
#[cfg(test)]
pub mod testonly;

/// Opaque error type representing storage errors.
///
/// Invariant: any store error is a critical operational operational error
/// which signals about data corruption. It wouldn't be wrong to replace all places /// where the error originates with outright panics.
///
/// If you have an error condition which needs to be handled somehow, it should be
/// some *other* error type.
#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub(crate) struct Error(schema::Error);

/// Store allows for performing synchronous atomic operations on the DB.
/// In particular it doesn't implement Clone and requires &mut self for
/// methods writing to the DB.
#[derive(Clone)]
pub(crate) struct Store(schema::Store);

/// Everytime a group of peers becomes unreachable at the same time; We store edges belonging to
/// them in components. We remove all of those edges from memory, and save them to database,
/// If any of them become reachable again, we re-add whole component.
///
/// To store components, we have following column in the DB.
/// DBCol::LastComponentNonce -> stores component_nonce: u64, which is the lowest nonce that
///                          hasn't been used yet. If new component gets created it will use
///                          this nonce.
/// DBCol::ComponentEdges     -> Mapping from `component_nonce` to list of edges
/// DBCol::PeerComponent      -> Mapping from `peer_id` to last component nonce if there
///                          exists one it belongs to.
impl Store {
    /// Inserts (account_id,aa) to the AccountAnnouncements column.
    pub fn set_account_announcement(
        &mut self,
        account_id: &AccountId,
        aa: &AnnounceAccount,
    ) -> Result<(), Error> {
        let mut update = self.0.new_update();
        update.set::<schema::AccountAnnouncements>(account_id, aa);
        self.0.commit(update).map_err(Error)
    }

    /// Fetches row with key account_id from the AccountAnnouncements column.
    pub fn get_account_announcement(
        &self,
        account_id: &AccountId,
    ) -> Result<Option<AnnounceAccount>, Error> {
        self.0.get::<schema::AccountAnnouncements>(account_id).map_err(Error)
    }

    /// Atomically stores a graph component consisting of <peers> and <edges>
    /// to the DB. On completion, all peers are considered members of the new component
    /// (even if they were members of a different component so far).
    /// The name (even though technically correct) is misleading, because the <edges> do
    /// NOT have to constitute a CONNECTED component. I'm not fixing that because
    /// the whole routing table in the current form is scheduled for deprecation.
    pub fn push_component(
        &mut self,
        peers: &HashSet<PeerId>,
        edges: &Vec<Edge>,
    ) -> Result<(), Error> {
        debug!(target: "network", "push_component: moving {} peers from memory to DB", peers.len());
        let component =
            self.0.get::<schema::LastComponentNonce>(&()).map_err(Error)?.unwrap_or(0) + 1;
        let mut update = self.0.new_update();
        update.set::<schema::LastComponentNonce>(&(), &component);
        update.set::<schema::ComponentEdges>(&component, &edges);
        for peer_id in peers {
            update.set::<schema::PeerComponent>(peer_id, &component);
        }
        self.0.commit(update).map_err(Error)
    }

    /// Reads and deletes from DB the component that <peer_id> is a member of.
    /// Returns Ok(vec![]) if peer_id is not a member of any component.
    pub fn pop_component(&mut self, peer_id: &PeerId) -> Result<Vec<Edge>, Error> {
        // Fetch the component assigned to the peer.
        let component = match self.0.get::<schema::PeerComponent>(peer_id).map_err(Error)? {
            Some(c) => c,
            None => return Ok(vec![]),
        };
        let edges =
            self.0.get::<schema::ComponentEdges>(&component).map_err(Error)?.unwrap_or(vec![]);
        let mut update = self.0.new_update();
        update.delete::<schema::ComponentEdges>(&component);
        let mut peers_checked = HashSet::new();
        for edge in &edges {
            let key = edge.key();
            for peer_id in [&key.0, &key.1] {
                if !peers_checked.insert(peer_id.clone()) {
                    // Store doesn't accept 2 mutations modifying the same row in a single
                    // transaction, even if they are identical. Therefore tracking peers_checked
                    // is critical for correctness, rather than just an optimization minimizing
                    // the number of lookups.
                    continue;
                }
                match self.0.get::<schema::PeerComponent>(&peer_id).map_err(Error)? {
                    Some(c) if c == component => update.delete::<schema::PeerComponent>(&peer_id),
                    _ => {}
                }
            }
        }
        self.0.commit(update).map_err(Error)?;
        Ok(edges)
    }
}

// ConnectionStore storage.
impl Store {
    pub fn set_recent_outbound_connections(
        &mut self,
        recent_outbound_connections: &Vec<ConnectionInfo>,
    ) -> Result<(), Error> {
        let mut update = self.0.new_update();
        update.set::<schema::RecentOutboundConnections>(&(), &recent_outbound_connections);
        self.0.commit(update).map_err(Error)
    }

    pub fn get_recent_outbound_connections(&self) -> Vec<ConnectionInfo> {
        self.0
            .get::<schema::RecentOutboundConnections>(&())
            .unwrap_or(Some(vec![]))
            .unwrap_or(vec![])
    }
}

impl From<Arc<dyn near_store::db::Database>> for Store {
    fn from(store: Arc<dyn near_store::db::Database>) -> Self {
        Self(schema::Store::from(store))
    }
}

#[cfg(test)]
impl From<Arc<near_store::db::TestDB>> for Store {
    fn from(store: Arc<near_store::db::TestDB>) -> Self {
        let database: Arc<dyn near_store::db::Database> = store;
        Self(schema::Store::from(database))
    }
}
