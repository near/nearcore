/// Store module defines atomic DB operations on top of schema module.
/// All transactions should be implemented within this module,
/// in particular schema::StoreUpdate is not exported.
use crate::types::ConnectionInfo;
use std::sync::Arc;

mod schema;

/// Store allows for performing synchronous atomic operations on the DB.
/// In particular it doesn't implement Clone and requires &mut self for
/// methods writing to the DB.
#[derive(Clone)]
pub(crate) struct Store(schema::Store);

// ConnectionStore storage.
impl Store {
    #[tracing::instrument(
        target = "network::store",
        level = "trace",
        "Store::set_recent_outbound_connections",
        skip_all
    )]
    pub fn set_recent_outbound_connections(
        &self,
        recent_outbound_connections: &Vec<ConnectionInfo>,
    ) {
        let mut update = self.0.new_update();
        update.set::<schema::RecentOutboundConnections>(&(), &recent_outbound_connections);
        self.0.commit(update)
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
