/// Store module defines atomic DB operations on top of schema module.
/// All transactions should be implemented within this module,
/// in particular schema::StoreUpdate is not exported.
use crate::types::ConnectionInfo;
use near_primitives::network::AnnounceAccount;
use near_primitives::types::AccountId;
use std::sync::Arc;

mod schema;

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
