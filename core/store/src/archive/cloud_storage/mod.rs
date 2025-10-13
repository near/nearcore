use std::sync::Arc;

use near_external_storage::ExternalConnection;

use crate::db::Database;

pub mod download;
pub mod opener;
pub mod upload;

pub(super) mod block_data;
pub(super) mod file_id;

/// Represents the external storage for archival data.
pub struct CloudStorage {
    external: ExternalConnection,
    prefetched_db: Arc<dyn Database>,
}
