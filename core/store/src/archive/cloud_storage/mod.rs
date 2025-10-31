use std::sync::Arc;

use near_external_storage::ExternalConnection;

use crate::db::Database;

pub mod config;
pub mod download;
pub mod opener;
pub mod upload;

pub(super) mod block_data;
pub(super) mod file_id;

/// Handles operations related to cloud storage used for archival data.
pub struct CloudStorage {
    external: ExternalConnection,
    pub prefetch_db: Option<Arc<dyn Database>>,
}
