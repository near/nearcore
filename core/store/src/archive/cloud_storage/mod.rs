use near_external_storage::ExternalConnection;

pub mod config;
pub mod download;
pub mod get;
pub mod opener;
pub mod upload;

pub(super) mod block_data;
pub(super) mod file_id;
pub(super) mod shard_data;

/// Handles operations related to cloud storage used for archival data.
pub struct CloudStorage {
    /// Connection to the external storage backend (e.g. S3, GCS, filesystem).
    external: ExternalConnection,
    chain_id: String,
}
