use near_external_storage::ExternalConnection;

pub mod config;
pub mod download;
pub mod opener;
pub mod upload;

pub(super) mod block_data;
pub(super) mod file_id;

pub struct CloudStorage {
    external: ExternalConnection,
}
