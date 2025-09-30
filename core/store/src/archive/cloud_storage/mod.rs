use near_primitives::external::ExternalConnection;

pub(crate) mod block_data;
pub mod opener;
pub mod update;

#[allow(unused)]
/// Represents the external storage for archival data.
pub struct CloudStorage {
    pub(crate) connection: ExternalConnection,
}
