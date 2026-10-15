#[cfg(feature = "cloud_archive")]
pub mod cloud_storage;
pub mod cold_storage;

/// Uninhabited stand-in so `NodeStorage` keeps its API without the
/// `cloud_archive` feature; a cloud storage can never be constructed.
#[cfg(not(feature = "cloud_archive"))]
pub mod cloud_storage {
    pub enum CloudStorage {}
}
