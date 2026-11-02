#[cfg(feature = "rocksdb")]
pub mod cloud_storage;
pub mod cold_storage;

/// Uninhabited stand-in so `NodeStorage` keeps its API without the `rocksdb`
/// feature; a cloud storage can never be constructed.
#[cfg(not(feature = "rocksdb"))]
pub mod cloud_storage {
    pub enum CloudStorage {}
}
