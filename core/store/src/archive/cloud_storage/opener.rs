use std::sync::Arc;

use near_external_storage::ExternalConnection;

use near_chain_configs::ExternalStorageLocation;

use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::config::CloudStorageConfig;

/// Opener for the external archival storage, which results in an `CloudStorage` instance.
pub struct CloudStorageOpener {
    config: CloudStorageConfig,
}

impl CloudStorageOpener {
    pub fn new(config: CloudStorageConfig) -> Self {
        Self { config }
    }

    pub fn open(&self) -> Arc<CloudStorage> {
        let external = self.create_external_connection();
        Arc::new(CloudStorage { external })
    }

    /// Returns `true` if the given storage backend is supported by cloud archival.
    pub fn is_storage_location_supported(location: &ExternalStorageLocation) -> bool {
        match location {
            ExternalStorageLocation::Filesystem { .. } | ExternalStorageLocation::GCS { .. } => {
                true
            }
            // TODO(cloud_archival) Add S3 support
            ExternalStorageLocation::S3 { .. } => false,
        }
    }

    /// Initializes a connection to the configured cloud storage.
    ///
    /// Panics if the configured storage location is not supported.
    fn create_external_connection(&self) -> ExternalConnection {
        let location = &self.config.location;
        if !Self::is_storage_location_supported(location) {
            panic!("{} is not supported cloud storage location", location.name())
        }
        ExternalConnection::new(location, self.config.credentials_file.clone(), None)
    }
}
