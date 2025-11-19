use std::io::Result;
use std::sync::Arc;

use near_external_storage::ExternalConnection;

use near_chain_configs::ExternalStorageLocation;

use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::config::CloudArchivalConfig;

/// Opener for the external archival storage, which results in an `CloudStorage` instance.
pub struct CloudStorageOpener {
    /// Configuration for the cloud archival storage.
    config: CloudArchivalConfig,
}

impl CloudStorageOpener {
    pub fn new(config: CloudArchivalConfig) -> Self {
        Self { config }
    }

    pub fn open(&self) -> Result<Arc<CloudStorage>> {
        let external = self.create_external_connection();
        let cloud_storage = CloudStorage { external };
        Ok(Arc::new(cloud_storage))
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
        let location = &self.config.cloud_storage.location;
        if !Self::is_storage_location_supported(location) {
            panic!("{} is not supported cloud storage location", location.name())
        }
        ExternalConnection::new(location, self.config.cloud_storage.credentials_file.clone(), None)
    }
}
