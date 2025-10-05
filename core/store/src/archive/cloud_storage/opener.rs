use std::sync::Arc;

use near_primitives::external::{ExternalConnection, ExternalStorageLocation};

use near_chain_configs::CloudStorageConfig;

use crate::archive::cloud_storage::CloudStorage;

/// Opener for the external archival storage, which results in an `CloudStorage` instance.
#[allow(unused)]
pub struct CloudStorageOpener {
    /// Configuration for the external storage.
    config: CloudStorageConfig,
}

impl CloudStorageOpener {
    pub fn new(config: CloudStorageConfig) -> Self {
        Self { config }
    }

    pub fn open(&self) -> Arc<CloudStorage> {
        let location = &self.config.storage;
        if !Self::is_storage_location_supported(location) {
            panic!("{} is not supported cloud storage location", location.name())
        }
        let connection =
            ExternalConnection::new(location, self.config.credentials_file.clone(), None);
        let cloud_storage = CloudStorage { external: connection };
        Arc::new(cloud_storage)
    }

    pub fn is_storage_location_supported(location: &ExternalStorageLocation) -> bool {
        match location {
            ExternalStorageLocation::Filesystem { .. } | ExternalStorageLocation::GCS { .. } => {
                true
            }
            // TODO(cloud_archival) Add S3 support
            _ => false,
        }
    }
}
