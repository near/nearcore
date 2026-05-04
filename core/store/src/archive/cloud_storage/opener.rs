use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::bucket_config::BucketConfig;
use crate::archive::cloud_storage::config::CloudStorageContext;
use near_chain_configs::ExternalStorageLocation;
use near_external_storage::ExternalConnection;
use std::io::Result;
use std::sync::Arc;

/// Opener for the external archival storage, which results in an `CloudStorage` instance.
pub struct CloudStorageOpener {
    /// Context for the cloud archival storage (location, credentials, chain_id).
    context: CloudStorageContext,
    /// Archive-wide config carried through to the opened `CloudStorage`. Not
    /// used during open itself.
    bucket_config: BucketConfig,
}

impl CloudStorageOpener {
    pub fn new(context: CloudStorageContext, bucket_config: BucketConfig) -> Self {
        Self { context, bucket_config }
    }

    pub fn open(&self) -> Result<Arc<CloudStorage>> {
        let external = self.create_external_connection();
        let chain_id = self.context.chain_id.clone();
        let cloud_storage = CloudStorage::new(external, chain_id, self.bucket_config.clone());
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
        let location = &self.context.cloud_archive.location;
        if !Self::is_storage_location_supported(location) {
            panic!("{:?} is not a supported cloud storage location", location)
        }
        ExternalConnection::new(location, self.context.cloud_archive.credentials_file.clone(), None)
    }
}
