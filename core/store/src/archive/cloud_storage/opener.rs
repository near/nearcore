use std::io::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use near_external_storage::ExternalConnection;

use near_chain_configs::ExternalStorageLocation;

use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::config::CloudArchivalConfig;
use crate::db::{Database, RocksDB};
use crate::{Mode, Temperature};

/// Opener for the external archival storage, which results in an `CloudStorage` instance.
pub struct CloudStorageOpener {
    home_dir: PathBuf,
    config: CloudArchivalConfig,
}

impl CloudStorageOpener {
    pub fn new(home_dir: &Path, config: CloudArchivalConfig) -> Self {
        Self { home_dir: home_dir.to_path_buf(), config }
    }

    pub fn open(&self) -> Result<Arc<CloudStorage>> {
        let external = self.create_external_connection();
        let prefetch_db = self.open_prefetch_db()?;
        let cloud_storage = CloudStorage { external, prefetch_db };
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

    fn open_prefetch_db(&self) -> Result<Option<Arc<dyn Database>>> {
        let Some(config) = &self.config.prefetch_db else {
            return Ok(None);
        };
        let path = config.path.as_deref().unwrap_or_else(|| std::path::Path::new("cloud_prefetch"));
        let path = self.home_dir.join(path);
        let db = RocksDB::open(&path, config, Mode::ReadWrite, Temperature::Cold)?;
        Ok(Some(Arc::new(db)))
    }
}
