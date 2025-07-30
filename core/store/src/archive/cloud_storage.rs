use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use near_primitives::types::BlockHeight;

use crate::Store;
use crate::config::CloudStorageConfig;

/// Represents the external storage for archival data.
pub struct CloudStorage;

/// Opener for the external archival storage, which results in an `CloudStorage` instance.
pub struct CloudStorageOpener {
    /// NEAR home directory (eg. '/home/ubuntu/.near')
    _home_dir: PathBuf,
    /// Configuration for the external storage.
    _config: CloudStorageConfig,
}

impl CloudStorageOpener {
    pub fn new(home_dir: PathBuf, config: CloudStorageConfig) -> Self {
        Self { _home_dir: home_dir, _config: config }
    }

    pub fn open(&self) -> io::Result<Arc<CloudStorage>> {
        unimplemented!("TODO(cloud_archival): Implement opening cloud storage")
    }
}

/// Saves the archival data associated with the block at the given height.
pub fn update_cloud_storage(
    _cloud_storage: &Arc<CloudStorage>,
    _hot_store: &Store,
    _height: &BlockHeight,
) -> io::Result<()> {
    unimplemented!("TODO(cloud_archival): Implement saving to cloud storage")
}
