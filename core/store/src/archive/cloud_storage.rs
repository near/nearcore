use std::io;
use std::sync::Arc;

use near_chain_configs::CloudStorageConfig;
use near_primitives::types::BlockHeight;

use crate::Store;

/// Represents the external storage for archival data.
pub struct CloudStorage;

/// Opener for the external archival storage, which results in an `CloudStorage` instance.
pub struct CloudStorageOpener {
    /// Configuration for the external storage.
    _config: CloudStorageConfig,
}

impl CloudStorageOpener {
    pub fn new(config: CloudStorageConfig) -> Self {
        Self { _config: config }
    }

    pub fn open(&self) -> Arc<CloudStorage> {
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
