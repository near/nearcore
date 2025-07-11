use std::io;
use std::sync::Arc;

use near_primitives::types::BlockHeight;

use crate::Store;
use crate::config::ArchivalStorageConfig;

/// Represents the storage for archival data.
pub struct ArchivalStorage;

/// Opener for the archival storage, which results in an `ArchivalStorage` instance.
pub struct ArchivalStorageOpener {
    /// NEAR home directory (eg. '/home/ubuntu/.near')
    _home_dir: std::path::PathBuf,
    /// Configuration for the archival storage.
    _config: ArchivalStorageConfig,
}

impl ArchivalStorageOpener {
    pub fn new(home_dir: std::path::PathBuf, config: ArchivalStorageConfig) -> Self {
        Self { _home_dir: home_dir, _config: config }
    }

    pub fn open(&self) -> io::Result<Arc<ArchivalStorage>> {
        unimplemented!("TODO(archival_v2): Implement opening archival storage")
    }
}

/// Saves the archival data associated with the block at the given height.
pub fn update_archival_storage(
    _archival_storage: &Arc<ArchivalStorage>,
    _hot_store: &Store,
    _height: &BlockHeight,
) -> io::Result<()> {
    unimplemented!("TODO(archival_v2): Implement saving to archival storage")
}
