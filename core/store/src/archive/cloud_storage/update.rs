use std::io;
use std::sync::Arc;

use near_primitives::types::BlockHeight;

use crate::archive::cloud_storage::CloudStorage;
use crate::Store;

/// Saves the archival data associated with the block at the given height.
pub fn update_cloud_storage(
    _cloud_storage: &Arc<CloudStorage>,
    _hot_store: &Store,
    _height: &BlockHeight,
) -> io::Result<()> {
    unimplemented!("TODO(cloud_archival): Implement saving to cloud storage")
}
