use std::path::{Path, PathBuf};
use std::sync::Arc;

use near_chain_primitives::Error;
use near_primitives::types::BlockHeight;

use crate::Store;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::block_data::{BlockData, build_block_data};

/// Saves the archival data associated with the block at the given height.
pub fn update_cloud_storage(
    cloud_storage: &Arc<CloudStorage>,
    hot_store: &Store,
    block_height: &BlockHeight,
) -> Result<(), Error> {
    let block_data = build_block_data(hot_store, block_height)?;
    save_block_data(cloud_storage, block_height, block_data)
}

#[allow(unused)]
fn save_block_data(
    cloud_storage: &Arc<CloudStorage>,
    block_height: &BlockHeight,
    block_data: BlockData,
) -> Result<(), Error> {
    let path = PathBuf::from(block_height.to_string()).join(Path::new("block"));
    let value = borsh::to_vec(&block_data)?;
    cloud_storage.connection.put(&path.to_str().expect("non UTF-8 string"), &value);
    Ok(())
}
