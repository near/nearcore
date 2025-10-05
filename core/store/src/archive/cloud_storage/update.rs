use std::path::{Path, PathBuf};
use std::sync::Arc;

use near_primitives::types::BlockHeight;

use crate::Store;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::block_data::{BlockData, build_block_data};

/// Error surfaced while archiving data or performing sanity checks.
#[derive(thiserror::Error, Debug)]
pub enum CloudArchivingError {
    #[error("Cloud archiving IO error: {message}")]
    IOError { message: String },
    #[error("Cloud archiving chain error: {error}")]
    ChainError { error: near_chain_primitives::Error },
    #[error("Error when performing put to cloud archival: {error}")]
    PutError { error: anyhow::Error },
}

impl From<std::io::Error> for CloudArchivingError {
    fn from(error: std::io::Error) -> Self {
        CloudArchivingError::IOError { message: error.to_string() }
    }
}

impl From<near_chain_primitives::Error> for CloudArchivingError {
    fn from(error: near_chain_primitives::Error) -> Self {
        CloudArchivingError::ChainError { error }
    }
}

/// Saves the archival data associated with the block at the given height.
pub async fn update_cloud_storage(
    cloud_storage: &Arc<CloudStorage>,
    hot_store: &Store,
    block_height: BlockHeight,
) -> Result<(), CloudArchivingError> {
    let block_data = build_block_data(hot_store, block_height)?;
    save_block_data(cloud_storage, block_height, block_data).await?;
    Ok(())
}

async fn save_block_data(
    cloud_storage: &Arc<CloudStorage>,
    block_height: BlockHeight,
    block_data: BlockData,
) -> Result<(), CloudArchivingError> {
    let path = PathBuf::from(block_height.to_string()).join(Path::new("block"));
    let path = path.to_str().expect("non UTF-8 string");
    let value = borsh::to_vec(&block_data)?;
    cloud_storage
        .external
        .put(&path, &value)
        .await
        .map_err(|error| CloudArchivingError::PutError { error })?;
    Ok(())
}
