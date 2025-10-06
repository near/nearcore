use std::path::PathBuf;
use std::sync::Arc;

use near_primitives::types::BlockHeight;

use crate::Store;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::block_data::build_block_data;

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

enum CloudStorageFileID {
    Head,
    Block(BlockHeight),
}

impl CloudStorageFileID {
    fn to_path(&self) -> String {
        let path_parts = match self {
            CloudStorageFileID::Head => vec!["head".to_string()],
            CloudStorageFileID::Block(height) => vec![height.to_string(), "block".to_string()],
        };
        let path = PathBuf::from_iter(path_parts);
        path.to_str().expect("non UTF-8 string").to_string()
    }
}

async fn put(
    cloud_storage: &Arc<CloudStorage>,
    file_id: CloudStorageFileID,
    value: Vec<u8>,
) -> Result<(), CloudArchivingError> {
    let path = file_id.to_path();
    cloud_storage
        .external
        .put(&path, &value)
        .await
        .map_err(|error| CloudArchivingError::PutError { error })
}

/// Saves the archival data associated with the block at the given height.
pub async fn archive_block_data(
    cloud_storage: &Arc<CloudStorage>,
    hot_store: &Store,
    block_height: BlockHeight,
) -> Result<(), CloudArchivingError> {
    let block_data = build_block_data(hot_store, block_height)?;
    let file_id = CloudStorageFileID::Block(block_height);
    let blob = borsh::to_vec(&block_data)?;
    put(cloud_storage, file_id, blob).await
}

/// Persists the cloud head to external storage.
pub async fn update_cloud_head(
    cloud_storage: &Arc<CloudStorage>,
    head: BlockHeight,
) -> Result<(), CloudArchivingError> {
    put(cloud_storage, CloudStorageFileID::Head, borsh::to_vec(&head)?).await
}
