use near_primitives::types::BlockHeight;

use crate::Store;
use crate::archive::cloud_storage::block_data::build_block_data;
use crate::archive::cloud_storage::{CloudStorage, CloudStorageFileID};

/// Error surfaced while archiving data or performing sanity checks.
#[derive(thiserror::Error, Debug)]
pub enum CloudArchivingError {
    #[error("I/O error during cloud archiving: {message}")]
    IOError { message: String },
    #[error("Chain error during cloud archiving: {error}")]
    ChainError { error: near_chain_primitives::Error },
    #[error("Failed to upload {file_id:?} to the cloud archive: {error}")]
    PutError { file_id: CloudStorageFileID, error: anyhow::Error },
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

impl CloudStorage {
    /// Saves the archival data associated with the block at the given height.
    pub async fn archive_block_data(
        &self,
        hot_store: &Store,
        block_height: BlockHeight,
    ) -> Result<(), CloudArchivingError> {
        let block_data = build_block_data(hot_store, block_height)?;
        let file_id = CloudStorageFileID::Block(block_height);
        let blob = borsh::to_vec(&block_data)?;
        self.put(file_id, blob).await
    }

    /// Persists the cloud head to external storage.
    pub async fn update_cloud_head(&self, head: BlockHeight) -> Result<(), CloudArchivingError> {
        self.put(CloudStorageFileID::Head, borsh::to_vec(&head)?).await
    }

    /// Uploads the given value to the external cloud storage under the specified
    /// `file_id`.
    async fn put(
        &self,
        file_id: CloudStorageFileID,
        value: Vec<u8>,
    ) -> Result<(), CloudArchivingError> {
        let path = file_id.path();
        self.external
            .put(&path, &value)
            .await
            .map_err(|error| CloudArchivingError::PutError { file_id, error })
    }
}
