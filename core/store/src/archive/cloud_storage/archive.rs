use near_primitives::errors::EpochError;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::BlockHeight;

use crate::Store;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::block_data::build_block_data;
use crate::archive::cloud_storage::file_id::CloudStorageFileID;
use crate::archive::cloud_storage::shard_data::build_shard_data;

/// Error surfaced while archiving data or performing sanity checks.
#[derive(thiserror::Error, Debug)]
pub enum CloudArchivingError {
    #[error("I/O error during cloud archiving: {message}")]
    IOError { message: String },
    #[error("Chain error during cloud archiving: {error}")]
    ChainError { error: near_chain_primitives::Error },
    #[error("Failed to upload {file_id:?} to the cloud archive: {error}")]
    PutError { file_id: CloudStorageFileID, error: anyhow::Error },
    #[error("Epoch error during cloud archiving: {error}")]
    EpochError { error: EpochError },
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

impl From<EpochError> for CloudArchivingError {
    fn from(error: EpochError) -> Self {
        CloudArchivingError::EpochError { error }
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
        self.upload(file_id, blob).await
    }

    /// Saves the archival data associated with the given block height and shard ID.
    pub async fn archive_shard_data(
        &self,
        hot_store: &Store,
        genesis_height: BlockHeight,
        shard_layout: &ShardLayout,
        block_height: BlockHeight,
        shard_uid: ShardUId,
    ) -> Result<(), CloudArchivingError> {
        let shard_data =
            build_shard_data(hot_store, genesis_height, shard_layout, block_height, shard_uid)?;
        let file_id = CloudStorageFileID::Shard(block_height, shard_uid.shard_id());
        let blob = borsh::to_vec(&shard_data)?;
        self.upload(file_id, blob).await
    }

    /// Persists the cloud head to external storage.
    pub async fn update_cloud_head(&self, head: BlockHeight) -> Result<(), CloudArchivingError> {
        self.upload(CloudStorageFileID::Head, borsh::to_vec(&head)?).await
    }

    /// Uploads the given value to the external cloud storage under the specified
    /// `file_id`.
    async fn upload(
        &self,
        file_id: CloudStorageFileID,
        value: Vec<u8>,
    ) -> Result<(), CloudArchivingError> {
        let path = self.file_path(&file_id);
        self.external
            .put(&path, &value)
            .await
            .map_err(|error| CloudArchivingError::PutError { file_id, error })
    }
}
