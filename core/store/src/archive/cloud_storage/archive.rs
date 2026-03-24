use crate::Store;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::block_data::build_block_data;
use crate::archive::cloud_storage::bucket_config::BucketConfig;
use crate::archive::cloud_storage::epoch_data::build_epoch_data;
use crate::archive::cloud_storage::file_id::{CloudDir, CloudStorageFileID};
use crate::archive::cloud_storage::retrieve::CloudRetrievalError;
use crate::archive::cloud_storage::shard_data::build_shard_data;
use near_primitives::errors::EpochError;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::{BlockHeight, EpochId, ShardId};

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
    #[error("Retrieval error during cloud archiving: {error}")]
    RetrievalError { error: CloudRetrievalError },
    #[error("Bucket config mismatch: local {local:?} != remote {remote:?}")]
    ConfigMismatch { local: BucketConfig, remote: BucketConfig },
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

impl From<CloudRetrievalError> for CloudArchivingError {
    fn from(error: CloudRetrievalError) -> Self {
        CloudArchivingError::RetrievalError { error }
    }
}

impl CloudStorage {
    /// Ensures the bucket config matches the local config.
    /// If no config exists in the bucket, writes the local config.
    /// If a config exists, validates it matches. Returns an error on mismatch.
    // TODO(cloud_archival): Race condition between this check and the upload below.
    // Will be replaced with ifGenerationMatch:0 atomic uploads.
    pub async fn ensure_bucket_config(&self) -> Result<(), CloudArchivingError> {
        let file_id = CloudStorageFileID::Config;
        let (_, filename) = self.location_dir_and_file(&file_id);
        let exists = self
            .dir_contains(&CloudDir::Metadata, &filename)
            .await
            .map_err(CloudArchivingError::from)?;
        let existing: Option<BucketConfig> = if exists {
            Some(self.retrieve(&file_id).await.map_err(CloudArchivingError::from)?)
        } else {
            None
        };
        let local_config = BucketConfig::local();
        match existing {
            None => {
                tracing::info!(target: "cloud_archival", ?local_config, "Writing bucket config");
                let blob = borsh::to_vec(&local_config).unwrap();
                self.upload(file_id, blob).await
            }
            Some(remote_config) => {
                if remote_config != local_config {
                    return Err(CloudArchivingError::ConfigMismatch {
                        local: local_config,
                        remote: remote_config,
                    });
                }
                tracing::info!(target: "cloud_archival", ?local_config, "Bucket config validated");
                Ok(())
            }
        }
    }

    /// Saves the archival data associated with the given epoch ID.
    pub async fn archive_epoch_data(
        &self,
        hot_store: &Store,
        shard_layout: &ShardLayout,
        epoch_id: EpochId,
    ) -> Result<(), CloudArchivingError> {
        let epoch_data = build_epoch_data(hot_store, shard_layout.clone(), epoch_id)?;
        let file_id = CloudStorageFileID::Epoch(epoch_id);
        let blob = borsh::to_vec(&epoch_data).unwrap();
        self.upload_compressed(file_id, blob).await
    }

    /// Saves the archival data associated with the block at the given height.
    pub async fn archive_block_data(
        &self,
        hot_store: &Store,
        block_height: BlockHeight,
    ) -> Result<(), CloudArchivingError> {
        let block_data = build_block_data(hot_store, block_height)?;
        let file_id = CloudStorageFileID::Block(block_height);
        let blob = borsh::to_vec(&block_data).unwrap();
        self.upload_compressed(file_id, blob).await
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
        let blob = borsh::to_vec(&shard_data).unwrap();
        self.upload_compressed(file_id, blob).await
    }

    /// Compresses and uploads data to cloud storage.
    /// Used for block, shard, and epoch data blobs — NOT for metadata.
    // TODO(cloud_archival): Benchmark compression: optimal level, spawn_blocking,
    // multithreaded compression for large shard blobs.
    pub(super) async fn upload_compressed(
        &self,
        file_id: CloudStorageFileID,
        value: Vec<u8>,
    ) -> Result<(), CloudArchivingError> {
        let compressed =
            zstd::encode_all(value.as_slice(), BucketConfig::local().compression_level())?;
        self.upload(file_id, compressed).await
    }

    /// Persists the block head to external storage.
    pub async fn update_cloud_block_head(
        &self,
        head: BlockHeight,
    ) -> Result<(), CloudArchivingError> {
        let file_id = CloudStorageFileID::BlockHead;
        let blob = borsh::to_vec(&head).unwrap();
        self.upload(file_id, blob).await
    }

    /// Persists a shard head to external storage.
    pub async fn update_cloud_shard_head(
        &self,
        shard_id: ShardId,
        head: BlockHeight,
    ) -> Result<(), CloudArchivingError> {
        let file_id = CloudStorageFileID::ShardHead(shard_id);
        let blob = borsh::to_vec(&head).unwrap();
        self.upload(file_id, blob).await
    }

    /// Uploads the given value to the external cloud storage under the specified
    /// `file_id`.
    pub(super) async fn upload(
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
