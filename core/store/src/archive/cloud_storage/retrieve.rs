use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::blocks::BlockBatch;
use crate::archive::cloud_storage::epoch_data::EpochData;
use crate::archive::cloud_storage::file_id::{BatchId, CloudStorageFileID, ListableCloudDir};
use crate::archive::cloud_storage::shards::ShardBatch;
use borsh::BorshDeserialize;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};

/// Errors surfaced while retrieving data from the cloud archive.
#[derive(thiserror::Error, Debug)]
pub enum CloudRetrievalError {
    #[error("Failed to retrieve {file_id:?} from the cloud archive: {error}")]
    GetError { file_id: CloudStorageFileID, error: anyhow::Error },
    #[error("Failed to decompress {file_id:?} from the cloud archive: {error}")]
    DecompressionError { file_id: CloudStorageFileID, error: std::io::Error },
    #[error("Failed to deserialize {file_id:?} from the cloud archive: {error}")]
    DeserializeError { file_id: CloudStorageFileID, error: borsh::io::Error },
    #[error("Failed to list directory in the cloud archive: {dir}; error: {error}")]
    ListError { dir: String, error: anyhow::Error },
    #[error("Invalid file {file_id:?} from the cloud archive: {reason}")]
    InvalidFile { file_id: CloudStorageFileID, reason: String },
}

impl CloudStorage {
    /// Lists the entries in a cloud directory.
    pub async fn list_dir(
        &self,
        dir: &ListableCloudDir,
    ) -> Result<Vec<String>, CloudRetrievalError> {
        let full_path = format!("chain_id={}/{}", self.chain_id, dir.path());
        self.external
            .list(&full_path)
            .await
            .map_err(|error| CloudRetrievalError::ListError { dir: full_path, error })
    }

    /// Returns the block head from external storage, if present.
    pub async fn retrieve_cloud_block_head_if_exists(
        &self,
    ) -> Result<Option<BlockHeight>, CloudRetrievalError> {
        let file_id = CloudStorageFileID::BlockHead;
        let (_, filename) = self.location_dir_and_file(&file_id);
        if !self.dir_contains(&ListableCloudDir::Metadata, &filename).await? {
            return Ok(None);
        }
        self.retrieve(&file_id).await.map(Some)
    }

    /// Returns a shard head from external storage, if present.
    pub async fn retrieve_cloud_shard_head_if_exists(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<BlockHeight>, CloudRetrievalError> {
        let file_id = CloudStorageFileID::ShardHead(shard_id);
        let (_, filename) = self.location_dir_and_file(&file_id);
        if !self.dir_contains(&ListableCloudDir::ShardHeads, &filename).await? {
            return Ok(None);
        }
        self.retrieve(&file_id).await.map(Some)
    }

    /// Returns the state snapshot header from external storage.
    pub async fn retrieve_state_header(
        &self,
        epoch_height: EpochHeight,
        epoch_id: EpochId,
        shard_id: ShardId,
    ) -> Result<ShardStateSyncResponseHeader, CloudRetrievalError> {
        let file_id = CloudStorageFileID::StateHeader(epoch_height, epoch_id, shard_id);
        self.retrieve(&file_id).await
    }

    pub(super) async fn retrieve_epoch_data(
        &self,
        epoch_id: EpochId,
    ) -> Result<EpochData, CloudRetrievalError> {
        let file_id = CloudStorageFileID::Epoch(epoch_id);
        self.retrieve_compressed(&file_id).await
    }

    pub(super) async fn retrieve_block_batch(
        &self,
        batch_id: BatchId,
    ) -> Result<BlockBatch, CloudRetrievalError> {
        let file_id = CloudStorageFileID::BlockBatch(batch_id);
        let batch: BlockBatch = self.retrieve_compressed(&file_id).await?;
        batch
            .validate_blob()
            .map_err(|reason| CloudRetrievalError::InvalidFile { file_id, reason })?;
        Ok(batch)
    }

    pub(super) async fn retrieve_shard_batch(
        &self,
        shard_id: ShardId,
        batch_id: BatchId,
    ) -> Result<ShardBatch, CloudRetrievalError> {
        let file_id = CloudStorageFileID::ShardBatch(shard_id, batch_id);
        let batch: ShardBatch = self.retrieve_compressed(&file_id).await?;
        batch
            .validate_blob()
            .map_err(|reason| CloudRetrievalError::InvalidFile { file_id, reason })?;
        Ok(batch)
    }

    /// Downloads, decompresses, and deserializes a file from the cloud archive.
    // TODO(cloud_archival): Benchmark decompression: spawn_blocking,
    // multithreaded decompression for large shard blobs.
    pub(super) async fn retrieve_compressed<T: BorshDeserialize>(
        &self,
        file_id: &CloudStorageFileID,
    ) -> Result<T, CloudRetrievalError> {
        let compressed = self.download(file_id).await?;
        let bytes = zstd::decode_all(compressed.as_slice()).map_err(|error| {
            CloudRetrievalError::DecompressionError { file_id: file_id.clone(), error }
        })?;
        T::try_from_slice(&bytes).map_err(|error| CloudRetrievalError::DeserializeError {
            file_id: file_id.clone(),
            error,
        })
    }

    /// Downloads and deserializes a file from the cloud archive.
    pub(super) async fn retrieve<T: BorshDeserialize>(
        &self,
        file_id: &CloudStorageFileID,
    ) -> Result<T, CloudRetrievalError> {
        let bytes = self.download(file_id).await?;
        T::try_from_slice(&bytes).map_err(|error| CloudRetrievalError::DeserializeError {
            file_id: file_id.clone(),
            error,
        })
    }

    /// Downloads the raw bytes for a given file in the cloud archive.
    async fn download(&self, file_id: &CloudStorageFileID) -> Result<Vec<u8>, CloudRetrievalError> {
        let path = self.file_path(file_id);
        self.external
            .get(&path)
            .await
            .map_err(|error| CloudRetrievalError::GetError { file_id: file_id.clone(), error })
    }

    /// Checks if a directory contains a file with the given name.
    pub(super) async fn dir_contains(
        &self,
        dir: &ListableCloudDir,
        filename: &str,
    ) -> Result<bool, CloudRetrievalError> {
        let files = self.list_dir(dir).await?;
        Ok(files.iter().any(|f| f == filename))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::cloud_storage::bucket_config::BucketConfig;
    use near_external_storage::ExternalConnection;

    fn test_cloud_storage(root_dir: &std::path::Path) -> CloudStorage {
        CloudStorage::new(
            ExternalConnection::Filesystem { root_dir: root_dir.to_path_buf() },
            "test".to_string(),
            BucketConfig::canonical(),
        )
    }

    #[tokio::test]
    async fn test_list_dir_constructs_correct_path() {
        let tmp = tempfile::tempdir().unwrap();
        let cs = test_cloud_storage(tmp.path());

        let shard_dir = tmp.path().join("chain_id=test/archive/metadata/shard_head");
        std::fs::create_dir_all(&shard_dir).unwrap();
        std::fs::write(shard_dir.join("0"), b"").unwrap();
        std::fs::write(shard_dir.join("1"), b"").unwrap();

        let mut entries = cs.list_dir(&ListableCloudDir::ShardHeads).await.unwrap();
        entries.sort();
        assert_eq!(entries, vec!["0", "1"]);
    }

    #[tokio::test]
    async fn test_list_dir_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let cs = test_cloud_storage(tmp.path());

        let entries = cs.list_dir(&ListableCloudDir::ShardHeads).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_dir_contains() {
        let tmp = tempfile::tempdir().unwrap();
        let cs = test_cloud_storage(tmp.path());

        let meta_dir = tmp.path().join("chain_id=test/archive/metadata");
        std::fs::create_dir_all(&meta_dir).unwrap();
        std::fs::write(meta_dir.join("block_head"), b"").unwrap();

        assert!(cs.dir_contains(&ListableCloudDir::Metadata, "block_head").await.unwrap());
        assert!(!cs.dir_contains(&ListableCloudDir::Metadata, "nonexistent").await.unwrap());
    }
}
