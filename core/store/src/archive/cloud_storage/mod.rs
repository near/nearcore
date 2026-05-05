use crate::archive::cloud_storage::batch::compute_batch_id;
pub use crate::archive::cloud_storage::batch::{BatchId, BatchRange, compute_next_batch};
pub use crate::archive::cloud_storage::blocks::{BlockBatch, BlockData};
pub use crate::archive::cloud_storage::bucket_config::BucketConfig;
pub use crate::archive::cloud_storage::epoch_data::EpochData;
pub use crate::archive::cloud_storage::shards::{ShardBatch, ShardData};
use near_external_storage::ExternalConnection;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};
use std::io::{Error, Result};

pub mod config;
pub mod opener;

pub mod archive;
pub mod bucket_config;
pub mod retrieve;

pub(super) mod batch;
pub(super) mod blocks;
pub(super) mod epoch_data;
pub(super) mod file_id;
pub(super) mod shards;

pub use file_id::ListableCloudDir;

/// Handles operations related to cloud storage used for archival data.
pub struct CloudStorage {
    /// Connection to the external storage backend (e.g. S3, GCS, filesystem).
    external: ExternalConnection,
    chain_id: String,
    bucket_config: BucketConfig,
}

impl CloudStorage {
    pub fn new(
        external: ExternalConnection,
        chain_id: String,
        bucket_config: BucketConfig,
    ) -> Self {
        Self { external, chain_id, bucket_config }
    }

    pub fn connection(&self) -> &ExternalConnection {
        &self.external
    }

    pub fn chain_id(&self) -> &str {
        &self.chain_id
    }

    pub fn batch_size(&self) -> u32 {
        self.bucket_config.batch_size()
    }

    pub fn get_state_header(
        &self,
        epoch_height: EpochHeight,
        epoch_id: EpochId,
        shard_id: ShardId,
    ) -> Result<ShardStateSyncResponseHeader> {
        let fut = self.retrieve_state_header(epoch_height, epoch_id, shard_id);
        let state_header = block_on_future(fut).map_err(Error::other)?;
        Ok(state_header)
    }

    pub fn is_state_header_stored(
        &self,
        epoch_height: EpochHeight,
        epoch_id: EpochId,
        shard_id: ShardId,
    ) -> Result<bool> {
        let dir = ListableCloudDir::StateHeader { epoch_height, epoch_id, shard_id };
        block_on_future(self.dir_contains(&dir, "header")).map_err(Error::other)
    }

    pub fn get_epoch_data(&self, epoch_id: EpochId) -> Result<EpochData> {
        let epoch_data =
            block_on_future(self.retrieve_epoch_data(epoch_id)).map_err(Error::other)?;
        Ok(epoch_data)
    }

    /// Fetches the full block batch containing `block_height`. There is no
    /// single-block fetch on purpose, so callers cannot accidentally call one
    /// in a loop over consecutive heights.
    pub fn get_block_batch_for_height(&self, block_height: BlockHeight) -> Result<BlockBatch> {
        let batch_id = compute_batch_id(block_height, self.batch_size());
        block_on_future(self.retrieve_block_batch(batch_id)).map_err(Error::other)
    }

    /// Fetches the full shard batch containing `block_height`. See
    /// `get_block_batch_for_height`.
    pub fn get_shard_batch_for_height(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<ShardBatch> {
        let batch_id = compute_batch_id(block_height, self.batch_size());
        block_on_future(self.retrieve_shard_batch(shard_id, batch_id)).map_err(Error::other)
    }

    /// Test-only: fetch a single block's data. Production callers must go
    /// through `get_block_batch_for_height` so consecutive-height loops
    /// reuse the batch. Tests don't care about that cost.
    #[cfg(feature = "test_features")]
    pub fn get_block_data(&self, block_height: BlockHeight) -> Result<BlockData> {
        let batch = self.get_block_batch_for_height(block_height)?;
        Ok(batch.get_block_at_height(block_height).clone())
    }

    /// Test-only: fetch a single shard's data. See `get_block_data`.
    #[cfg(feature = "test_features")]
    pub fn get_shard_data(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<ShardData> {
        let batch = self.get_shard_batch_for_height(block_height, shard_id)?;
        Ok(batch.get_shard_at_height(block_height).clone())
    }
}

// TODO(cloud_archival): This is a temporary solution for development.
// Ensure the final implementation does not negatively impact or crash the application.
fn block_on_future<F: Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}

#[cfg(test)]
mod tests {
    use super::CloudStorage;
    use super::batch::BatchId;
    use super::file_id::CloudStorageFileID;
    use crate::archive::cloud_storage::bucket_config::BucketConfig;
    use near_external_storage::ExternalConnection;

    pub fn test_cloud_storage(tmp_dir: &tempfile::TempDir) -> CloudStorage {
        CloudStorage::new(
            ExternalConnection::Filesystem { root_dir: tmp_dir.path().to_path_buf() },
            "test".to_string(),
            BucketConfig::canonical(),
        )
    }

    #[tokio::test]
    async fn data_blobs_are_compressed() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cloud_storage = test_cloud_storage(&tmp_dir);
        let payload: u64 = 42;
        let original = borsh::to_vec(&payload).unwrap();
        let file_id = CloudStorageFileID::BlockBatch(BatchId(0));
        cloud_storage.upload_compressed(file_id.clone(), original.clone()).await.unwrap();

        // Read raw bytes from the filesystem to verify they are compressed.
        let raw_path = tmp_dir.path().join(cloud_storage.file_path(&file_id));
        let raw_bytes = std::fs::read(&raw_path).unwrap();
        assert_ne!(raw_bytes, original, "blob should be compressed, not raw borsh");

        // Verify retrieve_compressed round-trips correctly.
        let retrieved: u64 = cloud_storage.retrieve_compressed(&file_id).await.unwrap();
        assert_eq!(retrieved, payload);
    }
}
