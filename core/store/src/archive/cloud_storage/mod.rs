use crate::archive::cloud_storage::batch::compute_batch_id;
pub use crate::archive::cloud_storage::batch::{BatchId, BatchRange, compute_next_batch};
pub use crate::archive::cloud_storage::blocks::{BlockBatch, BlockData};
pub use crate::archive::cloud_storage::bucket_config::BucketConfig;
pub use crate::archive::cloud_storage::epoch_data::EpochData;
pub use crate::archive::cloud_storage::retrieve::CloudRetrievalError;
pub use crate::archive::cloud_storage::shards::{ShardBatch, ShardData};
use near_external_storage::ExternalConnection;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};

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
    ) -> Result<ShardStateSyncResponseHeader, CloudRetrievalError> {
        let fut = self.retrieve_state_header(epoch_height, epoch_id, shard_id);
        block_on_future(fut)
    }

    pub fn is_state_header_stored(
        &self,
        epoch_height: EpochHeight,
        epoch_id: EpochId,
        shard_id: ShardId,
    ) -> Result<bool, CloudRetrievalError> {
        let dir = ListableCloudDir::StateHeader { epoch_height, epoch_id, shard_id };
        block_on_future(self.dir_contains(&dir, "header"))
    }

    pub fn get_epoch_data(&self, epoch_id: EpochId) -> Result<EpochData, CloudRetrievalError> {
        block_on_future(self.retrieve_epoch_data(epoch_id))
    }

    /// Fetches the full block batch containing `block_height`. There is no
    /// single-block fetch on purpose, so callers cannot accidentally call one
    /// in a loop over consecutive heights.
    pub fn get_block_batch_for_height(
        &self,
        block_height: BlockHeight,
    ) -> Result<BlockBatch, CloudRetrievalError> {
        let batch_id = compute_batch_id(block_height, self.batch_size());
        let batch = block_on_future(self.retrieve_block_batch(batch_id))?;
        if block_height < batch.start_height() || block_height > batch.end_height() {
            // Batch is partial and doesn't cover the requested height (e.g. pre-writer-init).
            return Err(CloudRetrievalError::NoBlockData { height: block_height });
        }
        Ok(batch)
    }

    /// Fetches the full shard batch containing `block_height`. See
    /// `get_block_batch_for_height`.
    pub fn get_shard_batch_for_height(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<ShardBatch, CloudRetrievalError> {
        let batch_id = compute_batch_id(block_height, self.batch_size());
        let batch = block_on_future(self.retrieve_shard_batch(shard_id, batch_id))?;
        if block_height < batch.start_height() || block_height > batch.end_height() {
            // Batch is partial and doesn't cover the requested height (e.g. pre-resharding child).
            return Err(CloudRetrievalError::NoShardData { height: block_height, shard_id });
        }
        Ok(batch)
    }

    /// Test-only: fetch a single block's data. Production callers must go
    /// through `get_block_batch_for_height` so consecutive-height loops
    /// reuse the batch. Tests don't care about that cost. Returns `Ok(None)`
    /// when the height has no block (skipped slot).
    #[cfg(feature = "test_features")]
    pub fn get_block_data(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<BlockData>, CloudRetrievalError> {
        let batch = self.get_block_batch_for_height(block_height)?;
        Ok(batch.get_block_at_height(block_height).cloned())
    }

    /// Test-only: fetch a single shard's data. See `get_block_data`. Returns
    /// `Ok(None)` when the height has no block or this shard's chunk is not
    /// new at that height.
    #[cfg(feature = "test_features")]
    pub fn get_shard_data(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<Option<ShardData>, CloudRetrievalError> {
        let batch = self.get_shard_batch_for_height(block_height, shard_id)?;
        Ok(batch.get_shard_at_height(block_height).cloned())
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
    use crate::DBCol;
    use crate::archive::cloud_storage::bucket_config::BucketConfig;
    use near_external_storage::ExternalConnection;
    use strum::IntoEnumIterator;

    /// Cold columns whose data is carried by per-block-height batch blobs in cloud
    /// archive, either as a direct field of `BlockData` / `ShardData` or extracted
    /// from one of those at read time.
    const CLOUD_BATCH_COLUMNS: &[DBCol] = &[
        // Carried by BlockData (one entry per block).
        DBCol::Block,
        DBCol::BlockHeader,
        DBCol::BlockInfo,
        DBCol::NextBlockHashes,
        // Carried by ShardData (one entry per (block, shard)).
        DBCol::Chunks,
        DBCol::Transactions,
        DBCol::Receipts,
        DBCol::OutcomeIds,
        DBCol::TransactionResultForBlock,
        DBCol::ReceiptToTx,
        DBCol::IncomingReceipts,
        DBCol::OutgoingReceipts,
        DBCol::ChunkExtra,
        DBCol::ChunkApplyStats,
        DBCol::StateChanges,
    ];

    /// Cold columns intentionally not carried by batch blobs.
    const CLOUD_NON_BATCH_COLUMNS: &[DBCol] = &[
        // Reconstructed from BlockData by the reader at save time.
        DBCol::BlockPerHeight,
        // Reconstructed from BlockData by the reader at save time.
        DBCol::ChunkHashesByHeight,
        // Per-epoch state snapshots cover state, not per-block deltas.
        DBCol::State,
        // Per-epoch state snapshots cover the shard-layout mapping that keys state.
        DBCol::StateShardUIdMapping,
        // Uploaded as a separate StateHeader file per (epoch_height, shard_id).
        DBCol::StateHeaders,
        // Not GC-ed; cloud archive only handles GC-ed data.
        DBCol::StateChangesForSplitStates,
        // Spice cold columns - cloud-archive integration is a separate task.
        #[cfg(feature = "protocol_feature_spice")]
        DBCol::ReceiptProofs,
    ];

    /// Every cold column must be classified as either batch-carried or
    /// intentionally non-batch. A new cold column fails this test until
    /// classified with a rationale.
    #[test]
    fn every_cold_column_is_classified_for_cloud_archive() {
        for col in DBCol::iter() {
            if !col.is_cold() {
                continue;
            }
            assert!(
                CLOUD_BATCH_COLUMNS.contains(&col) ^ CLOUD_NON_BATCH_COLUMNS.contains(&col),
                "cold column {col:?} must be classified as batch or non-batch \
                 (with a rationale)"
            );
        }
    }

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
