use crate::DBCol;
use crate::archive::cloud_storage::batch::compute_batch_id;
pub use crate::archive::cloud_storage::batch::{BatchId, BatchRange, compute_next_batch};
pub use crate::archive::cloud_storage::blocks::{BlockBatch, BlockData};
pub use crate::archive::cloud_storage::bucket_config::BucketConfig;
pub use crate::archive::cloud_storage::epoch_data::EpochData;
pub use crate::archive::cloud_storage::retrieve::CloudRetrievalError;
pub use crate::archive::cloud_storage::shards::{InverseStateChanges, ShardBatch, ShardData};
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
    /// `Ok(None)` when the height has no block.
    #[cfg(feature = "test_features")]
    pub fn get_shard_data(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<Option<ShardData>, CloudRetrievalError> {
        let batch = self.get_shard_batch_for_height(block_height, shard_id)?;
        Ok(batch.get_data_at_height(block_height).cloned())
    }
}

// TODO(cloud_archival): This is a temporary solution for development.
// Ensure the final implementation does not negatively impact or crash the application.
fn block_on_future<F: Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}

/// Columns the cloud-archive reader reproduces from cloud data.
pub fn is_cloud_archive_reader_bootstrapped(col: DBCol) -> bool {
    // From BlockData.
    #[cfg(feature = "nightly")]
    if col == DBCol::ChunkProducers {
        return true;
    }
    matches!(
        col,
        // From BlockData.
        DBCol::Block
            | DBCol::BlockInfo
            | DBCol::NextBlockHashes
            // Reconstructed from BlockData.
            | DBCol::BlockHeader
            | DBCol::BlockHeight
            // TODO(cloud_archival): reconstruct from BlockHeight in the reader.
            | DBCol::BlockPerHeight
            // TODO(cloud_archival): reconstruct from Block in the reader.
            | DBCol::ChunkHashesByHeight

            // From ShardData.
            | DBCol::Chunks
            | DBCol::OutcomeIds
            | DBCol::TransactionResultForBlock
            | DBCol::ReceiptToTx
            | DBCol::IncomingReceipts
            | DBCol::OutgoingReceipts
            | DBCol::ChunkExtra
            | DBCol::ChunkApplyStats
            | DBCol::StateChanges
            // Reconstructed from ShardData.
            | DBCol::Receipts
            | DBCol::Transactions

            // From EpochData.
            | DBCol::EpochInfo
            | DBCol::EpochStart
            | DBCol::BlockMerkleTree

            // From a state snapshot.
            | DBCol::State
    )
}

/// Columns the cloud-archive reader does not reproduce.
#[cfg(test)]
fn is_cloud_archive_reader_skipped(col: DBCol) -> bool {
    // TODO(spice): decide how the reader handles spice columns.
    #[cfg(feature = "protocol_feature_spice")]
    if col == DBCol::ReceiptProofs {
        return true;
    }
    matches!(
        col,
        // DB-level metadata; the reader maintains its own.
        DBCol::DbVersion
            | DBCol::BlockMisc
            // State-sync header, used only transiently for State bootstrap, not persisted.
            | DBCol::StateHeaders
            // Resharding bookkeeping; the reader bootstraps State from per-epoch snapshots instead.
            | DBCol::StateChangesForSplitStates
            | DBCol::StateShardUIdMapping

            // TODO(cloud_archival): the reader may need the following for validator /
            // light-client / block-ordinal / epoch-sync queries; confirm, and reproduce if so.
            | DBCol::BlockOrdinal
            | DBCol::EpochLightClientBlocks
            | DBCol::EpochSyncProof
            | DBCol::EpochValidatorInfo
    )
}

#[cfg(test)]
mod tests {
    use super::CloudStorage;
    use super::batch::BatchId;
    use super::file_id::CloudStorageFileID;
    use crate::archive::cloud_storage::bucket_config::BucketConfig;
    use crate::archive::cloud_storage::{
        is_cloud_archive_reader_bootstrapped, is_cloud_archive_reader_skipped,
    };
    use crate::{DBCol, GcPolicy};
    use near_external_storage::ExternalConnection;
    use strum::IntoEnumIterator;

    /// A cloud-bootstrapped reader must reproduce every column an archival node
    /// keeps long-term, i.e. `is_in_colddb() || gc_policy() == GcPolicy::Permanent`.
    /// Each such column must be in exactly one of the two predicates above, so a new
    /// one fails this test until a cloud-archive decision is made.
    #[test]
    fn every_retained_column_is_classified_for_cloud_archive() {
        for col in DBCol::iter() {
            let retained = col.is_in_colddb() || matches!(col.gc_policy(), GcPolicy::Permanent);
            if !retained {
                continue;
            }
            let categories =
                [is_cloud_archive_reader_bootstrapped(col), is_cloud_archive_reader_skipped(col)];
            assert_eq!(
                categories.iter().filter(|x| **x).count(),
                1,
                "retained column {col:?} must be in exactly one of \
                 is_cloud_archive_reader_bootstrapped / is_cloud_archive_reader_skipped",
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
