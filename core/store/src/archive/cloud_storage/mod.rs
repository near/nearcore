use crate::DBCol;
use crate::archive::cloud_storage::batch::compute_batch_id;
pub use crate::archive::cloud_storage::batch::{BatchId, BatchRange, compute_next_batch};
pub use crate::archive::cloud_storage::blocks::{BlockBatch, BlockData, read_chunk_hashes};
pub use crate::archive::cloud_storage::bucket_config::BucketConfig;
pub use crate::archive::cloud_storage::epoch_data::EpochData;
pub use crate::archive::cloud_storage::retrieve::CloudRetrievalError;
pub use crate::archive::cloud_storage::shards::{
    InverseStateChanges, NewChunkData, ShardBatch, ShardData,
};
use near_external_storage::ExternalConnection;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};

pub mod config;
pub mod opener;

pub mod archive;
pub mod bucket_config;
pub mod metrics;
pub mod retrieve;
#[cfg(feature = "test_features")]
pub mod test_utils;

pub(super) mod batch;
pub(super) mod blocks;
pub(super) mod epoch_data;
pub(super) mod file_id;
pub(super) mod shards;

pub use file_id::{CloudStorageFileID, ListableCloudDir};

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

    pub fn batch_size(&self) -> u32 {
        self.bucket_config.batch_size()
    }

    pub async fn is_state_header_stored(
        &self,
        epoch_height: EpochHeight,
        epoch_id: EpochId,
        shard_id: ShardId,
    ) -> Result<bool, CloudRetrievalError> {
        let dir = ListableCloudDir::StateHeader { epoch_height, epoch_id, shard_id };
        self.dir_contains(&dir, "header").await
    }

    pub async fn get_epoch_data(
        &self,
        epoch_id: EpochId,
    ) -> Result<EpochData, CloudRetrievalError> {
        self.retrieve_epoch_data(epoch_id).await
    }

    /// Highest height whose block data is in the bucket, if the writer has
    /// published a block head at all.
    pub async fn get_cloud_block_head(&self) -> Result<Option<BlockHeight>, CloudRetrievalError> {
        self.retrieve_cloud_block_head_if_exists().await
    }

    /// Fetches the full block batch containing `block_height`. There is no
    /// single-block fetch on purpose, so callers cannot accidentally call one
    /// in a loop over consecutive heights.
    pub async fn get_block_batch_for_height(
        &self,
        block_height: BlockHeight,
    ) -> Result<BlockBatch, CloudRetrievalError> {
        let batch_id = compute_batch_id(block_height, self.batch_size());
        let batch = self.retrieve_block_batch(batch_id).await?;
        if block_height < batch.start_height() || block_height > batch.end_height() {
            // Batch is partial and doesn't cover the requested height (e.g. pre-writer-init).
            return Err(CloudRetrievalError::NoBlockData { height: block_height });
        }
        Ok(batch)
    }

    /// Highest height whose data for `shard_id` is in the bucket, if the writer has
    /// published a head for that shard at all.
    pub async fn get_cloud_shard_head(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<BlockHeight>, CloudRetrievalError> {
        self.retrieve_cloud_shard_head_if_exists(shard_id).await
    }

    /// Fetches `shard_id`'s batch for the window `block_height` falls in. What the batch
    /// carries can open above or end below that height, which is what a shard added or
    /// retired by a resharding looks like.
    pub async fn get_shard_batch(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<ShardBatch, CloudRetrievalError> {
        let batch_id = compute_batch_id(block_height, self.batch_size());
        self.retrieve_shard_batch(shard_id, batch_id).await
    }
}

/// Columns the cloud-archive reader reproduces from cloud data.
pub fn is_cloud_archive_reader_bootstrapped(col: DBCol) -> bool {
    matches!(
        col,
        // From BlockData.
        DBCol::Block
            | DBCol::BlockInfo
            | DBCol::NextBlockHashes
            | DBCol::BlockMerkleTree
            | DBCol::ChunkProducers
            // Reconstructed from BlockData.
            | DBCol::BlockHeader
            | DBCol::BlockHeight
            | DBCol::BlockOrdinal
            | DBCol::BlockPerHeight
            | DBCol::ChunkHashesByHeight

            // From ShardData.
            | DBCol::Chunks
            | DBCol::OutcomeIds
            | DBCol::TransactionResultForBlock
            | DBCol::ReceiptToTx
            | DBCol::IncomingReceipts
            | DBCol::OutgoingReceipts
            | DBCol::ProcessedReceiptIds
            | DBCol::ChunkExtra
            | DBCol::ChunkApplyStats
            | DBCol::StateChanges
            // Reconstructed from ShardData.
            | DBCol::Receipts
            | DBCol::Transactions

            // From EpochData.
            | DBCol::EpochInfo
            | DBCol::EpochStart

            // From a state snapshot.
            | DBCol::State
    )
}

/// Columns the cloud-archive reader does not reproduce.
#[cfg(test)]
fn is_cloud_archive_reader_skipped(col: DBCol) -> bool {
    // TODO(spice): decide how the reader handles spice columns.
    #[cfg(feature = "protocol_feature_spice")]
    if col == DBCol::ReceiptProofs || col == DBCol::SpiceInvalidChunks {
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

            // Block-keyed, and every one of them is read by something a reader's store
            // never runs: garbage collection, which a reader refuses, block processing,
            // which a reader does not do, state sync, and one debug-UI view. Listed so
            // the block-level set is settled rather than left to be re-derived.
            | DBCol::BlockRefCount
            | DBCol::BlocksToCatchup
            | DBCol::ChallengedBlocks
            | DBCol::HeaderHashesByHeight
            | DBCol::ProcessedBlockHeights
            | DBCol::StateDlInfos
            | DBCol::StateSyncNewChunks

            // Read only by epoch sync and one migration, and a reader is refused by a
            // running node, so nothing that reads this column runs against its store.
            | DBCol::EpochSyncProof

            // TODO(cloud_archival): `next_light_client_block` and `validators` read these
            // two, so reproduce them; the second needs a field on `EpochData`.
            | DBCol::EpochLightClientBlocks
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
