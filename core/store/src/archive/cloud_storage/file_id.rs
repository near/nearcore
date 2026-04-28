use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::batch::BatchId;
use near_primitives::types::{EpochHeight, EpochId, ShardId};

/// Cloud directories that can be safely listed (recursively).
/// Unbounded directories (blocks/, shards/, epochs/) must not be listed.
#[derive(Clone, Copy, Debug)]
pub enum ListableCloudDir {
    Metadata,
    ShardHeads,
}

impl ListableCloudDir {
    pub fn path(&self) -> String {
        match self {
            Self::Metadata => "archive/metadata".into(),
            Self::ShardHeads => "archive/metadata/shard_head".into(),
        }
    }
}

/// Identifiers of files stored in cloud archival storage.
/// Each variant maps to a specific logical file within the archive.
#[derive(Clone, Debug)]
pub enum CloudStorageFileID {
    /// Archive-wide configuration (compression level, batch size, etc.).
    Config,
    /// Tracks the latest block height for which block data has been archived.
    BlockHead,
    /// Tracks the latest block height for which shard data has been archived
    /// for the given shard.
    ShardHead(ShardId),
    /// Identifier of the epoch file for the given epoch ID.
    Epoch(EpochId),
    /// Identifier of the block batch for the given batch ID.
    BlockBatch(BatchId),
    /// Identifier of the shard batch for the given shard and batch ID.
    ShardBatch(ShardId, BatchId),
    /// Identifier of the state snapshot header file for the given epoch and shard.
    StateHeader(EpochHeight, EpochId, ShardId),
}

impl CloudStorage {
    /// Returns the directory path and file name for the given file identifier.
    pub fn location_dir_and_file(&self, file_id: &CloudStorageFileID) -> (String, String) {
        let (mut dir_path, file_name) = match file_id {
            CloudStorageFileID::Config => (ListableCloudDir::Metadata.path(), "config".into()),
            CloudStorageFileID::BlockHead => {
                (ListableCloudDir::Metadata.path(), "block_head".into())
            }
            CloudStorageFileID::ShardHead(shard_id) => {
                (ListableCloudDir::ShardHeads.path(), format!("{shard_id}"))
            }
            CloudStorageFileID::Epoch(epoch_id) => {
                (format!("archive/epochs/epoch_id={}", epoch_id.0), "epoch_data".into())
            }
            CloudStorageFileID::BlockBatch(batch_id) => {
                (format!("archive/blocks/batch_id={}", batch_id.0), "data".into())
            }
            CloudStorageFileID::ShardBatch(shard_id, batch_id) => (
                format!("archive/shards/shard_id={shard_id}/batch_id={}", batch_id.0),
                "data".into(),
            ),
            CloudStorageFileID::StateHeader(epoch_height, epoch_id, shard_id) => (
                format!(
                    "epoch_height={}/epoch_id={}/headers/shard_id={}",
                    epoch_height, epoch_id.0, shard_id,
                ),
                "header".into(),
            ),
        };
        dir_path = format!("chain_id={}/{}", self.chain_id, dir_path);
        (dir_path, file_name)
    }

    /// Returns the full file path for the given file identifier.
    pub fn file_path(&self, file_id: &CloudStorageFileID) -> String {
        let (location_dir, file_name) = self.location_dir_and_file(file_id);
        format!("{}/{}", location_dir, file_name)
    }
}
