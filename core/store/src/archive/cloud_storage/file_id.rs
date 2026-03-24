use crate::archive::cloud_storage::CloudStorage;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};

/// Directories that can be safely listed (recursively).
#[derive(Clone, Debug)]
pub enum CloudDir {
    Metadata,
    ShardHeads,
}

impl CloudDir {
    pub fn path(&self) -> String {
        match self {
            Self::Metadata => "metadata".into(),
            Self::ShardHeads => "metadata/shard_head".into(),
        }
    }
}

/// Identifiers of files stored in cloud archival storage.
/// Each variant maps to a specific logical file within the archive.
#[derive(Clone, Debug)]
pub enum CloudStorageFileID {
    /// Archive-wide configuration (compression level, etc.).
    Config,
    /// Tracks the latest block height for which block data has been archived.
    BlockHead,
    /// Tracks the latest block height for which shard data has been archived
    /// for the given shard.
    ShardHead(ShardId),
    /// Identifier of the epoch file for the given epoch ID.
    Epoch(EpochId),
    /// Identifier of the block file for the given block height.
    Block(BlockHeight),
    /// Identifier of the shard file for the given block height and shard.
    Shard(BlockHeight, ShardId),
    /// Identifier of the state snapshot header file for the given epoch and shard.
    StateHeader(EpochHeight, EpochId, ShardId),
}

impl CloudStorage {
    /// Returns the directory path and file name for the given file identifier.
    pub fn location_dir_and_file(&self, file_id: &CloudStorageFileID) -> (String, String) {
        let (mut dir_path, file_name) = match file_id {
            CloudStorageFileID::Config => (CloudDir::Metadata.path(), "config".into()),
            CloudStorageFileID::BlockHead => (CloudDir::Metadata.path(), "block_head".into()),
            CloudStorageFileID::ShardHead(shard_id) => {
                (CloudDir::ShardHeads.path(), format!("{shard_id}"))
            }
            CloudStorageFileID::Epoch(epoch_id) => {
                (format!("epochs/epoch_id={}", epoch_id.0), "epoch_data".into())
            }
            CloudStorageFileID::Block(height) => {
                (format!("heights/block_height={height}"), "block_data".into())
            }
            CloudStorageFileID::Shard(height, shard_id) => {
                (format!("heights/block_height={height}/shard_id={shard_id}"), "shard_data".into())
            }
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
