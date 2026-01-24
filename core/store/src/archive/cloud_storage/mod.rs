use std::io::{Error, Result};

use near_external_storage::ExternalConnection;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};

use crate::archive::cloud_storage::block_data::BlockData;
use crate::archive::cloud_storage::epoch_data::EpochData;
use crate::archive::cloud_storage::shard_data::ShardData;

pub mod config;
pub mod opener;

pub mod archive;
pub mod retrieve;

pub(super) mod block_data;
pub(super) mod epoch_data;
pub(super) mod file_id;
pub(super) mod shard_data;

/// Handles operations related to cloud storage used for archival data.
pub struct CloudStorage {
    /// Connection to the external storage backend (e.g. S3, GCS, filesystem).
    external: ExternalConnection,
    chain_id: String,
}

impl CloudStorage {
    pub fn connection(&self) -> &ExternalConnection {
        &self.external
    }

    pub fn chain_id(&self) -> &String {
        &self.chain_id
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

    pub fn get_epoch_data(&self, epoch_id: EpochId) -> Result<EpochData> {
        let epoch_data =
            block_on_future(self.retrieve_epoch_data(epoch_id)).map_err(Error::other)?;
        Ok(epoch_data)
    }

    pub fn get_block_data(&self, block_height: BlockHeight) -> Result<BlockData> {
        let block_data =
            block_on_future(self.retrieve_block_data(block_height)).map_err(Error::other)?;
        Ok(block_data)
    }

    pub fn get_shard_data(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<ShardData> {
        let shard_data = block_on_future(self.retrieve_shard_data(block_height, shard_id))
            .map_err(Error::other)?;
        Ok(shard_data)
    }
}

// TODO(cloud_archival): This is a temporary solution for development.
// Ensure the final implementation does not negatively impact or crash the application.
fn block_on_future<F: Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}
