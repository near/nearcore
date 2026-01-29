use std::sync::Arc;

use near_chain_primitives::Error;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::version::ProtocolFeature;
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;

use near_epoch_manager::shard_tracker::ShardTracker;

#[derive(Clone)]
pub struct SpiceChainReader {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
}

impl SpiceChainReader {
    pub fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
    ) -> Self {
        Self { chain_store, epoch_manager, shard_tracker }
    }

    /// Checks if a block has been executed by looking for chunk_extra on any
    /// tracked shard. Non-spice blocks are always considered executed since
    /// execution is synchronous with block processing.
    pub fn is_block_executed(&self, header: &BlockHeader) -> Result<bool, Error> {
        let epoch_id = header.epoch_id();
        let protocol_version =
            self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
        if !ProtocolFeature::Spice.enabled(protocol_version) {
            return Ok(true);
        }
        let shard_ids = self.epoch_manager.shard_ids(epoch_id)?;
        for shard_id in shard_ids {
            if !self.shard_tracker.cares_about_shard(header.hash(), shard_id) {
                continue;
            }
            let shard_uid =
                shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, epoch_id)?;
            match self.chain_store.chunk_store().get_chunk_extra(header.hash(), &shard_uid) {
                Ok(_) => return Ok(true),
                Err(Error::DBNotFoundErr(_)) => return Ok(false),
                Err(err) => return Err(err),
            }
        }
        Ok(false)
    }

    /// For spice, returns an error if the block has not been executed yet.
    /// Non-spice blocks always pass.
    pub fn check_block_executed(&self, header: &BlockHeader) -> Result<(), Error> {
        if !self.is_block_executed(header)? {
            return Err(Error::DBNotFoundErr(format!(
                "block {} not yet executed",
                header.hash()
            )));
        }
        Ok(())
    }

    /// Finds the first executed block walking back from `start_hash`.
    pub fn find_first_executed_ancestor(
        &self,
        start_hash: &CryptoHash,
    ) -> Result<Arc<BlockHeader>, Error> {
        let final_execution_head = self.chain_store.spice_final_execution_head().ok();
        let mut header = self.chain_store.get_block_header(start_hash)?;
        loop {
            if self.is_block_executed(&header)? {
                return Ok(header);
            }
            if header.is_genesis() {
                return Ok(header);
            }
            if let Some(final_execution_head) = &final_execution_head {
                if header.height() <= final_execution_head.height {
                    return Err(Error::DBNotFoundErr(
                        "no executed ancestor found".to_string(),
                    ));
                }
            }
            header = self.chain_store.get_block_header(header.prev_hash())?;
        }
    }
}
