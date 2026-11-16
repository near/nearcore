use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::version::ProtocolFeature;
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::sync::Arc;

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

    /// Checks if a block has been executed by looking for chunk_extra on every
    /// shard tracked this epoch, so a node tracking no shards treats every block
    /// as executed. Non-spice blocks are always considered executed since
    /// execution is synchronous with block processing.
    fn is_block_executed(&self, header: &BlockHeader) -> Result<bool, Error> {
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(header.epoch_id())?;
        if !ProtocolFeature::Spice.enabled(protocol_version) || header.is_genesis() {
            return Ok(true);
        }
        let chunk_store = self.chain_store.chunk_store();
        for shard_uid in self.shard_tracker.tracked_shard_uids(header.prev_hash())? {
            match chunk_store.get_chunk_extra(header.hash(), &shard_uid) {
                Ok(_) => {}
                Err(Error::DBNotFoundErr(_)) => return Ok(false),
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    /// For spice, returns an error if the block has not been executed yet.
    /// Non-spice blocks always pass.
    pub fn check_block_executed(&self, header: &BlockHeader) -> Result<(), Error> {
        if !self.is_block_executed(header)? {
            return Err(Error::DBNotFoundErr(format!("block {} not yet executed", header.hash())));
        }
        Ok(())
    }

    /// Finds the first executed block walking back from `start_hash`.
    pub fn find_first_executed_ancestor(
        &self,
        start_hash: &CryptoHash,
    ) -> Result<Arc<BlockHeader>, Error> {
        let final_execution_head = self.chain_store.spice_final_execution_head()?;
        let mut header = self.chain_store.get_block_header(start_hash)?;
        loop {
            if self.is_block_executed(&header)? {
                return Ok(header);
            }
            if header.is_genesis() {
                return Ok(header);
            }
            if header.height() <= final_execution_head.height {
                return Err(Error::DBNotFoundErr("no executed ancestor found".to_string()));
            }
            header = self.chain_store.get_block_header(header.prev_hash())?;
        }
    }
}
