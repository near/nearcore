use crate::spice::chunk_executor_actor::get_witness;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::types::{AccountId, SpiceChunkId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashSet;
use std::sync::Arc;

/// State witnesses on a producer: pushed to the chunk's validators and served from the store.
pub(crate) struct WitnessServePolicy {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
}

impl WitnessServePolicy {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self { chain_store, epoch_manager }
    }

    /// The chunk validators assigned to the chunk.
    pub(in crate::spice::data_manager::policy) fn recipients(
        &self,
        chunk_id: &SpiceChunkId,
    ) -> Result<HashSet<AccountId>, Error> {
        let header = self.chain_store.get_block_header(&chunk_id.block_hash)?;
        let assignments = self.epoch_manager.get_chunk_validator_assignments(
            header.epoch_id(),
            chunk_id.shard_id,
            header.height(),
        )?;
        Ok(assignments.ordered_chunk_validators().into_iter().collect())
    }

    /// The witness, if this node has it in the store.
    pub(in crate::spice::data_manager::policy) fn stored_data(
        &self,
        chunk_id: &SpiceChunkId,
    ) -> Option<SpiceChunkStateWitness> {
        get_witness(self.chain_store.store_ref(), &chunk_id.block_hash, chunk_id.shard_id)
    }
}
