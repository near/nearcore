mod fetch;
mod serve;

use crate::spice::data_manager::DataId;
pub(super) use fetch::ReceiptProofFetchPolicy;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::AccountId;
use near_store::adapter::chain_store::ChainStoreAdapter;
pub(super) use serve::ReceiptProofServePolicy;

/// The source chunk's producers.
// TODO(spice-resharding): validate whether the source and destination shards are correct
// when resharding.
fn source_producers(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    id: &DataId,
) -> Result<Vec<AccountId>, Error> {
    let DataId::ReceiptProof { source, .. } = id;
    let header = chain_store.get_block_header(&source.block_hash)?;
    Ok(epoch_manager.get_epoch_chunk_producers_for_shard(header.epoch_id(), source.shard_id)?)
}
