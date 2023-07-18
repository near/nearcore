use crate::store::ChainStoreAccess;
use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;

/// Check that epoch of block with given prev_block_hash is the first one with current protocol version.
fn is_first_epoch_with_protocol_version(
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block_hash: &CryptoHash,
) -> Result<bool, Error> {
    tracing::error!(?prev_block_hash, "is_first_epoch_with_protocol_version $1");
    let prev_epoch_id = epoch_manager.get_prev_epoch_id_from_prev_block(prev_block_hash)?;
    tracing::error!(?prev_block_hash, ?prev_epoch_id, "is_first_epoch_with_protocol_version $2");
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
    tracing::error!(?prev_block_hash, ?epoch_id, "is_first_epoch_with_protocol_version $3");
    let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
    tracing::error!(?prev_block_hash, ?protocol_version, "is_first_epoch_with_protocol_version $4");
    let prev_epoch_protocol_version = epoch_manager.get_epoch_protocol_version(&prev_epoch_id)?;
    tracing::error!(?prev_block_hash, ?prev_epoch_protocol_version, "is_first_epoch_with_protocol_version $5");
    Ok(protocol_version != prev_epoch_protocol_version)
}

/// Check that block is the first one with existing chunk for the given shard in the chain with its protocol version.
/// We assume that current block contain the chunk for shard with the given id.
pub fn check_if_block_is_first_with_chunk_of_version(
    chain_store: &dyn ChainStoreAccess,
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<bool, Error> {
    tracing::error!(?prev_block_hash, ?shard_id, "check_if_block_is_first_with_chunk_of_version #1");
    // Check that block belongs to the first epoch with current protocol version
    // to avoid get_epoch_id_of_last_block_with_chunk call in the opposite case
    if is_first_epoch_with_protocol_version(epoch_manager, prev_block_hash)? {
        tracing::error!(?prev_block_hash, ?shard_id, "check_if_block_is_first_with_chunk_of_version #2");
        // Compare only epochs because we already know that current epoch is the first one with current protocol version
        // convert shard id to shard id of previous epoch because number of shards may change
        let shard_id = epoch_manager.get_prev_shard_ids(prev_block_hash, vec![shard_id])?[0];
        tracing::error!(?prev_block_hash, ?shard_id, "check_if_block_is_first_with_chunk_of_version #3");
        let prev_epoch_id = chain_store.get_epoch_id_of_last_block_with_chunk(
            epoch_manager,
            prev_block_hash,
            shard_id,
        )?;
        tracing::error!(?prev_block_hash, ?shard_id, "check_if_block_is_first_with_chunk_of_version #4");
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        tracing::error!(?prev_block_hash, ?shard_id, "check_if_block_is_first_with_chunk_of_version #5");
        Ok(prev_epoch_id != epoch_id)
    } else {
        tracing::error!(?prev_block_hash, ?shard_id, "check_if_block_is_first_with_chunk_of_version #6");
        Ok(false)
    }
}
