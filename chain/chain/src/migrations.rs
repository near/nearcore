use crate::store::ChainStoreAccess;
use crate::types::RuntimeAdapter;
use near_chain_primitives::error::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;

/// Check that block is the first one with existing chunk for the given shard in the chain with its protocol version.
/// We assume that current block contain the chunk for shard with the given id.
pub fn check_if_block_is_first_with_chunk_of_version(
    chain_store: &mut dyn ChainStoreAccess,
    runtime_adapter: &dyn RuntimeAdapter,
    prev_block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<bool, Error> {
    // At first, check that shard id = 0 and don't do unnecessary computation otherwise
    if shard_id != 0 {
        return Ok(false);
    }

    let epoch_id = runtime_adapter.get_epoch_id_from_prev_block(prev_block_hash)?;
    let protocol_version = runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
    let prev_epoch_id = match runtime_adapter.get_prev_epoch_id_from_prev_block(prev_block_hash) {
        Ok(epoch_id) => epoch_id,
        _ => {
            return Ok(false);
        }
    };
    let prev_epoch_protocol_version = runtime_adapter.get_epoch_protocol_version(&prev_epoch_id)?;
    // Check that block belongs to the first epoch with current protocol version
    // to avoid get_epoch_id_of_last_block_with_chunk call in the opposite case
    if protocol_version != prev_epoch_protocol_version {
        // Compare only epochs because we already know that current epoch is the first one with current protocol version
        Ok(chain_store.get_epoch_id_of_last_block_with_chunk(prev_block_hash, shard_id)?
            != epoch_id)
    } else {
        Ok(false)
    }
}
