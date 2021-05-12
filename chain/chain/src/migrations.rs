use crate::store::ChainStoreAccess;
use crate::types::RuntimeAdapter;
use near_chain_primitives::error::Error;
use near_primitives::checked_feature;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{EpochId, ShardId};

/// Get epoch id of the last block in which chunk for the given shard was presented.
fn get_epoch_id_of_last_block_with_chunk(
    chain_store: &mut dyn ChainStoreAccess,
    hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<EpochId, Error> {
    let mut candidate_hash = hash.clone();
    loop {
        let block_header = chain_store.get_block_header(&candidate_hash)?.clone();
        if block_header.chunk_mask()[shard_id as usize] {
            break Ok(block_header.epoch_id().clone());
        }
        candidate_hash = block_header.prev_hash().clone();
    }
}

/// Get epoch id by hash of previous block.
fn get_prev_epoch_id_from_prev_block(
    chain_store: &mut dyn ChainStoreAccess,
    runtime_adapter: &dyn RuntimeAdapter,
    block_hash: &CryptoHash,
) -> Result<EpochId, Error> {
    let epoch_start_header = {
        let height = runtime_adapter.get_epoch_start_height(block_hash)?;
        let hash = chain_store.get_block_hash_by_height(height)?;
        chain_store.get_block_header(&hash)?.clone()
    };
    if runtime_adapter.is_next_block_epoch_start(block_hash).unwrap() {
        runtime_adapter.get_epoch_id_from_prev_block(epoch_start_header.prev_hash())
    } else {
        let prev_epoch_last_block =
            chain_store.get_previous_header(&epoch_start_header)?.prev_hash().clone();
        runtime_adapter.get_epoch_id_from_prev_block(&prev_epoch_last_block)
    }
}

/// We take the first block with existing chunk in the first epoch in which protocol feature
/// RestoreReceiptsAfterFix was enabled, and put the restored receipts there.
/// Needed to re-introduce receipts previously lost in apply_chunks (see https://github.com/near/nearcore/pull/4248/).
pub fn check_if_block_is_valid_for_migration(
    chain_store: &mut dyn ChainStoreAccess,
    runtime_adapter: &dyn RuntimeAdapter,
    block_hash: &CryptoHash,
    prev_block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<bool, Error> {
    let block_header = chain_store.get_block_header(block_hash)?.clone();
    let protocol_version = runtime_adapter.get_epoch_protocol_version(block_header.epoch_id())?;
    let prev_epoch_id =
        get_prev_epoch_id_from_prev_block(chain_store, runtime_adapter, prev_block_hash)?.clone();
    let prev_epoch_protocol_version = runtime_adapter.get_epoch_protocol_version(&prev_epoch_id)?;
    // At first, check that block belongs to the first epoch where the protocol feature was enabled
    // to avoid get_epoch_id_of_last_block_with_chunk call in the opposite case
    if checked_feature!(
        "protocol_feature_restore_receipts_after_fix",
        RestoreReceiptsAfterFix,
        protocol_version
    ) && !checked_feature!(
        "protocol_feature_restore_receipts_after_fix",
        RestoreReceiptsAfterFix,
        prev_epoch_protocol_version
    ) {
        let prev_protocol_version = runtime_adapter.get_epoch_protocol_version(
            &get_epoch_id_of_last_block_with_chunk(chain_store, prev_block_hash, shard_id)?,
        )?;
        Ok(shard_id == 0
            && !checked_feature!(
                "protocol_feature_restore_receipts_after_fix",
                RestoreReceiptsAfterFix,
                prev_protocol_version
            ))
    } else {
        Ok(false)
    }
}
