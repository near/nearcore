use crate::store::ChainStoreAccess;
use crate::types::RuntimeAdapter;
use near_chain_primitives::error::Error;
use near_primitives::checked_feature;
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;

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
    // At first, check that shard id = 0 and don't do unnecessary computation otherwise
    if shard_id != 0 {
        return Ok(false);
    }

    let block_header = chain_store.get_block_header(block_hash)?.clone();
    let protocol_version = runtime_adapter.get_epoch_protocol_version(block_header.epoch_id())?;
    let prev_epoch_id = runtime_adapter.get_prev_epoch_id_from_prev_block(prev_block_hash)?;
    let prev_epoch_protocol_version = runtime_adapter.get_epoch_protocol_version(&prev_epoch_id)?;
    // Check that block belongs to the first epoch where the protocol feature was enabled
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
            &chain_store.get_epoch_id_of_last_block_with_chunk(prev_block_hash, shard_id)?,
        )?;
        Ok(!checked_feature!(
            "protocol_feature_restore_receipts_after_fix",
            RestoreReceiptsAfterFix,
            prev_protocol_version
        ))
    } else {
        Ok(false)
    }
}
