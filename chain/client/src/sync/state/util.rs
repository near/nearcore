use crate::metrics;
use near_chain::BlockHeader;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::{ShardStateSyncResponseHeader, StateHeaderKey};
use near_primitives::types::{EpochHeight, EpochId, ShardId};
use near_store::{DBCol, Store};

pub(super) fn increment_download_count(shard_id: ShardId, typ: &str, source: &str, result: &str) {
    metrics::STATE_SYNC_DOWNLOAD_RESULT
        .with_label_values(&[&shard_id.to_string(), typ, source, result])
        .inc();
}

pub(super) fn query_epoch_id_and_height_for_block(
    store: &Store,
    block_hash: CryptoHash,
) -> Result<(EpochId, EpochHeight), near_chain::Error> {
    let block_header =
        store.get_ser::<BlockHeader>(DBCol::BlockHeader, block_hash.as_bytes())?.ok_or_else(
            || near_chain::Error::DBNotFoundErr(format!("No block header {}", block_hash)),
        )?;
    let epoch_id = *block_header.epoch_id();
    let epoch_info = store
        .get_ser::<EpochInfo>(DBCol::EpochInfo, epoch_id.0.as_bytes())?
        .ok_or_else(|| near_chain::Error::DBNotFoundErr(format!("No epoch info {:?}", epoch_id)))?;
    let epoch_height = epoch_info.epoch_height();
    Ok((epoch_id, epoch_height))
}

pub fn get_state_header_if_exists_in_storage(
    store: &Store,
    sync_hash: CryptoHash,
    shard_id: ShardId,
) -> Result<Option<ShardStateSyncResponseHeader>, near_chain::Error> {
    Ok(store.get_ser::<ShardStateSyncResponseHeader>(
        DBCol::StateHeaders,
        &borsh::to_vec(&StateHeaderKey(shard_id, sync_hash)).unwrap(),
    )?)
}
