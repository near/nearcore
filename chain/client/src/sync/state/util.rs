use crate::metrics;
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::{ShardStateSyncResponseHeader, StateHeaderKey};
use near_primitives::types::ShardId;
use near_store::{DBCol, Store};

pub(super) fn increment_download_count(shard_id: ShardId, typ: &str, source: &str, result: &str) {
    metrics::STATE_SYNC_DOWNLOAD_RESULT
        .with_label_values(&[shard_id.to_string().as_str(), typ, source, result])
        .inc();
}

pub fn get_state_header_if_exists_in_storage(
    store: &Store,
    sync_hash: CryptoHash,
    shard_id: ShardId,
) -> Result<Option<ShardStateSyncResponseHeader>, near_chain::Error> {
    Ok(store.get_ser::<ShardStateSyncResponseHeader>(
        DBCol::StateHeaders,
        &borsh::to_vec(&StateHeaderKey(shard_id, sync_hash)).unwrap(),
    ))
}
