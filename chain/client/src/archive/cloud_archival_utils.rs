use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};
use near_primitives::utils::{get_block_shard_id, index_to_bytes};
use near_store::archive::cloud_storage::{
    BlockData, CloudRetrievalError, CloudStorage, EpochData, ShardData,
};
use near_store::{DBCol, Store, StoreUpdate};

/// Errors from reader-side custom logic on top of cloud retrieval.
#[derive(thiserror::Error, Debug)]
pub enum CloudArchivalReaderError {
    #[error(transparent)]
    Retrieval(#[from] CloudRetrievalError),
    #[error("walked back to genesis without finding a state snapshot")]
    NoSnapshotFound,
}

/// Writes block-level columns from a cloud `BlockData` into `update`.
///
/// Block, BlockHeader, BlockInfo (content-addressed by hash) and ChunkProducers
/// all use `insert_ser` (insert-only columns). BlockHeight and
/// BlockMerkleTree use `set_ser` (regular columns, keyed by height or hash, safe
/// to overwrite).
pub fn save_block_data(update: &mut StoreUpdate, block_data: &BlockData) {
    let block = block_data.block();
    let header = block.header();
    let block_hash = *header.hash();
    let height = header.height();

    update.insert_ser(DBCol::BlockHeader, block_hash.as_ref(), header);
    update.insert_ser(DBCol::Block, block_hash.as_ref(), block);
    update.insert_ser(DBCol::BlockInfo, block_hash.as_ref(), block_data.block_info());
    update.set_ser(DBCol::BlockHeight, &index_to_bytes(height), &block_hash);
    update.set_ser(DBCol::BlockMerkleTree, block_hash.as_ref(), block_data.block_merkle_tree());

    for (shard_id, stake) in block_data.chunk_producers() {
        update.insert_ser(
            DBCol::ChunkProducers,
            &get_block_shard_id(&block_hash, *shard_id),
            stake,
        );
    }
}

/// Writes epoch-level data from cloud storage into the local store.
pub fn save_epoch_data(store: &Store, epoch_id: &EpochId, epoch_data: &EpochData) {
    let mut update = store.store_update();

    update.set_ser(DBCol::EpochInfo, epoch_id.as_ref(), epoch_data.epoch_info());
    update.set_ser(DBCol::EpochStart, epoch_id.as_ref(), &epoch_data.epoch_start_height());

    update.commit();
}

/// Writes one shard's columns from its cloud `ShardData` into `update`.
pub(crate) fn save_shard_data(update: &mut StoreUpdate, shard_id: ShardId, shard_data: &ShardData) {
    // TODO(cloud_archival): reconstruct the remaining shard columns and apply
    // per-block state deltas.
    let block_shard_id = get_block_shard_id(shard_data.block_hash(), shard_id);
    update.set_ser(DBCol::ChunkApplyStats, &block_shard_id, shard_data.chunk_apply_stats());
    if let Some(chunk) = shard_data.chunk() {
        update.insert_ser(DBCol::Chunks, chunk.chunk_hash().as_ref(), chunk);
    }
    if let Some(outgoing_receipts) = shard_data.outgoing_receipts() {
        update.set_ser(DBCol::OutgoingReceipts, &block_shard_id, outgoing_receipts);
    }
}

/// First present block at or below `height`. Errors if no such block exists
/// in cloud (e.g. `height` is below the first archived block).
pub fn find_present_block_at_or_below(
    cloud_storage: &CloudStorage,
    height: BlockHeight,
) -> Result<(BlockHeight, BlockData), CloudRetrievalError> {
    let mut h = height;
    let mut batch = cloud_storage.get_block_batch_for_height(h)?;
    loop {
        if h < batch.start_height() {
            batch = cloud_storage.get_block_batch_for_height(h)?;
        }
        if let Some(block) = batch.get_block_at_height(h) {
            return Ok((h, block.clone()));
        }
        assert!(h > 0, "walked past height 0 without finding the genesis block");
        h -= 1;
    }
}

/// Walks epochs backward from `height` and returns the first `(epoch_height, epoch_id)`
/// whose state-header is present in cloud for `shard_id`. Errors when the walk-back
/// reaches below the earliest archived data without finding a snapshot.
pub fn find_snapshot_at_or_before(
    cloud_storage: &CloudStorage,
    height: BlockHeight,
    shard_id: ShardId,
) -> Result<(EpochHeight, EpochId), CloudArchivalReaderError> {
    let (_, initial_block) = find_present_block_at_or_below(cloud_storage, height)?;
    let mut epoch_id = *initial_block.block().header().epoch_id();

    loop {
        let epoch_data = cloud_storage.get_epoch_data(epoch_id)?;
        let epoch_height = epoch_data.epoch_info().epoch_height();
        let epoch_start_height = epoch_data.epoch_start_height();

        tracing::info!(epoch_height, ?epoch_id, "probing for state snapshot");

        if cloud_storage.is_state_header_stored(epoch_height, epoch_id, shard_id)? {
            return Ok((epoch_height, epoch_id));
        }

        let batch = cloud_storage.get_block_batch_for_height(epoch_start_height)?;
        // Epoch start is by chain definition always produced; if it's None in cloud
        // we don't have earlier chain data, so the walk-back can't continue.
        let Some(epoch_start_block) = batch.get_block_at_height(epoch_start_height) else {
            return Err(CloudArchivalReaderError::NoSnapshotFound);
        };
        if epoch_start_block.block_info().is_genesis() {
            return Err(CloudArchivalReaderError::NoSnapshotFound);
        }
        let (_, prev_block) =
            find_present_block_at_or_below(cloud_storage, epoch_start_height - 1)?;
        epoch_id = *prev_block.block().header().epoch_id();
    }
}

/// Downloads and saves epoch-level data for a new epoch.
pub(crate) fn save_new_epoch(
    store: &Store,
    cloud_storage: &CloudStorage,
    epoch_id: &EpochId,
) -> Result<EpochData, CloudRetrievalError> {
    let epoch_data = cloud_storage.get_epoch_data(*epoch_id)?;
    save_epoch_data(store, epoch_id, &epoch_data);
    tracing::info!(
        ?epoch_id,
        epoch_start_height = epoch_data.epoch_start_height(),
        "saved epoch data"
    );
    Ok(epoch_data)
}
