use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::get_block_shard_uid;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};
use near_primitives::utils::{get_block_shard_id, index_to_bytes};
use near_store::adapter::StoreUpdateAdapter;
use near_store::adapter::cloud_archival_store::CloudReaderHead;
use near_store::archive::cloud_storage::{
    BlockData, CloudRetrievalError, CloudStorage, EpochData, ShardData,
};
use near_store::{DBCol, ShardUId, Store, StoreUpdate};
use std::collections::HashSet;

/// Errors from reader-side custom logic on top of cloud retrieval.
#[derive(thiserror::Error, Debug)]
pub enum CloudArchivalReaderError {
    #[error(transparent)]
    Retrieval(#[from] CloudRetrievalError),
    #[error(transparent)]
    Chain(#[from] Error),
    #[error(transparent)]
    Epoch(#[from] EpochError),
    #[error("walked back to genesis without finding a state snapshot")]
    NoSnapshotFound,
    #[error("no block below {start_height}, which must be above the first archived block")]
    NoAnchorBelow { start_height: BlockHeight },
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

/// What one pull of a block batch found.
pub(crate) struct BlockBatchPull {
    /// The batch's last height.
    pub end_height: BlockHeight,
    /// The epoch opening inside the batch.
    pub opening_epoch_id: Option<EpochId>,
    /// The last block written, absent when none was.
    pub last_present_block_hash: Option<CryptoHash>,
}

/// The last block of an epoch.
struct EpochEnd {
    height: BlockHeight,
    /// The epoch the block above it opens.
    next_epoch_id: EpochId,
}

/// Downloads the batch containing `start_height` and writes its block rows from there
/// to the batch's end in one commit. When an epoch ends inside the batch, the epoch
/// starting after it is pulled too.
pub(crate) async fn pull_block_batch(
    store: &Store,
    cloud_storage: &CloudStorage,
    epoch_manager: &dyn EpochManagerAdapter,
    start_height: BlockHeight,
) -> Result<BlockBatchPull, CloudArchivalReaderError> {
    let block_batch = cloud_storage.get_block_batch_for_height(start_height).await?;
    let mut last_present_block_hash = None;
    let mut epoch_end: Option<EpochEnd> = None;
    let mut update = store.store_update();
    for block_height in start_height..=block_batch.end_height() {
        let Some(block_data) = block_batch.get_block_at_height(block_height) else {
            continue;
        };
        save_block_data(&mut update, block_data);
        last_present_block_hash = Some(*block_data.block().header().hash());
        if epoch_end.is_none()
            && epoch_manager.is_next_block_in_next_epoch(block_data.block_info())?
        {
            epoch_end = Some(EpochEnd {
                height: block_height,
                next_epoch_id: *block_data.block().header().next_epoch_id(),
            });
        }
    }
    update.commit();
    if let Some(epoch_end) = &epoch_end {
        pull_epoch_data(store, cloud_storage, &epoch_end.next_epoch_id).await?;
    }
    let opening_epoch_id =
        epoch_end.filter(|end| end.height < block_batch.end_height()).map(|end| end.next_epoch_id);
    Ok(BlockBatchPull {
        end_height: block_batch.end_height(),
        opening_epoch_id,
        last_present_block_hash,
    })
}

/// Downloads the batch of `shard_id` data that `from_height` falls in and writes the rows
/// it holds at that height and above in one commit.
pub(crate) async fn pull_shard_batch(
    store: &Store,
    cloud_storage: &CloudStorage,
    shard_uid: ShardUId,
    from_height: BlockHeight,
) -> Result<(), CloudArchivalReaderError> {
    let shard_batch = cloud_storage.get_shard_batch(from_height, shard_uid.shard_id()).await?;
    // TODO(cloud_archival): in case of resharding, install the shard's state from its epoch
    // snapshot and walk the recorded inverse changes down.
    let start_height = std::cmp::max(from_height, shard_batch.start_height());
    let mut update = store.store_update();
    for height in start_height..=shard_batch.end_height() {
        if let Some(shard_data) = shard_batch.get_data_at_height(height) {
            save_shard_data(&mut update, shard_uid, shard_data);
        }
    }
    update.commit();
    Ok(())
}

/// Seeds the store with what the epoch manager needs to answer for `height`, and
/// returns the hash of the nearest present block below it. `height` must therefore
/// be above the first archived block.
pub(crate) async fn install_anchors(
    store: &Store,
    cloud_storage: &CloudStorage,
    epoch_manager: &dyn EpochManagerAdapter,
    height: BlockHeight,
) -> Result<CryptoHash, CloudArchivalReaderError> {
    let (_, prev_block) =
        find_present_block_below(cloud_storage, height).await.map_err(|err| match err {
            CloudRetrievalError::NoBlockData { .. } => {
                CloudArchivalReaderError::NoAnchorBelow { start_height: height }
            }
            err => err.into(),
        })?;
    let prev_block_epoch_id = *prev_block.block().header().epoch_id();
    pull_epoch_data(store, cloud_storage, &prev_block_epoch_id).await?;

    let prev_block_hash = *prev_block.block().header().hash();
    let mut update = store.store_update();
    // `get_epoch_id_from_prev_block` starts by reading this row.
    update.insert_ser(DBCol::BlockInfo, prev_block_hash.as_ref(), prev_block.block_info());
    update.commit();

    let start_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;
    if start_epoch_id != prev_block_epoch_id {
        pull_epoch_data(store, cloud_storage, &start_epoch_id).await?;
    }
    Ok(prev_block_hash)
}

/// The shards this reader tracks in both epochs a batch can hold blocks in.
pub(crate) fn shards_tracked_in_batch(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    prev_block_hash: &CryptoHash,
    opening_epoch_id: Option<EpochId>,
) -> Result<HashSet<ShardUId>, CloudArchivalReaderError> {
    let batch_epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
    let mut epoch_ids = vec![batch_epoch_id];
    epoch_ids.extend(opening_epoch_id);
    let mut shard_uids = HashSet::new();
    for epoch_id in epoch_ids {
        shard_uids.extend(shard_tracker.get_tracked_shards_for_non_validator_in_epoch(&epoch_id)?);
    }
    Ok(shard_uids)
}

/// Stores how far the reader has taken the archive, and returns that head.
pub(crate) fn save_reader_head(
    store: &Store,
    height: BlockHeight,
    last_present_block_hash: CryptoHash,
) -> CloudReaderHead {
    let head = CloudReaderHead { height, last_present_block_hash };
    let mut update = store.store_update();
    update.cloud_archival_store_update().set_reader_head(&head);
    update.commit();
    head
}

/// Writes one epoch's cloud data into `update`.
pub(crate) fn save_epoch_data(update: &mut StoreUpdate, epoch_data: &EpochData) {
    let epoch_id = epoch_data.epoch_id();
    update.set_ser(DBCol::EpochInfo, epoch_id.as_ref(), epoch_data.epoch_info());
    update.set_ser(DBCol::EpochStart, epoch_id.as_ref(), &epoch_data.epoch_start_height());
    let first_block_info = epoch_data.epoch_first_block_info();
    update.insert_ser(DBCol::BlockInfo, first_block_info.hash().as_ref(), first_block_info);
}

/// Writes one shard's columns from its cloud `ShardData` into `update`.
pub(crate) fn save_shard_data(
    update: &mut StoreUpdate,
    shard_uid: ShardUId,
    shard_data: &ShardData,
) {
    // TODO(cloud_archival): reconstruct the remaining shard columns and apply
    // per-block state deltas.
    let block_hash = shard_data.block_hash();
    let block_shard_id = get_block_shard_id(block_hash, shard_uid.shard_id());
    update.set_ser(DBCol::ChunkApplyStats, &block_shard_id, shard_data.chunk_apply_stats());
    // ChunkExtra is the one shard column keyed by the shard's uid.
    update.set_ser(
        DBCol::ChunkExtra,
        &get_block_shard_uid(block_hash, &shard_uid),
        shard_data.chunk_extra(),
    );
    if let Some(chunk) = shard_data.chunk() {
        update.insert_ser(DBCol::Chunks, chunk.chunk_hash().as_ref(), chunk);
    }
    if let Some(outgoing_receipts) = shard_data.outgoing_receipts() {
        update.set_ser(DBCol::OutgoingReceipts, &block_shard_id, outgoing_receipts);
    }
}

/// First present block below `height`. Errors if no such block exists in cloud
/// (e.g. `height` sits below the first archived block).
pub async fn find_present_block_below(
    cloud_storage: &CloudStorage,
    height: BlockHeight,
) -> Result<(BlockHeight, BlockData), CloudRetrievalError> {
    assert!(height > 0, "no block sits below height 0");
    let mut h = height - 1;
    let mut batch = cloud_storage.get_block_batch_for_height(h).await?;
    loop {
        if h < batch.start_height() {
            batch = cloud_storage.get_block_batch_for_height(h).await?;
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
pub async fn find_snapshot_at_or_before(
    cloud_storage: &CloudStorage,
    height: BlockHeight,
    shard_id: ShardId,
) -> Result<(EpochHeight, EpochId), CloudArchivalReaderError> {
    let (_, initial_block) = find_present_block_below(cloud_storage, height + 1).await?;
    let mut epoch_id = *initial_block.block().header().epoch_id();

    loop {
        let epoch_data = cloud_storage.get_epoch_data(epoch_id).await?;
        let epoch_height = epoch_data.epoch_info().epoch_height();
        let epoch_start_height = epoch_data.epoch_start_height();

        tracing::info!(epoch_height, ?epoch_id, "probing for state snapshot");

        if cloud_storage.is_state_header_stored(epoch_height, epoch_id, shard_id).await? {
            return Ok((epoch_height, epoch_id));
        }

        let batch = cloud_storage.get_block_batch_for_height(epoch_start_height).await?;
        // Epoch start is by chain definition always produced; if it's None in cloud
        // we don't have earlier chain data, so the walk-back can't continue.
        let Some(epoch_start_block) = batch.get_block_at_height(epoch_start_height) else {
            return Err(CloudArchivalReaderError::NoSnapshotFound);
        };
        if epoch_start_block.block_info().is_genesis() {
            return Err(CloudArchivalReaderError::NoSnapshotFound);
        }
        let (_, prev_block) = find_present_block_below(cloud_storage, epoch_start_height).await?;
        epoch_id = *prev_block.block().header().epoch_id();
    }
}

/// Downloads one epoch's data out of the bucket and writes it into the store.
pub(crate) async fn pull_epoch_data(
    store: &Store,
    cloud_storage: &CloudStorage,
    epoch_id: &EpochId,
) -> Result<(), CloudRetrievalError> {
    let epoch_data = cloud_storage.get_epoch_data(*epoch_id).await?;
    let mut update = store.store_update();
    save_epoch_data(&mut update, &epoch_data);
    update.commit();
    tracing::info!(
        ?epoch_id,
        epoch_start_height = epoch_data.epoch_start_height(),
        "pulled epoch data"
    );
    Ok(())
}
