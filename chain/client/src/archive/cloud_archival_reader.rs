use near_primitives::merkle::PartialMerkleTree;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};
use near_primitives::utils::index_to_bytes;
use near_store::archive::cloud_storage::{BlockData, CloudRetrievalError, CloudStorage, EpochData};
use near_store::{DBCol, Store};
use std::collections::HashSet;

/// Errors from reader-side custom logic on top of cloud retrieval.
#[derive(thiserror::Error, Debug)]
pub enum CloudArchivalReaderError {
    #[error(transparent)]
    Retrieval(#[from] CloudRetrievalError),
    #[error("walked back to genesis without finding a state snapshot")]
    NoSnapshotFound,
}

/// Writes block-level data from cloud storage into the local store.
///
/// The merkle tree is updated incrementally: the previous block's tree is read,
/// extended with prev_hash, and saved under the current block's hash. The
/// initial tree for each epoch must be written by `save_epoch_data` first.
///
/// Block, BlockHeader, BlockInfo use `insert_ser` (insert-only, content-addressed
/// by hash). BlockHeight and BlockMerkleTree use `set_ser` (regular columns,
/// keyed by height or hash, safe to overwrite).
pub fn save_block_data(store: &Store, block_data: &BlockData) {
    let block = block_data.block();
    let header = block.header();
    let block_hash = *header.hash();
    let height = header.height();

    let mut update = store.store_update();

    update.insert_ser(DBCol::BlockHeader, block_hash.as_ref(), header);
    update.insert_ser(DBCol::Block, block_hash.as_ref(), block);
    update.insert_ser(DBCol::BlockInfo, block_hash.as_ref(), block_data.block_info());
    update.set_ser(DBCol::BlockHeight, &index_to_bytes(height), &block_hash);

    // Update block merkle tree incrementally.
    if header.is_genesis() {
        update.set_ser(DBCol::BlockMerkleTree, block_hash.as_ref(), &PartialMerkleTree::default());
    } else {
        let prev_tree: PartialMerkleTree = store
            .get_ser(DBCol::BlockMerkleTree, header.prev_hash().as_ref())
            .expect("prev block's merkle tree must exist; ensure save_epoch_data was called first");
        let mut tree = prev_tree;
        tree.insert(*header.prev_hash());
        update.set_ser(DBCol::BlockMerkleTree, block_hash.as_ref(), &tree);
    }

    update.commit();
}

/// Writes epoch-level data from cloud storage into the local store. Uses the
/// prev_hash of the epoch start block (carried inside `epoch_data`) as the key
/// for the epoch's initial BlockMerkleTree.
pub fn save_epoch_data(store: &Store, epoch_id: &EpochId, epoch_data: &EpochData) {
    let mut update = store.store_update();

    update.set_ser(DBCol::EpochInfo, epoch_id.as_ref(), epoch_data.epoch_info());
    update.set_ser(DBCol::EpochStart, epoch_id.as_ref(), &epoch_data.epoch_start_height());

    update.set_ser(
        DBCol::BlockMerkleTree,
        epoch_data.epoch_start_prev_hash().as_ref(),
        epoch_data.epoch_start_prev_block_merkle_tree(),
    );

    update.commit();
}

/// Downloads block and epoch data for [start_height, end_height] from cloud
/// storage and writes it into the local store.
///
/// When `start_height` falls mid-epoch, blocks from the epoch start are
/// backfilled automatically so the merkle tree chain is complete.
///
/// TODO(cloud_archival): Also download and apply shard (state) data per block.
pub fn bootstrap_range(
    store: &Store,
    cloud_storage: &CloudStorage,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> anyhow::Result<()> {
    let mut saved_epochs = HashSet::<EpochId>::new();

    // Backfill blocks from the first epoch's start to start_height so the
    // merkle tree chain is complete when starting mid-epoch. Clip
    // `start_height` to the nearest present block at-or-below it so the main
    // loop starts at a real block.
    let (start_height, first_epoch_data) =
        backfill_epoch_start(store, cloud_storage, start_height)?;
    saved_epochs.insert(*first_epoch_data.epoch_id());

    let range_length = end_height - start_height + 1;
    let epoch_length = end_height - first_epoch_data.epoch_start_height();
    let log_interval = std::cmp::max(10, std::cmp::min(epoch_length, range_length / 100));

    // Fetch one batch per iteration and consume all its heights, so each
    // batch blob is downloaded and decompressed once rather than per height.
    let mut height = start_height;
    while height <= end_height {
        let batch = cloud_storage.get_block_batch_for_height(height)?;
        let last_in_batch = std::cmp::min(batch.end_height(), end_height);
        for h in height..=last_in_batch {
            let Some(block_data) = batch.get_block_at_height(h) else {
                continue;
            };
            let epoch_id = *block_data.block().header().epoch_id();
            if saved_epochs.insert(epoch_id) {
                save_new_epoch(store, cloud_storage, &epoch_id)?;
            }
            save_block_data(store, block_data);
            if (h - start_height).is_multiple_of(log_interval) || h == end_height {
                tracing::info!(height = h, end_height, "bootstrap progress");
            }
        }
        height = last_in_batch + 1;
    }

    Ok(())
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

/// Downloads and saves epoch data, then backfills all blocks from epoch start
/// up to (but excluding) the nearest present block at-or-below `start_height`,
/// so the merkle tree chain is complete for mid-epoch starts. Returns that
/// present-block height so the caller can begin its main loop on a real block.
fn backfill_epoch_start(
    store: &Store,
    cloud_storage: &CloudStorage,
    start_height: BlockHeight,
) -> anyhow::Result<(BlockHeight, EpochData)> {
    let (start_height, start_block) = find_present_block_at_or_below(cloud_storage, start_height)?;
    let epoch_id = *start_block.block().header().epoch_id();
    let epoch_data = save_new_epoch(store, cloud_storage, &epoch_id)?;

    let epoch_start = epoch_data.epoch_start_height();
    if epoch_start < start_height {
        tracing::info!(epoch_start, start_height, "backfilling blocks from epoch start");
    }
    // Fetch one batch per iteration; one batch spans up to `batch_size` heights.
    let mut height = epoch_start;
    while height < start_height {
        let batch = cloud_storage.get_block_batch_for_height(height)?;
        let last_in_batch = std::cmp::min(batch.end_height(), start_height - 1);
        for h in height..=last_in_batch {
            if let Some(block_data) = batch.get_block_at_height(h) {
                save_block_data(store, block_data);
            }
        }
        height = last_in_batch + 1;
    }

    Ok((start_height, epoch_data))
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
fn save_new_epoch(
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
