use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::shard_layout::get_block_shard_uid;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};
use near_primitives::utils::{get_block_shard_id, get_outcome_id_block_hash, index_to_bytes};
use near_store::archive::cloud_storage::{
    BlockData, CloudRetrievalError, CloudStorage, EpochData, ShardData,
};
use near_store::{DBCol, KeyForStateChanges, ShardUId, Store};
use std::collections::{HashMap, HashSet};

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
/// `save_epoch_data` must run first for the current epoch to seed the merkle
/// tree at the epoch start; this function then extends the tree incrementally.
pub fn save_block_data(store: &Store, block_data: &BlockData) {
    let block = block_data.block();
    let header = block.header();
    let block_hash = *header.hash();
    let height = header.height();

    let mut update = store.store_update();

    // BlockHeader, Block, BlockInfo are content-addressed by hash; use
    // insert_ser to keep the column insert-only.
    update.insert_ser(DBCol::BlockHeader, block_hash.as_ref(), header);
    update.insert_ser(DBCol::Block, block_hash.as_ref(), block);
    update.insert_ser(DBCol::BlockInfo, block_hash.as_ref(), block_data.block_info());
    update.set_ser(DBCol::BlockHeight, &index_to_bytes(height), &block_hash);

    // Archive only carries canonical blocks, so one hash per height per epoch.
    let mut block_per_height: HashMap<EpochId, HashSet<CryptoHash>> = HashMap::new();
    block_per_height.insert(*header.epoch_id(), HashSet::from([block_hash]));
    update.set_ser(DBCol::BlockPerHeight, &index_to_bytes(height), &block_per_height);

    update.set_ser(DBCol::NextBlockHashes, block_hash.as_ref(), block_data.next_block_hash());

    let mut chunk_hashes: HashSet<ChunkHash> = HashSet::new();
    for chunk_header in block.chunks().iter_raw() {
        if chunk_header.is_new_chunk(height) {
            chunk_hashes.insert(chunk_header.chunk_hash().clone());
        }
    }
    // Skip the write when no shard has a new chunk so the on-disk shape
    // matches what the chain store produces at apply time.
    if !chunk_hashes.is_empty() {
        update.set_ser(DBCol::ChunkHashesByHeight, &index_to_bytes(height), &chunk_hashes);
    }

    // Update the block merkle tree incrementally: read the previous block's
    // tree, extend with prev_hash, and save under the current block's hash.
    // save_epoch_data must run first to seed the tree at the epoch start.
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

/// Writes shard-level data for one `(block_hash, shard_uid)` into the local
/// store. Mirrors the chain store's column shapes so reader queries hit the
/// same code paths as a normal node.
pub fn save_shard_data(
    store: &Store,
    block_hash: &CryptoHash,
    shard_uid: ShardUId,
    shard_data: &ShardData,
) {
    let shard_id = shard_uid.shard_id();
    let block_shard_id_key = get_block_shard_id(block_hash, shard_id);
    let block_shard_uid_key = get_block_shard_uid(block_hash, &shard_uid);
    let chunk = shard_data.chunk();
    // Chunks presence marks (block, shard) already saved; reruns would inflate refcounts.
    if store.exists(DBCol::Chunks, chunk.chunk_hash().as_ref()) {
        return;
    }
    let mut update = store.store_update();

    // Chunks, ReceiptToTx, and TransactionResultForBlock are content-addressed;
    // use insert_ser to keep the columns insert-only.
    update.insert_ser(DBCol::Chunks, chunk.chunk_hash().as_ref(), chunk);

    let chunk_extra: &ChunkExtra = shard_data.chunk_extra();
    update.set_ser(DBCol::ChunkExtra, &block_shard_uid_key, chunk_extra);

    update.set_ser(DBCol::ChunkApplyStats, &block_shard_id_key, shard_data.chunk_apply_stats());

    // Transactions and Receipts are reference-counted; bump the refcount once
    // per stored value, matching what a chunk-applying node would write.
    for tx in chunk.to_transactions() {
        let bytes = borsh::to_vec(tx).expect("borsh cannot fail on SignedTransaction");
        update.increment_refcount(DBCol::Transactions, tx.get_hash().as_ref(), &bytes);
    }
    for receipt in chunk.prev_outgoing_receipts() {
        let bytes = borsh::to_vec(receipt).expect("borsh cannot fail on Receipt");
        update.increment_refcount(DBCol::Receipts, receipt.get_hash().as_ref(), &bytes);
    }

    let outcome_ids: Vec<CryptoHash> =
        shard_data.transaction_result_for_block().iter().map(|(id, _)| *id).collect();
    update.set_ser(DBCol::OutcomeIds, &block_shard_id_key, &outcome_ids);

    for (outcome_id, outcome_with_proof) in shard_data.transaction_result_for_block() {
        update.insert_ser(
            DBCol::TransactionResultForBlock,
            &get_outcome_id_block_hash(outcome_id, block_hash),
            outcome_with_proof,
        );
    }
    for (receipt_id, info) in shard_data.receipt_to_tx() {
        update.insert_ser(DBCol::ReceiptToTx, receipt_id.as_ref(), info);
    }

    update.set_ser(DBCol::IncomingReceipts, &block_shard_id_key, shard_data.incoming_receipts());
    update.set_ser(DBCol::OutgoingReceipts, &block_shard_id_key, shard_data.outgoing_receipts());

    for change in shard_data.state_changes() {
        let key_for_changes = KeyForStateChanges::from_trie_key(block_hash, &change.trie_key);
        update.set_ser(DBCol::StateChanges, key_for_changes.as_ref(), change);
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

/// Downloads block + epoch + per-shard data for `[start_height, end_height]`
/// from cloud storage and writes the cold columns into the local store.
///
/// When `start_height` falls mid-epoch, blocks from the epoch start are
/// backfilled automatically so the merkle tree chain is complete. Shard data
/// is saved only for `[start_height, end_height]` because earlier heights are
/// just block-level backfill for the merkle chain.
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
    let shard_layout = first_epoch_data.shard_layout().clone();

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

    // TODO(cloud_archival): support resharding. Layout is captured once;
    // a mid-range layout change would write under the wrong ShardUId.
    for shard_id in shard_layout.shard_ids() {
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        save_shard_range(store, cloud_storage, shard_uid, start_height, end_height)?;
    }

    Ok(())
}

/// Writes shard-level data for one shard across `[start_height, end_height]`.
fn save_shard_range(
    store: &Store,
    cloud_storage: &CloudStorage,
    shard_uid: ShardUId,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> anyhow::Result<()> {
    // TODO(cloud_archival): download and apply the state snapshot for this
    // shard before this loop, and apply per-block state deltas alongside the
    // column saves so the reader's trie advances with the saved data.
    let shard_id = shard_uid.shard_id();
    let mut height = start_height;
    while height <= end_height {
        let batch = cloud_storage.get_shard_batch_for_height(height, shard_id)?;
        let last_in_batch = std::cmp::min(batch.end_height(), end_height);
        for h in height..=last_in_batch {
            let Some(shard_data) = batch.get_shard_at_height(h) else {
                continue;
            };
            // save_block_data above writes BlockHeight at every present height.
            let block_hash: CryptoHash = store
                .get_ser(DBCol::BlockHeight, &index_to_bytes(h))
                .expect("BlockHeight saved earlier in bootstrap_range");
            save_shard_data(store, &block_hash, shard_uid, shard_data);
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
