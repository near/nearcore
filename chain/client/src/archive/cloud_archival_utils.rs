use crate::archive::cloud_reader_trie_utils::BatchTrieUpdate;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::{Block, Tip};
use near_primitives::errors::{EpochError, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::state_part::StatePartId;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId};
use near_primitives::utils::index_to_bytes;
use near_store::adapter::cloud_archival_store::CloudReaderHead;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::archive::cloud_storage::{
    BlockData, CloudRetrievalError, CloudStorage, EpochData, NewChunkData, ShardBatch, ShardData,
};
use near_store::{DBCol, KeyForStateChanges, ShardTries, ShardUId, Store, StoreUpdate};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;

/// Errors from reader-side custom logic on top of cloud retrieval.
#[derive(thiserror::Error, Debug)]
pub enum CloudArchivalReaderError {
    #[error(transparent)]
    Retrieval(#[from] CloudRetrievalError),
    #[error(transparent)]
    Chain(#[from] Error),
    #[error(transparent)]
    Epoch(#[from] EpochError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("walked back to genesis without finding a state snapshot")]
    NoSnapshotFound,
    #[error("no block below {start_height}, which must be above the first archived block")]
    NoAnchorBelow { start_height: BlockHeight },
    #[error("no state root for shard {shard_uid} under block {block_hash}")]
    NoStateAnchor { shard_uid: ShardUId, block_hash: CryptoHash },
    #[error("shard {shard_uid} state part {part_id:?}: {error}")]
    StatePartDeserialization { shard_uid: ShardUId, part_id: StatePartId, error: io::Error },
    #[error(
        "state root at block {block_hash} shard {shard_id}: applied {applied}, recorded {recorded}"
    )]
    StateRootMismatch {
        block_hash: CryptoHash,
        shard_id: ShardId,
        applied: CryptoHash,
        recorded: CryptoHash,
    },
}

/// The chunks this block is the first to include, keyed by the height each was created at:
/// this block's own height, and every height below it back to the previous block, which
/// produced none of its own.
///
/// A chunk created at a height that produced no block reaches the chain in the next block
/// that does, so those rows belong to this one.
pub fn chunk_hashes_by_height(block: &Block) -> Vec<(BlockHeight, HashSet<ChunkHash>)> {
    let block_height = block.header().height();
    // Genesis has no previous block, and owns its own height alone. Headers below V3 do
    // not carry the previous height; the archive never holds blocks that old.
    let first_height = match block.header().prev_height() {
        Some(prev_height) if !block.header().is_genesis() => prev_height + 1,
        _ => block_height,
    };
    let mut rows: BTreeMap<BlockHeight, HashSet<ChunkHash>> = BTreeMap::new();
    for chunk in block.chunks().iter_raw() {
        let height_created = chunk.height_created();
        if height_created < first_height {
            continue;
        }
        rows.entry(height_created).or_default().insert(chunk.chunk_hash().clone());
    }
    rows.into_iter().collect()
}

/// Writes one block's cloud data into the block-level columns a reader reproduces.
pub fn save_block_data(update: &mut StoreUpdate, block_data: &BlockData) {
    let block = block_data.block();
    let header = block.header();
    let block_hash = *header.hash();
    let height = header.height();

    update.insert_ser(DBCol::Block, block_hash.as_ref(), block);
    let mut chain_store_update = update.chain_store_update();
    chain_store_update.set_block_header_only(header);
    chain_store_update.set_block_height(&block_hash, height);
    chain_store_update.set_block_merkle_tree(&block_hash, block_data.block_merkle_tree());
    // The block's own tree holds every block below it, so its size is this block's
    // ordinal, which is the key the block-merkle-proof walk looks the hash up by.
    chain_store_update.set_block_ordinal(block_data.block_merkle_tree().size(), &block_hash);
    update.set_ser(DBCol::NextBlockHashes, block_hash.as_ref(), block_data.next_block_hash());
    for (created_height, chunk_hashes) in chunk_hashes_by_height(block) {
        update.set_ser(DBCol::ChunkHashesByHeight, &index_to_bytes(created_height), &chunk_hashes);
    }
    // The archive holds the canonical chain alone, so this block is the whole set of
    // blocks at its height.
    let blocks_at_height = HashMap::from([(*header.epoch_id(), HashSet::from([block_hash]))]);
    update.set_ser(DBCol::BlockPerHeight, &index_to_bytes(height), &blocks_at_height);

    let mut epoch_store_update = update.epoch_store_update();
    epoch_store_update.set_block_info(block_data.block_info());
    for (shard_id, stake) in block_data.chunk_producers() {
        epoch_store_update.set_chunk_producer(&block_hash, *shard_id, stake);
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

/// Downloads `shard_uid`'s batch for the window `from_height` falls in and writes the
/// rows it carries at or above that height, in one commit. A batch that opens above
/// `from_height`, which is a shard a resharding added there, is written from its own start.
pub(crate) async fn pull_shard_batch(
    store: &Store,
    cloud_storage: &CloudStorage,
    shard_uid: ShardUId,
    from_height: BlockHeight,
) -> Result<ShardBatch, CloudArchivalReaderError> {
    let shard_batch = cloud_storage.get_shard_batch(from_height, shard_uid.shard_id()).await?;
    // A shard the reader still tracks at `from_height` cannot have a batch that ended
    // below it: a retired shard's batch ends where the epoch it belonged to does.
    if shard_batch.end_height() < from_height {
        return Err(CloudRetrievalError::NoShardData {
            height: from_height,
            shard_id: shard_uid.shard_id(),
        }
        .into());
    }
    // TODO(cloud_archival): in case of resharding, install the shard's state from its epoch
    // snapshot and walk the recorded inverse changes down.
    let mut start_height = from_height;
    if shard_batch.start_height() > from_height {
        tracing::info!(
            %shard_uid,
            from_height,
            batch_start = shard_batch.start_height(),
            "shard opens inside the batch, so a resharding added it there",
        );
        start_height = shard_batch.start_height();
    }
    let mut update = store.store_update();
    for height in start_height..=shard_batch.end_height() {
        if let Some(shard_data) = shard_batch.get_data_at_height(height) {
            save_shard_data(&mut update, shard_uid, shard_data);
        }
    }
    update.commit();
    Ok(shard_batch)
}

/// Applies the state changes `shard_batch` records at or above `from_height` on top of
/// `state_root`, in one commit, and returns the root the last of them leaves.
pub(crate) fn apply_batch_state_changes(
    tries: &ShardTries,
    shard_uid: ShardUId,
    shard_batch: &ShardBatch,
    from_height: BlockHeight,
    mut state_root: CryptoHash,
) -> Result<CryptoHash, CloudArchivalReaderError> {
    let start_height = from_height.max(shard_batch.start_height());
    let mut update = BatchTrieUpdate::new(tries, shard_uid, state_root);
    for height in start_height..=shard_batch.end_height() {
        let Some(shard_data) = shard_batch.get_data_at_height(height) else {
            continue;
        };
        state_root = update.apply(shard_data.state_changes())?;
        let recorded = *shard_data.chunk_extra().state_root();
        if state_root != recorded {
            return Err(CloudArchivalReaderError::StateRootMismatch {
                block_hash: *shard_data.block_hash(),
                shard_id: shard_uid.shard_id(),
                applied: state_root,
                recorded,
            });
        }
    }
    // TODO(cloud_archival): consider one commit per window, so a retry refcounts once.
    update.commit();
    Ok(state_root)
}

/// The state root `shard_uid` stands at under `block_hash`, which is the root the height
/// above that block applies onto.
pub(crate) fn shard_state_anchor(
    tries: &ShardTries,
    block_hash: &CryptoHash,
    shard_uid: ShardUId,
) -> Result<CryptoHash, CloudArchivalReaderError> {
    match tries.store().store_ref().chunk_store().get_chunk_extra(block_hash, &shard_uid) {
        Ok(chunk_extra) => Ok(*chunk_extra.state_root()),
        Err(Error::DBNotFoundErr(_)) => {
            Err(CloudArchivalReaderError::NoStateAnchor { shard_uid, block_hash: *block_hash })
        }
        Err(error) => Err(error.into()),
    }
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

/// Stores how far the reader has taken the archive together with the heads a query
/// resolves against, and returns that reader head. One commit, so a chain head never
/// names a block whose rows a stopped run left unwritten.
pub(crate) fn save_reader_position(
    store: &Store,
    height: BlockHeight,
    last_present_block_hash: CryptoHash,
) -> Result<CloudReaderHead, CloudArchivalReaderError> {
    let head = CloudReaderHead { height, last_present_block_hash };
    let header = store.chain_store().get_block_header(&last_present_block_hash)?;
    let tip = Tip::from_header(&header);
    let mut update = store.store_update();
    update.cloud_archival_store_update().set_reader_head(&head);
    update.cloud_archival_store_update().set_chain_heads(&tip);
    update.commit();
    Ok(head)
}

/// Writes one epoch's cloud data into `update`.
pub(crate) fn save_epoch_data(update: &mut StoreUpdate, epoch_data: &EpochData) {
    let epoch_id = epoch_data.epoch_id();
    let mut epoch_store_update = update.epoch_store_update();
    epoch_store_update.set_epoch_info(epoch_id, epoch_data.epoch_info());
    epoch_store_update.set_epoch_start(epoch_id, epoch_data.epoch_start_height());
    epoch_store_update.set_block_info(epoch_data.epoch_first_block_info());
    // A blob is published at epoch start, so it cannot carry its own aggregate data.
    // What it carries is the epoch below's, and it goes under that epoch's id.
    let prev_epoch_id = epoch_data.prev_epoch_id();
    if let Some(summary) = epoch_data.prev_epoch_summary() {
        epoch_store_update.set_epoch_validator_info(prev_epoch_id, summary);
    }
    epoch_store_update.set_epoch_info(&epoch_data.next_epoch_id(), epoch_data.next_epoch_info());
    if let Some(light_client_block) = epoch_data.prev_epoch_light_client_block() {
        update.set_ser(DBCol::EpochLightClientBlocks, prev_epoch_id.as_ref(), light_client_block);
    }
}

/// Writes one shard's columns from its cloud `ShardData` into `update`.
pub(crate) fn save_shard_data(
    update: &mut StoreUpdate,
    shard_uid: ShardUId,
    shard_data: &ShardData,
) {
    let block_hash = shard_data.block_hash();
    let shard_id = shard_uid.shard_id();
    let mut chunk_store_update = update.chunk_store_update();
    chunk_store_update.set_chunk_apply_stats(block_hash, shard_id, shard_data.chunk_apply_stats());
    chunk_store_update.set_chunk_extra(block_hash, &shard_uid, shard_data.chunk_extra());
    if let Some(incoming_receipts) = shard_data.incoming_receipts() {
        update.chain_store_update().set_incoming_receipt(block_hash, shard_id, incoming_receipts);
    }
    for changes in shard_data.state_changes() {
        let row_key =
            KeyForStateChanges::for_state_change(block_hash, &changes.trie_key, &shard_uid);
        update.trie_store_update().set_state_changes(row_key, changes);
    }
    if let Some(new_chunk) = shard_data.new_chunk() {
        save_new_chunk_data(update, block_hash, shard_id, new_chunk);
    }
}

/// Writes the rows only a block that produced a new chunk for this shard has.
fn save_new_chunk_data(
    update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    new_chunk: &NewChunkData,
) {
    let chunk = new_chunk.chunk();
    update.insert_ser(DBCol::Chunks, chunk.chunk_hash().as_ref(), chunk);
    let mut chain_store_update = update.chain_store_update();
    chain_store_update.set_processed_receipt_ids(
        block_hash,
        shard_id,
        new_chunk.processed_receipts(),
        new_chunk.processed_receipt_bodies(),
    );
    chain_store_update.set_receipt_to_tx(new_chunk.receipt_to_tx());
    chain_store_update.set_outgoing_receipt(block_hash, shard_id, new_chunk.outgoing_receipts());
    chain_store_update.set_outcomes_with_proofs(
        block_hash,
        shard_id,
        new_chunk.transaction_result_for_block(),
    );
    // TODO(cloud_archival): address the rc columns a re-pull counts twice, in case we
    // need gc at the reader.
    for transaction in chunk.to_transactions() {
        let bytes = borsh::to_vec(transaction).expect("borsh cannot fail");
        update.increment_refcount(DBCol::Transactions, transaction.get_hash().as_ref(), &bytes);
    }
    for receipt in chunk.prev_outgoing_receipts() {
        let bytes = borsh::to_vec(receipt).expect("borsh cannot fail");
        update.increment_refcount(DBCol::Receipts, receipt.get_hash().as_ref(), &bytes);
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
/// whose state snapshot is stored in cloud for `shard_id`. Errors when the walk-back
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
            let header =
                cloud_storage.retrieve_state_header(epoch_height, epoch_id, shard_id).await?;
            // An epoch snapshots its sync block, a few heights into the epoch, so a
            // `height` below that block is served by the epoch under this one.
            // TODO(cloud_archival): stop here for a shard a resharding added, which is
            // reached by walking the recorded inverse changes down instead.
            if header.chunk_height_included() <= height {
                return Ok((epoch_height, epoch_id));
            }
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
