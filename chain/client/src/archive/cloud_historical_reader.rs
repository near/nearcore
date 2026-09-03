use crate::archive::cloud_archival_utils::{
    CloudArchivalReaderError, apply_batch_state_changes, find_present_block_below,
    find_snapshot_at_or_before, pull_block_batch, pull_epoch_data, pull_shard_batch,
    save_reader_position, shard_state_anchor, shards_tracked_in_batch,
};
use crate::archive::cloud_reader_trie_utils::{build_shard_tries, install_state_snapshot};
use near_chain_configs::TrackedShardsConfig;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::archive::cloud_storage::{CloudRetrievalError, CloudStorage};
use near_store::{ShardTries, ShardUId, Store};

/// Downloads block, epoch, and per-shard chunk data covering `[start_height,
/// end_height]` from cloud storage and writes it into the local store. Each shard's state
/// is reconstructed as the walk goes, unless `skip_state` leaves it out.
///
/// Rows reach past `end_height`, since each batch is written to its own end.
///
/// `start_height` must be above the first archived block, since the walk is anchored on
/// the block below it.
pub async fn bootstrap_range(
    store: &Store,
    cloud_storage: &CloudStorage,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    start_height: BlockHeight,
    end_height: BlockHeight,
    skip_state: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        start_height <= end_height,
        "start_height ({}) must be <= end_height ({})",
        start_height,
        end_height,
    );
    // `tracked_shards_config` defaults to `NoShards`, so a config that never named a
    // shard bootstraps block data alone.
    if matches!(shard_tracker.tracked_shards_config(), TrackedShardsConfig::NoShards) {
        tracing::warn!("tracked_shards_config selects no shards; bootstrapping block data only");
    }

    if skip_state {
        tracing::warn!("skipping state; a query against this store answers no state request");
    }

    let tries = build_shard_tries(store);
    let mut prev_block_hash =
        install_anchors(cloud_storage, &tries, shard_tracker, start_height, skip_state).await?;

    let range_length = end_height - start_height + 1;
    let log_interval = std::cmp::max(cloud_storage.batch_size() as u64, range_length / 100);
    let mut next_log_at = log_interval;

    // Fetch one batch per iteration and consume all its heights, so each
    // batch blob is downloaded and decompressed once rather than per height.
    let mut height = start_height;
    while height <= end_height {
        let batch_pull = pull_block_batch(store, cloud_storage, epoch_manager, height).await?;
        let shard_uids = shards_tracked_in_batch(
            epoch_manager,
            shard_tracker,
            &prev_block_hash,
            batch_pull.opening_epoch_id,
        )?;
        for shard_uid in shard_uids {
            let shard_batch = pull_shard_batch(store, cloud_storage, shard_uid, height).await?;
            if skip_state {
                continue;
            }
            // TODO(cloud_archival): install a shard a resharding adds inside the range,
            // which the walk did not open on.
            let state_root = shard_state_anchor(&tries, &prev_block_hash, shard_uid)?;
            apply_batch_state_changes(&tries, shard_uid, &shard_batch, height, state_root)?;
        }
        if let Some(block_hash) = batch_pull.last_present_block_hash {
            prev_block_hash = block_hash;
        }
        height = batch_pull.end_height + 1;
        save_reader_position(store, batch_pull.end_height, prev_block_hash)?;
        // Capped: a batch runs to its own end, which can be past `end_height`.
        let done = std::cmp::min(height - start_height, range_length);
        if done >= next_log_at || height > end_height {
            next_log_at = done + log_interval;
            let percent_done = done * 100 / range_length;
            tracing::info!(height, end_height, percent_done, "bootstrap progress");
        }
    }

    Ok(())
}

/// Seeds the store with what the epoch manager needs to answer for `start_height`, with
/// the anchor block's own header, and, unless `skip_state`, with each tracked shard's
/// state. Returns the hash of the nearest present block below `start_height`, which must
/// therefore be above the first archived block.
async fn install_anchors(
    cloud_storage: &CloudStorage,
    tries: &ShardTries,
    shard_tracker: &ShardTracker,
    start_height: BlockHeight,
    skip_state: bool,
) -> Result<CryptoHash, CloudArchivalReaderError> {
    let trie_store = tries.store();
    let store = trie_store.store_ref();
    let epoch_manager = shard_tracker.epoch_manager().as_ref();
    let (prev_block_height, prev_block) =
        find_present_block_below(cloud_storage, start_height).await.map_err(|err| match err {
            CloudRetrievalError::NoBlockData { .. } => {
                CloudArchivalReaderError::NoAnchorBelow { start_height }
            }
            err => err.into(),
        })?;
    let prev_block_epoch_id = *prev_block.block().header().epoch_id();
    pull_epoch_data(store, cloud_storage, &prev_block_epoch_id).await?;

    let prev_block_hash = *prev_block.block().header().hash();
    let mut update = store.store_update();
    // `get_epoch_id_from_prev_block` starts by reading this row.
    update.epoch_store_update().set_block_info(prev_block.block_info());
    // The reader head names this block whenever the range opens on skipped heights, so
    // the store has to be able to describe it.
    update.chain_store_update().set_block_header_only(prev_block.block().header());
    update.commit();

    let start_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;
    if start_epoch_id != prev_block_epoch_id {
        pull_epoch_data(store, cloud_storage, &start_epoch_id).await?;
    }

    if skip_state {
        return Ok(prev_block_hash);
    }
    let shard_uids = shards_tracked_in_batch(epoch_manager, shard_tracker, &prev_block_hash, None)?;
    for shard_uid in shard_uids {
        install_shard_state(cloud_storage, tries, shard_uid, prev_block_height, start_height)
            .await?;
    }
    Ok(prev_block_hash)
}

/// Builds one shard's state up to `start_height`, the first height of the range the caller
/// writes rows over, from the newest snapshot at or below it. Writes the chunk extra of the
/// block at `anchor_height`, the nearest present block under the range, so the first batch
/// finds the root to apply onto.
async fn install_shard_state(
    cloud_storage: &CloudStorage,
    tries: &ShardTries,
    shard_uid: ShardUId,
    anchor_height: BlockHeight,
    start_height: BlockHeight,
) -> Result<(), CloudArchivalReaderError> {
    let shard_id = shard_uid.shard_id();
    let (epoch_height, epoch_id) =
        find_snapshot_at_or_before(cloud_storage, start_height, shard_id).await?;
    let header = cloud_storage.retrieve_state_header(epoch_height, epoch_id, shard_id).await?;
    let mut state_root = header.chunk_prev_state_root();
    let mut height = header.chunk_height_included();
    tracing::info!(
        target: "cloud_archival",
        %shard_uid,
        epoch_height,
        snapshot_height = height,
        start_height,
        "installing a shard's state out of the bucket",
    );
    install_state_snapshot(cloud_storage, tries, shard_uid, epoch_height, epoch_id, &header)
        .await?;

    while height < start_height {
        let shard_batch = cloud_storage.get_shard_batch(height, shard_id).await?;
        state_root = apply_batch_state_changes(tries, shard_uid, &shard_batch, height, state_root)?;
        height = shard_batch.end_height() + 1;
    }
    save_shard_state_anchor(tries, cloud_storage, shard_uid, anchor_height).await
}

/// Writes the chunk extra of the block at `anchor_height`. The first batch above it reads
/// the root it applies onto from that row.
async fn save_shard_state_anchor(
    tries: &ShardTries,
    cloud_storage: &CloudStorage,
    shard_uid: ShardUId,
    anchor_height: BlockHeight,
) -> Result<(), CloudArchivalReaderError> {
    let shard_id = shard_uid.shard_id();
    let shard_batch = cloud_storage.get_shard_batch(anchor_height, shard_id).await?;
    let shard_data = shard_batch
        .get_data_at_height(anchor_height)
        .ok_or(CloudRetrievalError::NoShardData { height: anchor_height, shard_id })?;
    let mut update = tries.store().store_ref().store_update();
    update.chunk_store_update().set_chunk_extra(
        shard_data.block_hash(),
        &shard_uid,
        shard_data.chunk_extra(),
    );
    update.commit();
    Ok(())
}
