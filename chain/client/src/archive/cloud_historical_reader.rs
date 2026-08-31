use crate::archive::cloud_archival_utils::{
    find_present_block_at_or_below, pull_block_batch, pull_epoch_data, save_reader_head,
    save_shard_data,
};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::Store;
use near_store::archive::cloud_storage::CloudStorage;

/// Downloads block, epoch, and per-shard chunk data covering `[start_height,
/// end_height]` from cloud storage and writes it into the local store.
///
/// Block rows reach a little further on both sides: the walk starts at the nearest
/// present block at or below `start_height`, and each batch is written to its own end.
pub fn bootstrap_range(
    store: &Store,
    cloud_storage: &CloudStorage,
    epoch_manager: &dyn EpochManagerAdapter,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        start_height <= end_height,
        "start_height ({}) must be <= end_height ({})",
        start_height,
        end_height,
    );

    // `start_height` may carry no block, so the block walk starts at the nearest present
    // block below it and pulls that block's epoch.
    let (block_start_height, block) = find_present_block_at_or_below(cloud_storage, start_height)?;
    let epoch_id = *block.block().header().epoch_id();
    let epoch_data = pull_epoch_data(store, cloud_storage, &epoch_id)?;

    let range_length = end_height - block_start_height + 1;
    let log_interval = std::cmp::max(cloud_storage.batch_size() as u64, range_length / 100);
    let mut next_log_at = log_interval;

    // Fetch one batch per iteration and consume all its heights, so each
    // batch blob is downloaded and decompressed once rather than per height.
    let mut height = block_start_height;
    while height <= end_height {
        let batch_end = pull_block_batch(store, cloud_storage, epoch_manager, height)?;
        // A presence marker, so far: nothing reads the height it names.
        // TODO(cloud_archival): resume from it, counting the shard rows too.
        save_reader_head(store, batch_end);
        height = batch_end + 1;
        // Capped: a batch runs to its own end, which can be past `end_height`.
        let done = std::cmp::min(height - block_start_height, range_length);
        if done >= next_log_at || height > end_height {
            next_log_at = done + log_interval;
            let percent_done = done * 100 / range_length;
            tracing::info!(height, end_height, percent_done, "bootstrap progress");
        }
    }

    // Reconstruct chunks over the requested range.
    // TODO(cloud_archival): support resharding; the layout is read once, so a
    // mid-range layout change would iterate the wrong shards.
    let shard_layout = epoch_data.shard_layout().clone();
    for shard_id in shard_layout.shard_ids() {
        save_shard_range(store, cloud_storage, shard_id, start_height, end_height)?;
    }

    Ok(())
}

/// Writes one shard's data across `[start_height, end_height]`.
fn save_shard_range(
    store: &Store,
    cloud_storage: &CloudStorage,
    shard_id: ShardId,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> anyhow::Result<()> {
    let mut height = start_height;
    while height <= end_height {
        let batch = cloud_storage.get_shard_batch_for_height(height, shard_id)?;
        let last_in_batch = std::cmp::min(batch.end_height(), end_height);
        let mut update = store.store_update();
        for h in height..=last_in_batch {
            if let Some(shard_data) = batch.get_data_at_height(h) {
                save_shard_data(&mut update, shard_id, shard_data);
            }
        }
        update.commit();
        height = last_in_batch + 1;
    }
    Ok(())
}
