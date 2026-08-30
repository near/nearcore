use crate::archive::cloud_archival_utils::{
    batch_shard_ids, install_anchors, pull_block_batch, save_reader_head, save_shard_data,
};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::Store;
use near_store::archive::cloud_storage::CloudStorage;

/// Downloads block, epoch, and per-shard chunk data covering `[start_height,
/// end_height]` from cloud storage and writes it into the local store.
///
/// Rows reach past `end_height`, since each batch is written to its own end.
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

    let mut prev_block_hash = install_anchors(store, cloud_storage, epoch_manager, start_height)?;

    let range_length = end_height - start_height + 1;
    let log_interval = std::cmp::max(cloud_storage.batch_size() as u64, range_length / 100);
    let mut next_log_at = log_interval;

    // Fetch one batch per iteration and consume all its heights, so each
    // batch blob is downloaded and decompressed once rather than per height.
    let mut height = start_height;
    while height <= end_height {
        let batch_pull = pull_block_batch(store, cloud_storage, epoch_manager, height)?;
        let shard_ids =
            batch_shard_ids(epoch_manager, &prev_block_hash, batch_pull.opening_epoch_id)?;
        for shard_id in shard_ids {
            save_shard_range(store, cloud_storage, shard_id, height, batch_pull.end_height)?;
        }
        if let Some(block_hash) = batch_pull.last_present_block_hash {
            prev_block_hash = block_hash;
        }
        height = batch_pull.end_height + 1;
        save_reader_head(store, batch_pull.end_height, prev_block_hash);
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
        // TODO(cloud_archival): handle a shard whose blob starts above this height,
        // which a bootstrap crossing a resharding hits.
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
