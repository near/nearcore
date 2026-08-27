use crate::archive::cloud_archival_utils::{save_block_data, save_new_epoch, save_shard_data};
use anyhow::bail;
use near_primitives::types::{BlockHeight, EpochId, ShardId};
use near_store::Store;
use near_store::adapter::StoreUpdateAdapter;
use near_store::archive::cloud_storage::{CloudStorage, EpochData};
use std::collections::HashSet;

/// Downloads block, epoch, and per-shard chunk data for `[start_height,
/// end_height]` from cloud storage and writes it into the local store.
pub fn bootstrap_range(
    store: &Store,
    cloud_storage: &CloudStorage,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> anyhow::Result<()> {
    let mut saved_epochs = HashSet::<EpochId>::new();
    let mut first_epoch_data: Option<EpochData> = None;

    let range_length = end_height - start_height + 1;
    let log_interval = std::cmp::max(cloud_storage.batch_size() as u64, range_length / 100);

    // Fetch one batch per iteration and consume all its heights, so each
    // batch blob is downloaded and decompressed once rather than per height.
    let mut height = start_height;
    while height <= end_height {
        let batch = cloud_storage.get_block_batch_for_height(height)?;
        let last_in_batch = std::cmp::min(batch.end_height(), end_height);
        let mut update = store.store_update();
        for h in height..=last_in_batch {
            let Some(block_data) = batch.get_block_at_height(h) else {
                continue;
            };
            let epoch_id = *block_data.block().header().epoch_id();
            if saved_epochs.insert(epoch_id) {
                let epoch_data = save_new_epoch(store, cloud_storage, &epoch_id)?;
                first_epoch_data.get_or_insert(epoch_data);
            }
            save_block_data(&mut update, block_data);
            if (h - start_height).is_multiple_of(log_interval) || h == end_height {
                let percent_done = (h - start_height + 1) * 100 / range_length;
                tracing::info!(height = h, end_height, percent_done, "bootstrap progress");
            }
        }
        // A presence marker, so far: nothing reads the height it names.
        // TODO(cloud_archival): resume from it, counting the shard rows too.
        update.cloud_archival_store_update().set_reader_head(last_in_batch);
        update.commit();
        height = last_in_batch + 1;
    }

    let Some(first_epoch_data) = first_epoch_data else {
        bail!("no block found in [{start_height}, {end_height}]");
    };

    // Reconstruct chunks over the requested range.
    // TODO(cloud_archival): support resharding; the layout is read once, so a
    // mid-range layout change would iterate the wrong shards.
    let shard_layout = first_epoch_data.shard_layout().clone();
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
