use crate::archive::cloud_archival_utils::{
    install_anchors, pull_block_batch, pull_shard_batch, save_reader_head, shards_tracked_in_batch,
};
use near_chain_configs::TrackedShardsConfig;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::types::BlockHeight;
use near_store::Store;
use near_store::archive::cloud_storage::CloudStorage;

/// Downloads block, epoch, and per-shard chunk data covering `[start_height,
/// end_height]` from cloud storage and writes it into the local store.
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

    let mut prev_block_hash =
        install_anchors(store, cloud_storage, epoch_manager, start_height).await?;

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
            pull_shard_batch(store, cloud_storage, shard_uid, height).await?;
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
