use super::{FlatStorageManager, FlatStorageStatus};
use crate::metrics::flat_state_metrics;
use near_o11y::metrics::IntGauge;
use near_primitives::{shard_layout::ShardUId, types::BlockHeight};

pub(crate) struct FlatStorageMetrics {
    flat_head_height: IntGauge,
    distance_to_head: IntGauge,
    hops_to_head: IntGauge,
    cached_deltas: IntGauge,
    cached_changes_num_items: IntGauge,
    cached_changes_size: IntGauge,
}

impl FlatStorageMetrics {
    pub(crate) fn new(shard_uid: ShardUId) -> Self {
        let shard_uid_label = shard_uid.to_string();
        Self {
            flat_head_height: flat_state_metrics::FLAT_STORAGE_HEAD_HEIGHT
                .with_label_values(&[&shard_uid_label]),
            distance_to_head: flat_state_metrics::FLAT_STORAGE_DISTANCE_TO_HEAD
                .with_label_values(&[&shard_uid_label]),
            hops_to_head: flat_state_metrics::FLAT_STORAGE_HOPS_TO_HEAD
                .with_label_values(&[&shard_uid_label]),
            cached_deltas: flat_state_metrics::FLAT_STORAGE_CACHED_DELTAS
                .with_label_values(&[&shard_uid_label]),
            cached_changes_num_items: flat_state_metrics::FLAT_STORAGE_CACHED_CHANGES_NUM_ITEMS
                .with_label_values(&[&shard_uid_label]),
            cached_changes_size: flat_state_metrics::FLAT_STORAGE_CACHED_CHANGES_SIZE
                .with_label_values(&[&shard_uid_label]),
        }
    }

    pub(crate) fn set_distance_to_head(&self, distance: usize, height: Option<BlockHeight>) {
        self.distance_to_head.set(height.unwrap_or(0) as i64);
        self.hops_to_head.set(distance as i64);
    }

    pub(crate) fn set_flat_head_height(&self, height: u64) {
        self.flat_head_height.set(height as i64);
    }

    pub(crate) fn set_cached_deltas(
        &self,
        cached_deltas: usize,
        cached_changes_num_items: usize,
        cached_changes_size: u64,
    ) {
        self.cached_deltas.set(cached_deltas as i64);
        self.cached_changes_num_items.set(cached_changes_num_items as i64);
        self.cached_changes_size.set(cached_changes_size as i64);
    }
}

/// Metrics for flat storage resharding.
///
/// This struct is a collection of metrics to monitor the operation of splitting a shard.
pub struct FlatStorageReshardingShardSplitMetrics {
    parent_shard: ShardUId,
    left_child_shard: ShardUId,
    right_child_shard: ShardUId,
    parent_status: IntGauge,
    left_child_status: IntGauge,
    right_child_status: IntGauge,
    split_shard_processed_batches: IntGauge,
    split_shard_batch_size: IntGauge,
    split_shard_processed_bytes: IntGauge,
}

impl FlatStorageReshardingShardSplitMetrics {
    pub fn new(
        parent_shard: ShardUId,
        left_child_shard: ShardUId,
        right_child_shard: ShardUId,
    ) -> Self {
        use flat_state_metrics::*;
        let parent_shard_label = parent_shard.to_string();
        let left_child_shard_label = left_child_shard.to_string();
        let right_child_shard_label = right_child_shard.to_string();
        Self {
            parent_shard,
            left_child_shard,
            right_child_shard,
            parent_status: resharding::STATUS.with_label_values(&[&parent_shard_label]),
            left_child_status: resharding::STATUS.with_label_values(&[&left_child_shard_label]),
            right_child_status: resharding::STATUS.with_label_values(&[&right_child_shard_label]),
            split_shard_processed_batches: resharding::SPLIT_SHARD_PROCESSED_BATCHES
                .with_label_values(&[&parent_shard_label]),
            split_shard_batch_size: resharding::SPLIT_SHARD_BATCH_SIZE.clone(),
            split_shard_processed_bytes: resharding::SPLIT_SHARD_PROCESSED_BYTES
                .with_label_values(&[&parent_shard_label]),
        }
    }

    pub fn set_parent_status(&self, status: &FlatStorageStatus) {
        self.parent_status.set(status.into());
    }

    pub fn set_left_child_status(&self, status: &FlatStorageStatus) {
        self.left_child_status.set(status.into());
    }

    pub fn set_right_child_status(&self, status: &FlatStorageStatus) {
        self.right_child_status.set(status.into());
    }

    pub fn set_split_shard_processed_batches(&self, num_batches: usize) {
        self.split_shard_processed_batches.set(num_batches as i64);
    }

    pub fn update_shards_status(&self, manager: &FlatStorageManager) {
        self.set_parent_status(&manager.get_flat_storage_status(self.parent_shard));
        self.set_left_child_status(&manager.get_flat_storage_status(self.left_child_shard));
        self.set_right_child_status(&manager.get_flat_storage_status(self.right_child_shard));
    }

    pub fn set_split_shard_batch_size(&self, batch_size: usize) {
        self.split_shard_batch_size.set(batch_size as i64);
    }

    pub fn set_split_shard_processed_bytes(&self, bytes: usize) {
        self.split_shard_processed_bytes.set(bytes as i64);
    }

    pub fn inc_split_shard_processed_bytes_by(&self, processed_bytes: usize) {
        self.split_shard_processed_bytes.add(processed_bytes as i64);
    }
}

/// Metrics for flat storage resharding.
///
/// This struct is a collection of metrics to monitor the catch up phase of a new shard.
pub struct FlatStorageReshardingShardCatchUpMetrics {
    status: IntGauge,
    head_height: IntGauge,
}

impl FlatStorageReshardingShardCatchUpMetrics {
    pub fn new(shard_uid: &ShardUId) -> Self {
        use flat_state_metrics::*;
        let shard_label = shard_uid.to_string();
        Self {
            status: resharding::STATUS.with_label_values(&[&shard_label]),
            head_height: FLAT_STORAGE_HEAD_HEIGHT.with_label_values(&[&shard_label]),
        }
    }

    pub fn set_status(&self, status: &FlatStorageStatus) {
        self.status.set(status.into());
    }

    pub fn set_head_height(&self, height: u64) {
        self.head_height.set(height as i64);
    }
}
