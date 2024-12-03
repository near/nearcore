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
