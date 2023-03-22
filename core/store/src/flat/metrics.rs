use crate::metrics::flat_state_metrics;
use near_o11y::metrics::{IntCounter, IntGauge};
use near_primitives::types::ShardId;

use super::FlatStorageStatus;

pub(crate) struct FlatStorageMetrics {
    flat_head_height: IntGauge,
    distance_to_head: IntGauge,
    cached_deltas: IntGauge,
    cached_changes_num_items: IntGauge,
    cached_changes_size: IntGauge,
}

impl FlatStorageMetrics {
    pub(crate) fn new(shard_id: ShardId) -> Self {
        let shard_id_label = shard_id.to_string();
        Self {
            flat_head_height: flat_state_metrics::FLAT_STORAGE_HEAD_HEIGHT
                .with_label_values(&[&shard_id_label]),
            distance_to_head: flat_state_metrics::FLAT_STORAGE_DISTANCE_TO_HEAD
                .with_label_values(&[&shard_id_label]),
            cached_deltas: flat_state_metrics::FLAT_STORAGE_CACHED_DELTAS
                .with_label_values(&[&shard_id_label]),
            cached_changes_num_items: flat_state_metrics::FLAT_STORAGE_CACHED_CHANGES_NUM_ITEMS
                .with_label_values(&[&shard_id_label]),
            cached_changes_size: flat_state_metrics::FLAT_STORAGE_CACHED_CHANGES_SIZE
                .with_label_values(&[&shard_id_label]),
        }
    }

    pub(crate) fn set_distance_to_head(&self, distance: usize) {
        self.distance_to_head.set(distance as i64);
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

/// Metrics reporting about flat storage creation progress on each status update.
pub struct FlatStorageCreationMetrics {
    status: IntGauge,
    flat_head_height: IntGauge,
    remaining_state_parts: IntGauge,
    fetched_state_parts: IntCounter,
    fetched_state_items: IntCounter,
    threads_used: IntGauge,
}

impl FlatStorageCreationMetrics {
    pub fn new(shard_id: ShardId) -> Self {
        let shard_id_label = shard_id.to_string();
        Self {
            status: flat_state_metrics::FLAT_STORAGE_CREATION_STATUS
                .with_label_values(&[&shard_id_label]),
            flat_head_height: flat_state_metrics::FLAT_STORAGE_HEAD_HEIGHT
                .with_label_values(&[&shard_id_label]),
            remaining_state_parts: flat_state_metrics::FLAT_STORAGE_CREATION_REMAINING_STATE_PARTS
                .with_label_values(&[&shard_id_label]),
            fetched_state_parts: flat_state_metrics::FLAT_STORAGE_CREATION_FETCHED_STATE_PARTS
                .with_label_values(&[&shard_id_label]),
            fetched_state_items: flat_state_metrics::FLAT_STORAGE_CREATION_FETCHED_STATE_ITEMS
                .with_label_values(&[&shard_id_label]),
            threads_used: flat_state_metrics::FLAT_STORAGE_CREATION_THREADS_USED
                .with_label_values(&[&shard_id_label]),
        }
    }

    pub fn set_status(&self, status: &FlatStorageStatus) {
        self.status.set(status.into());
    }

    pub fn set_flat_head_height(&self, height: u64) {
        self.flat_head_height.set(height as i64);
    }

    pub fn set_remaining_state_parts(&self, remaining_parts: u64) {
        self.remaining_state_parts.set(remaining_parts as i64);
    }

    pub fn threads_used(&self) -> IntGauge {
        self.threads_used.clone()
    }

    pub fn inc_fetched_state(&self, num_items: u64) {
        self.fetched_state_items.inc_by(num_items);
        self.fetched_state_parts.inc();
    }
}
