use crate::rocksdb_metrics::export_stats_as_metrics;
use crate::{NodeStorage, Store, Temperature};
use actix_rt::ArbiterHandle;
use near_o11y::metrics::{
    exponential_buckets, try_create_histogram, try_create_histogram_vec,
    try_create_histogram_with_buckets, try_create_int_counter_vec, try_create_int_gauge,
    try_create_int_gauge_vec, Histogram, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
};
use once_cell::sync::Lazy;

pub(crate) static DATABASE_OP_LATENCY_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_database_op_latency_by_op_and_column",
        "Database operations latency by operation and column.",
        &["op", "column"],
        Some(vec![0.00002, 0.0001, 0.0002, 0.0005, 0.0008, 0.001, 0.002, 0.004, 0.008, 0.1]),
    )
    .unwrap()
});

pub static CHUNK_CACHE_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_chunk_cache_hits",
        "Chunk cache hits",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static CHUNK_CACHE_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_chunk_cache_misses",
        "Chunk cache misses",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_hits",
        "Shard cache hits",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_misses",
        "Shard cache misses",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_TOO_LARGE: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_too_large",
        "Number of values to be inserted into shard cache is too large",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_shard_cache_size", "Shard cache size", &["shard_id", "is_view"])
        .unwrap()
});

pub static CHUNK_CACHE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_chunk_cache_size", "Chunk cache size", &["shard_id", "is_view"])
        .unwrap()
});

pub static SHARD_CACHE_CURRENT_TOTAL_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_shard_cache_current_total_size",
        "Shard cache current total size",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_POP_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_pop_hits",
        "Shard cache pop hits",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static SHARD_CACHE_POP_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_pop_misses",
        "Shard cache pop misses",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static SHARD_CACHE_POP_LRU: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_pop_lru",
        "Shard cache LRU pops",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static SHARD_CACHE_GC_POP_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_gc_pop_misses",
        "Shard cache gc pop misses",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static SHARD_CACHE_DELETIONS_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_shard_cache_deletions_size",
        "Shard cache deletions size",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static APPLIED_TRIE_DELETIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_applied_trie_deletions",
        "Trie deletions applied to store",
        &["shard_id"],
    )
    .unwrap()
});
pub static APPLIED_TRIE_INSERTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_applied_trie_insertions",
        "Trie insertions applied to store",
        &["shard_id"],
    )
    .unwrap()
});
pub static REVERTED_TRIE_INSERTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_reverted_trie_insertions",
        "Trie insertions reverted due to GC of forks",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_SENT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_prefetch_sent", "Prefetch requests sent to DB", &["shard_id"])
        .unwrap()
});
pub static PREFETCH_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_prefetch_hits", "Prefetched trie keys", &["shard_id"]).unwrap()
});
pub static PREFETCH_PENDING: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_pending",
        "Prefetched trie keys that were still pending when main thread needed data",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_FAIL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_fail",
        "Prefetching trie key failed with an error",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_NOT_REQUESTED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_not_requested",
        "Number of values that had to be fetched without having been prefetched",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_MEMORY_LIMIT_REACHED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_memory_limit_reached",
        "Number of values that could not be prefetched due to prefetch staging area size limitations",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_CONFLICT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_conflict",
        "Main thread retrieved value from shard_cache after a conflict with another main thread from a fork.",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_RETRY: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_retries",
        "Main thread was waiting for prefetched value but had to retry fetch afterwards.",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_STAGED_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_prefetch_staged_bytes",
        "Upper bound on memory usage for holding prefetched data.",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_STAGED_SLOTS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_prefetch_staged_slots",
        "Number of slots used in staging area.",
        &["shard_id"],
    )
    .unwrap()
});
pub static COLD_MIGRATION_READS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_cold_migration_reads",
        "Number of get calls to hot store made for every column during copying data to cold storage.",
        &["col"],
    )
    .unwrap()
});
pub static COLD_HEAD_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_cold_head_height", "Height of the head of cold storage").unwrap()
});
pub static COLD_COPY_DURATION: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_cold_copy_duration",
        "Time it takes to copy one height to cold storage",
    )
    .unwrap()
});

pub(crate) static HAS_STATE_SNAPSHOT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_has_state_snapshot", "Whether a node has a state snapshot open")
        .unwrap()
});

pub(crate) static MAKE_STATE_SNAPSHOT_ELAPSED: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_make_state_snapshot_elapsed_sec",
        "Latency of making a state snapshot, in seconds",
        exponential_buckets(0.01, 1.3, 30).unwrap(),
    )
    .unwrap()
});

pub(crate) static DELETE_STATE_SNAPSHOT_ELAPSED: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_delete_state_snapshot_elapsed_sec",
        "Latency of deleting a state snapshot, in seconds",
        exponential_buckets(0.001, 1.6, 25).unwrap(),
    )
    .unwrap()
});

pub(crate) static COMPACT_STATE_SNAPSHOT_ELAPSED: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_compact_state_snapshot_elapsed_sec",
        "Latency of compaction of a state snapshot, in seconds",
        exponential_buckets(0.001, 1.6, 40).unwrap(),
    )
    .unwrap()
});

pub(crate) static MOVE_STATE_SNAPSHOT_FLAT_HEAD_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_move_state_snapshot_flat_head_elapsed_sec",
        "Latency of moving flat head of state snapshot, in seconds",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_NODES_WITH_FS_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_get_state_part_nodes_with_fs_elapsed_sec",
        "Latency of creating a state part using flat storage given the boundaries, in seconds",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_BOUNDARIES_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_get_state_part_boundaries_elapsed_sec",
        "Latency of finding state part boundaries, in seconds",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_READ_FS_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_get_state_part_with_fs_read_fs_elapsed_sec",
        "Latency of reading FS columns, in seconds",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_LOOKUP_REF_VALUES_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_get_state_part_with_fs_lookup_value_refs_elapsed_sec",
        "Latency of looking references values, in seconds",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_CREATE_TRIE_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_get_state_part_with_fs_create_trie_elapsed_sec",
        "Latency of creation of trie from the data read from FS, in seconds",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_COMBINE_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_get_state_part_with_fs_combine_elapsed_sec",
        "Latency of combining part boundaries and in-memory created nodes, in seconds",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_WITH_FS_VALUES_INLINED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_get_state_part_with_fs_values_inlined_count",
        "Number of FS values that were inlined",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_WITH_FS_VALUES_REF: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_get_state_part_with_fs_values_ref_count",
        "Number of FS values that were references",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_WITH_FS_NODES_FROM_DISK: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_get_state_part_with_fs_nodes_from_disk_count",
        "Number of nodes in state part that are state part boundaries",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_WITH_FS_NODES_IN_MEMORY: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_get_state_part_with_fs_nodes_in_memory_count",
        "Number of nodes in state part that created based on FS values",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static GET_STATE_PART_WITH_FS_NODES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_get_state_part_with_fs_nodes_count",
        "Total number of nodes in state parts created",
        &["shard_id"],
    )
    .unwrap()
});

pub mod flat_state_metrics {
    use super::*;

    pub static FLAT_STORAGE_CREATION_STATUS: Lazy<IntGaugeVec> = Lazy::new(|| {
        try_create_int_gauge_vec(
            "flat_storage_creation_status",
            "Integer representing status of flat storage creation",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_CREATION_REMAINING_STATE_PARTS: Lazy<IntGaugeVec> = Lazy::new(|| {
        try_create_int_gauge_vec(
            "flat_storage_creation_remaining_state_parts",
            "Number of remaining state parts to fetch to fill flat storage in bytes",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_CREATION_FETCHED_STATE_PARTS: Lazy<IntCounterVec> = Lazy::new(|| {
        try_create_int_counter_vec(
            "flat_storage_creation_fetched_state_parts",
            "Number of fetched state parts to fill flat storage in bytes",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_CREATION_FETCHED_STATE_ITEMS: Lazy<IntCounterVec> = Lazy::new(|| {
        try_create_int_counter_vec(
            "flat_storage_creation_fetched_state_items",
            "Number of fetched items to fill flat storage",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_CREATION_THREADS_USED: Lazy<IntGaugeVec> = Lazy::new(|| {
        try_create_int_gauge_vec(
            "flat_storage_creation_threads_used",
            "Number of currently used threads to fetch state",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_HEAD_HEIGHT: Lazy<IntGaugeVec> = Lazy::new(|| {
        try_create_int_gauge_vec(
            "flat_storage_head_height",
            "Height of flat storage head",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_CACHED_DELTAS: Lazy<IntGaugeVec> = Lazy::new(|| {
        try_create_int_gauge_vec(
            "flat_storage_cached_deltas",
            "Number of cached deltas in flat storage",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_CACHED_CHANGES_NUM_ITEMS: Lazy<IntGaugeVec> = Lazy::new(|| {
        try_create_int_gauge_vec(
            "flat_storage_cached_changes_num_items",
            "Number of items in all cached changes in flat storage",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_CACHED_CHANGES_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
        try_create_int_gauge_vec(
            "flat_storage_cached_changes_size",
            "Total size of cached changes in flat storage",
            &["shard_id"],
        )
        .unwrap()
    });
    pub static FLAT_STORAGE_DISTANCE_TO_HEAD: Lazy<IntGaugeVec> = Lazy::new(|| {
        try_create_int_gauge_vec(
            "flat_storage_distance_to_head",
            "Distance between processed block and flat storage head",
            &["shard_id"],
        )
        .unwrap()
    });

    pub mod inlining_migration {
        use near_o11y::metrics::{
            try_create_histogram, try_create_int_counter, Histogram, IntCounter,
        };
        use once_cell::sync::Lazy;

        pub static PROCESSED_COUNT: Lazy<IntCounter> = Lazy::new(|| {
            try_create_int_counter(
                "near_flat_state_inlining_migration_processed_count",
                "Total number of processed FlatState rows since the migration start.",
            )
            .unwrap()
        });
        pub static PROCESSED_TOTAL_VALUES_SIZE: Lazy<IntCounter> = Lazy::new(|| {
            try_create_int_counter(
                "near_flat_state_inlining_migration_processed_total_values_size",
                "Total size processed FlatState values since the migration start.",
            )
            .unwrap()
        });
        pub static INLINED_COUNT: Lazy<IntCounter> = Lazy::new(|| {
            try_create_int_counter(
                "near_flat_state_inlining_migration_inlined_count",
                "Total number of inlined FlatState values since the migration start.",
            )
            .unwrap()
        });
        pub static INLINED_TOTAL_VALUES_SIZE: Lazy<IntCounter> = Lazy::new(|| {
            try_create_int_counter(
                "near_flat_state_inlining_migration_inlined_total_values_size",
                "Total size of inlined FlatState values since the migration start.",
            )
            .unwrap()
        });
        pub static SKIPPED_COUNT: Lazy<IntCounter> = Lazy::new(|| {
            try_create_int_counter(
                "near_flat_state_inlining_migration_skipped_count",
                "Total number of FlatState values skipped since the migration start due to some kind of an issue while trying to read the value.",
            )
            .unwrap()
        });
        pub static FLAT_STATE_PAUSED_DURATION: Lazy<Histogram> = Lazy::new(|| {
            try_create_histogram(
                "near_flat_state_inlining_migration_flat_state_paused_duration",
                "FlatState inlining paused duration.",
            )
            .unwrap()
        });
    }
}
pub static COLD_STORE_MIGRATION_BATCH_WRITE_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_cold_migration_initial_writes",
        "Number of write calls to cold store made for every column during initial population of cold storage.",
        &["col"],
    )
    .unwrap()
});
pub static COLD_STORE_MIGRATION_BATCH_WRITE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_cold_migration_initial_writes_time",
        "Time spent on writing initial migration batches by column.",
        &["column"],
        None,
    )
    .unwrap()
});

fn export_store_stats(store: &Store, temperature: Temperature) {
    if let Some(stats) = store.get_store_statistics() {
        tracing::debug!(target:"metrics", "Exporting the db metrics for {temperature:?} store.");
        export_stats_as_metrics(stats, temperature);
    } else {
        // TODO Does that happen under normal circumstances?
        // Should this log be a warning or error instead?
        tracing::debug!(target:"metrics", "Exporting the db metrics for {temperature:?} store failed. The statistics are missing.");
    }
}

pub fn spawn_db_metrics_loop(
    storage: &NodeStorage,
    period: std::time::Duration,
) -> anyhow::Result<ArbiterHandle> {
    tracing::debug!(target:"metrics", "Spawning the db metrics loop.");
    let db_metrics_arbiter = actix_rt::Arbiter::new();

    let start = tokio::time::Instant::now();
    let mut interval = actix_rt::time::interval_at(start, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let hot_store = storage.get_hot_store();
    let cold_store = storage.get_cold_store();

    db_metrics_arbiter.spawn(async move {
        tracing::debug!(target:"metrics", "Starting the db metrics loop.");
        loop {
            interval.tick().await;

            export_store_stats(&hot_store, Temperature::Hot);
            if let Some(cold_store) = &cold_store {
                export_store_stats(cold_store, Temperature::Cold);
            }
        }
    });

    Ok(db_metrics_arbiter.handle())
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use actix;

    use crate::db::{StatsValue, StoreStatistics};
    use crate::metadata::{DbKind, DB_VERSION};
    use crate::test_utils::create_test_node_storage_with_cold;

    use near_o11y::testonly::init_test_logger;

    use super::spawn_db_metrics_loop;

    fn stat(name: &str, count: i64) -> (String, Vec<StatsValue>) {
        (name.into(), vec![StatsValue::Count(count)])
    }

    async fn test_db_metrics_loop_impl() -> anyhow::Result<()> {
        let (storage, hot, cold) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Cold);
        let period = Duration::from_millis(100);

        let handle = spawn_db_metrics_loop(&storage, period)?;

        let hot_column_name = "hot.colum".to_string();
        let cold_column_name = "cold.colum".to_string();

        let hot_gauge_name = hot_column_name.clone() + "";
        let cold_gauge_name = cold_column_name.clone() + "_cold";

        let hot_stats = StoreStatistics { data: vec![stat(&hot_column_name, 42)] };
        let cold_stats = StoreStatistics { data: vec![stat(&cold_column_name, 52)] };

        hot.set_store_statistics(hot_stats);
        cold.set_store_statistics(cold_stats);

        actix::clock::sleep(period).await;
        for _ in 0..10 {
            let int_gauges = crate::rocksdb_metrics::get_int_gauges();

            let has_hot_gauge = int_gauges.contains_key(&hot_gauge_name);
            let has_cold_gauge = int_gauges.contains_key(&cold_gauge_name);
            if has_hot_gauge && has_cold_gauge {
                break;
            }
            actix::clock::sleep(period / 10).await;
        }

        let int_gauges = crate::rocksdb_metrics::get_int_gauges();
        tracing::debug!("int_gauges {int_gauges:#?}");

        let hot_gauge = int_gauges.get(&hot_gauge_name);
        let hot_gauge = hot_gauge.ok_or(anyhow::anyhow!("hot gauge is missing"))?;

        let cold_gauge = int_gauges.get(&cold_gauge_name);
        let cold_gauge = cold_gauge.ok_or(anyhow::anyhow!("cold gauge is missing"))?;

        assert_eq!(hot_gauge.get(), 42);
        assert_eq!(cold_gauge.get(), 52);

        handle.stop();

        Ok(())
    }

    #[test]
    fn test_db_metrics_loop() {
        init_test_logger();

        let sys = actix::System::new();
        sys.block_on(test_db_metrics_loop_impl()).expect("test impl failed");

        actix::System::current().stop();
        sys.run().unwrap();
    }
}
