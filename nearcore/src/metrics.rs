use near_o11y::metrics::{
    exponential_buckets, linear_buckets, try_create_histogram_vec, try_create_int_counter_vec,
    try_create_int_gauge, try_create_int_gauge_vec, HistogramVec, IntCounterVec, IntGauge,
    IntGaugeVec,
};
use once_cell::sync::Lazy;

pub static APPLY_CHUNK_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_apply_chunk_delay_seconds",
        "Time to process a chunk. Gas used by the chunk is a metric label, rounded up to 100 teragas.",
        &["tgas_ceiling"],
        Some(linear_buckets(0.0, 0.05, 50).unwrap()),
    )
        .unwrap()
});

pub(crate) static CONFIG_CORRECT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_config_correct",
        "Are the current dynamically loadable configs correct",
    )
    .unwrap()
});

pub(crate) static COLD_STORE_COPY_RESULT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_cold_store_copy_result",
        "The result of a cold store copy iteration in the cold store loop.",
        &["copy_result"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_ITERATION_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_dump_iteration_elapsed_sec",
        "Time needed to obtain and write a part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});
pub(crate) static STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_dump_put_object_elapsed_sec",
        "Time needed to write a part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});
pub(crate) static STATE_SYNC_DUMP_OBTAIN_PART_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_dump_obtain_part_elapsed_sec",
        "Time needed to obtain a part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});
pub(crate) static STATE_SYNC_DUMP_NUM_PARTS_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_num_parts_total",
        "Total number of parts in the epoch that being dumped",
        &["shard_id"],
    )
    .unwrap()
});
pub(crate) static STATE_SYNC_DUMP_NUM_PARTS_DUMPED: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_num_parts_dumped",
        "Number of parts dumped in the epoch that is being dumped",
        &["shard_id"],
    )
    .unwrap()
});
pub(crate) static STATE_SYNC_DUMP_SIZE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_state_sync_dump_size_total",
        "Total size of parts written to S3",
        &["shard_id"],
    )
    .unwrap()
});
pub(crate) static STATE_SYNC_DUMP_EPOCH_HEIGHT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_epoch_height",
        "Epoch Height of an epoch being dumped",
        &["shard_id"],
    )
    .unwrap()
});
