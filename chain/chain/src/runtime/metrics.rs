use near_o11y::metrics::{
    exponential_buckets, linear_buckets, processing_time_buckets, try_create_histogram_vec,
    try_create_int_gauge_vec, HistogramVec, IntGaugeVec,
};

use once_cell::sync::Lazy;

pub(crate) static APPLY_CHUNK_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_apply_chunk_delay_seconds",
        "Time to process a chunk. Gas used by the chunk is a metric label, rounded up to 100 teragas.",
        &["tgas_ceiling"],
        Some(linear_buckets(0.0, 0.05, 50).unwrap()),
    )
        .unwrap()
});

pub(crate) static DELAYED_RECEIPTS_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_delayed_receipts_count",
        "The count of the delayed receipts. Indicator of congestion.",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static PREPARE_TX_SIZE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_prepare_tx_size",
        "Sum of transaction sizes per produced chunk, as a histogram",
        &["shard_id"],
        // Maximum is < 14MB, typical values are unknown right now so buckets
        // might need to be adjusted later when we have collected data
        Some(vec![1_000.0, 10_000., 100_000., 500_000., 1e6, 2e6, 4e6, 8e6, 12e6]),
    )
    .unwrap()
});

pub static APPLYING_CHUNKS_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_applying_chunks_time",
        "Time taken to apply chunks per shard",
        &["shard_id"],
        Some(processing_time_buckets()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_OBTAIN_PART_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_obtain_part_delay_sec",
        "Latency of applying a state part",
        &["shard_id", "result"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_APPLY_PART_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_apply_part_delay_sec",
        "Latency of applying a state part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});
