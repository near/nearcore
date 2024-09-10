use near_o11y::metrics::{
    exponential_buckets, linear_buckets, processing_time_buckets, try_create_histogram_vec,
    try_create_int_gauge_vec, HistogramVec, IntGaugeVec,
};

use std::sync::LazyLock;

pub(crate) static APPLY_CHUNK_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_apply_chunk_delay_seconds",
        "Time to process a chunk. Gas used by the chunk is a metric label, rounded up to 100 teragas.",
        &["tgas_ceiling"],
        Some(linear_buckets(0.0, 0.05, 50).unwrap()),
    )
        .unwrap()
});

pub(crate) static DELAYED_RECEIPTS_COUNT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_delayed_receipts_count",
        "The count of the delayed receipts. Indicator of congestion.",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static PREPARE_TX_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
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

pub(crate) static PREPARE_TX_GAS: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_prepare_tx_gas",
        "How much gas was spent for processing new transactions when producing a chunk.",
        &["shard_id"],
        // 100e9 = 100 Ggas
        // A transaction with no actions costs 108 Ggas to process.
        // A typical function call costs ~300 Ggas.
        // The absolute maximum is defined by `max_tx_gas` = 500 Tgas.
        // This ranges from 100 Ggas to 409.6 Tgas as the last bucket boundary.
        Some(exponential_buckets(100e9, 2.0, 12).unwrap()),
    )
    .unwrap()
});

pub(crate) static PREPARE_TX_REJECTED: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_prepare_tx_rejected",
        "The number of transactions rejected when producing a chunk.",
        // possible reasons:
        // - invalid_tx             The tx failed validation or the signer has not enough funds.
        // - invalid_block_hash     The block_hash field on the tx is expired or not on the canonical chain.
        // - congestion             The receiver shard is congested.
        &["shard_id", "reason"],
        // Histogram boundaries are inclusive. Pick the first boundary below 1
        // to have 0 values as a separate bucket.
        // In exclusive boundaries, this would be equivalent to:
        // [0, 10, 100, 1_000, 10_000]
        Some(exponential_buckets(0.99999, 10.0, 6).unwrap()),
    )
    .unwrap()
});

pub(crate) static CONGESTION_PREPARE_TX_GAS_LIMIT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_congestion_prepare_tx_gas_limit",
        "How much gas the shard spends at most per chunk to convert new transactions to receipts.",
        &["shard_id"],
    )
    .unwrap()
});

pub static APPLYING_CHUNKS_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_applying_chunks_time",
        "Time taken to apply chunks per shard",
        &["apply_reason", "shard_id"],
        Some(processing_time_buckets()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_OBTAIN_PART_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_state_sync_obtain_part_delay_sec",
        "Latency of applying a state part",
        &["shard_id", "result"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_APPLY_PART_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_state_sync_apply_part_delay_sec",
        "Latency of applying a state part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});
