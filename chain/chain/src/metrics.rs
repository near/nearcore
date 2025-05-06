use itertools::Itertools;
use near_o11y::metrics::{
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, exponential_buckets,
    try_create_histogram, try_create_histogram_vec, try_create_histogram_with_buckets,
    try_create_int_counter, try_create_int_counter_vec, try_create_int_gauge,
    try_create_int_gauge_vec,
};
use std::sync::LazyLock;

/// Exponential buckets for both negative and positive values.
fn two_sided_exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    let positive_buckets = exponential_buckets(start, factor, count).unwrap();
    let negative_buckets = positive_buckets.clone().into_iter().map(|x| -x).rev().collect_vec();
    let mut buckets = negative_buckets;
    buckets.push(0.0);
    buckets.extend(positive_buckets);
    buckets
}

pub static BLOCK_PROCESSING_ATTEMPTS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_block_processing_attempts_total",
        "Total number of block processing attempts. The most common reason for aborting block processing is missing chunks",
    )
    .unwrap()
});
pub static BLOCK_PROCESSED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter("near_block_processed_total", "Total number of blocks processed")
        .unwrap()
});
pub static BLOCK_PROCESSING_TIME: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram_with_buckets(
        "near_block_processing_time",
        "Time taken to process blocks successfully, from when a block is ready to be processed till when the processing is finished. Measures only the time taken by the successful attempts of block processing",
        exponential_buckets(0.001, 1.6, 20).unwrap()
    ).unwrap()
});
pub static OPTIMISTIC_BLOCK_PROCESSING_TIME: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram_with_buckets(
        "near_optimistic_block_processing_time",
        "Time taken to process optimistic blocks successfully, from when a block is ready to be processed till when the processing is finished. Measures only the time taken by the successful attempts of block processing",
        exponential_buckets(0.001, 1.6, 20).unwrap()
    ).unwrap()
});
pub static BLOCK_PREPROCESSING_TIME: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram("near_block_preprocessing_time", "Time taken to preprocess blocks, only include the time when the preprocessing is successful")
        .unwrap()
});
pub static BLOCK_POSTPROCESSING_TIME: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram("near_block_postprocessing_time", "Time taken to postprocess blocks")
        .unwrap()
});
pub static BLOCK_HEIGHT_HEAD: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_block_height_head", "Height of the current head of the blockchain")
        .unwrap()
});
pub static BLOCK_ORDINAL_HEAD: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_block_ordinal_head", "Ordinal of the current head of the blockchain")
        .unwrap()
});
pub static VALIDATOR_AMOUNT_STAKED: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_validators_stake_total",
        "The total stake of all active validators during the last block",
    )
    .unwrap()
});
pub static VALIDATOR_ACTIVE_TOTAL: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_validator_active_total",
        "The total number of validators active after last block",
    )
    .unwrap()
});
pub static NUM_ORPHANS: LazyLock<IntGauge> =
    LazyLock::new(|| try_create_int_gauge("near_num_orphans", "Number of orphan blocks.").unwrap());
pub static NUM_OPTIMISTIC_ORPHANS: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_num_optimistic_orphans", "Number of optimistic orphan blocks.")
        .unwrap()
});
pub static NUM_PENDING_BLOCKS: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_num_pending_blocks",
        "Number of blocks pending execution due to optimistic blocks in processing.",
    )
    .unwrap()
});
pub static BLOCK_PENDING_EXECUTION_DELAY: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram(
        "near_block_pending_execution_delay",
        "Time taken for a block to wait in pending execution pool",
    )
    .unwrap()
});
pub static HEADER_HEAD_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_header_head_height", "Height of the header head").unwrap()
});
pub static BOOT_TIME_SECONDS: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_boot_time_seconds",
        "Unix timestamp in seconds of the moment the client was started",
    )
    .unwrap()
});
pub static TAIL_HEIGHT: LazyLock<IntGauge> =
    LazyLock::new(|| try_create_int_gauge("near_tail_height", "Height of tail").unwrap());
pub static CHUNK_TAIL_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_chunk_tail_height", "Height of chunk tail").unwrap()
});
pub static FORK_TAIL_HEIGHT: LazyLock<IntGauge> =
    LazyLock::new(|| try_create_int_gauge("near_fork_tail_height", "Height of fork tail").unwrap());
pub static GC_STOP_HEIGHT: LazyLock<IntGauge> =
    LazyLock::new(|| try_create_int_gauge("near_gc_stop_height", "Target height of gc").unwrap());
pub static CHUNK_RECEIVED_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_receive_delay_seconds",
        "Delay between requesting and receiving a chunk.",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static BLOCK_ORPHANED_DELAY: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram("near_block_orphaned_delay", "How long blocks stay in the orphan pool")
        .unwrap()
});
pub static OPTIMISTIC_BLOCK_READINESS_GAP: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram_with_buckets(
        "near_optimistic_block_readiness_gap",
        "Gap between the optimistic block readiness and receiving full block",
        two_sided_exponential_buckets(0.001, 1.6, 20),
    )
    .unwrap()
});
pub static OPTIMISTIC_BLOCK_PROCESSED_GAP: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram_with_buckets(
        "near_optimistic_block_processed_gap",
        "Gap between the end of optimistic block processing and receiving full block",
        two_sided_exponential_buckets(0.001, 1.6, 20),
    )
    .unwrap()
});
pub static BLOCK_MISSING_CHUNKS_DELAY: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram(
        "near_block_missing_chunks_delay",
        "How long blocks stay in the missing chunks pool",
    )
    .unwrap()
});
pub static STATE_PART_ELAPSED: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_state_part_elapsed_sec",
        "Time needed to create a state part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static STATE_PART_CACHE_HIT: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_state_part_cache_hit",
        "Total number of state parts served from db cache",
    )
    .unwrap()
});
pub static STATE_PART_CACHE_MISS: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_state_part_cache_miss",
        "Total number of state parts built on-demand",
    )
    .unwrap()
});
pub static NUM_INVALID_BLOCKS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec("near_num_invalid_blocks", "Number of invalid blocks", &["error"])
        .unwrap()
});
pub static NUM_INVALID_OPTIMISTIC_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_num_invalid_optimistic_blocks",
        "Number of invalid optimistic blocks",
    )
    .unwrap()
});
pub static NUM_DROPPED_OPTIMISTIC_BLOCKS_BECAUSE_OF_PROCESSED_HEIGHT: LazyLock<IntCounter> =
    LazyLock::new(|| {
        try_create_int_counter(
            "near_num_dropped_optimistic_blocks_because_of_processed_height",
            "Number of optimistic blocks dropped because the height was already processed with a full block",
        )
        .unwrap()
    });
pub static NUM_FAILED_OPTIMISTIC_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_num_failed_optimistic_blocks",
        "Number of optimistic blocks which failed to be processed",
    )
    .unwrap()
});
pub(crate) static SCHEDULED_CATCHUP_BLOCK: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_catchup_scheduled_block_height",
        "Tracks the progress of blocks catching up",
    )
    .unwrap()
});
pub(crate) static LARGEST_TARGET_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_largest_target_height",
        "The largest height for which we sent an approval (or skip)",
    )
    .unwrap()
});
pub(crate) static LARGEST_THRESHOLD_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_largest_threshold_height",
        "The largest height where we got enough approvals",
    )
    .unwrap()
});
pub(crate) static LARGEST_APPROVAL_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_largest_approval_height",
        "The largest height for which we've got at least one approval",
    )
    .unwrap()
});
pub(crate) static LARGEST_FINAL_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_largest_final_height",
        "Largest height for which we saw a block containing 1/2 endorsements in it",
    )
    .unwrap()
});

pub(crate) static SHARD_LAYOUT_VERSION: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_shard_layout_version",
        "The version of the shard layout of the current head.",
    )
    .unwrap()
});

pub(crate) static SHARD_LAYOUT_NUM_SHARDS: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_shard_layout_num_shards",
        "The number of shards in the shard layout of the current head.",
    )
    .unwrap()
});

pub(crate) static APPLY_ALL_CHUNKS_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_apply_all_chunks_time",
        "Time taken to apply all chunks in a block",
        &["block_type"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static APPLY_CHUNK_RESULTS_CACHE_HITS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_apply_chunk_results_cache_hits",
        "Total number of apply chunk result cache hits",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static APPLY_CHUNK_RESULTS_CACHE_MISSES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_apply_chunk_results_cache_misses",
        "Total number of apply chunk result cache misses",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_TRANSITION_DATA_GC_TOTAL_ENTRIES: LazyLock<IntGauge> =
    LazyLock::new(|| {
        try_create_int_gauge(
            "near_state_transition_data_gc_total_entries",
            "Number of entries in state transaction data store column",
        )
        .unwrap()
    });

pub(crate) static STATE_TRANSITION_DATA_GC_CLEARED_ENTRIES: LazyLock<IntCounter> =
    LazyLock::new(|| {
        try_create_int_counter(
            "near_state_transition_data_gc_cleared_entries",
            "Time taken to do garbage collection of state transaction data",
        )
        .unwrap()
    });

pub(crate) static STATE_TRANSITION_DATA_GC_TIME: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram_with_buckets(
        "near_state_transition_data_gc_time",
        "Time taken to do garbage collection of state transaction data",
        // This is relatively not important metrics we are adding only just in case for ease
        // of debugging, so we only use small amounts of buckets to reduce it's impact on total
        // size of all metrics.
        // Generally since gc runs each second we want state transition data gc to take less than
        // that.
        vec![0.100, 0.5, 1.0, 5.0],
    )
    .unwrap()
});
