use near_o11y::metrics::{
    exponential_buckets, try_create_histogram, try_create_histogram_vec, try_create_int_counter,
    try_create_int_gauge, Histogram, HistogramVec, IntCounter, IntGauge,
};
use once_cell::sync::Lazy;

pub(crate) static BLOCK_PROCESSING_ATTEMPTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_block_processing_attempts_total",
        "Total number of block processing attempts. The most common reason for aborting block processing is missing chunks",
    )
    .unwrap()
});

pub(crate) static BLOCK_PROCESSED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_block_processed_total", "Total number of blocks processed")
        .unwrap()
});

pub(crate) static BLOCK_PROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_processing_time", "Time taken to process blocks successfully, from when a block is ready to be processed till when the processing is finished. Measures only the time taken by the successful attempts of block processing")
        .unwrap()
});

pub(crate) static APPLYING_CHUNKS_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_applying_chunks_time",
        "Time taken to apply chunks per shard",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static BLOCK_PREPROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_preprocessing_time", "Time taken to preprocess blocks, only include the time when the preprocessing is successful")
        .unwrap()
});

pub(crate) static BLOCK_POSTPROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_postprocessing_time", "Time taken to postprocess blocks")
        .unwrap()
});

pub(crate) static BLOCK_HEIGHT_HEAD: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_block_height_head", "Height of the current head of the blockchain")
        .unwrap()
});

pub(crate) static BLOCK_ORDINAL_HEAD: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_block_ordinal_head", "Ordinal of the current head of the blockchain")
        .unwrap()
});

pub(crate) static VALIDATOR_AMOUNT_STAKED: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_validators_stake_total",
        "The total stake of all active validators during the last block",
    )
    .unwrap()
});

pub(crate) static VALIDATOR_ACTIVE_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_validator_active_total",
        "The total number of validators active after last block",
    )
    .unwrap()
});

pub(crate) static NUM_ORPHANS: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_num_orphans", "Number of orphan blocks.").unwrap());

pub(crate) static HEADER_HEAD_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_header_head_height", "Height of the header head").unwrap()
});

pub(crate) static BOOT_TIME_SECONDS: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_boot_time_seconds",
        "Unix timestamp in seconds of the moment the client was started",
    )
    .unwrap()
});

pub(crate) static TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_tail_height", "Height of tail").unwrap());

pub(crate) static CHUNK_TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_chunk_tail_height", "Height of chunk tail").unwrap());

pub(crate) static FORK_TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_fork_tail_height", "Height of fork tail").unwrap());

pub(crate) static GC_STOP_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_gc_stop_height", "Target height of gc").unwrap());

pub(crate) static CHUNK_RECEIVED_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_receive_delay_seconds",
        "Delay between requesting and receiving a chunk.",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static BLOCK_ORPHANED_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_orphaned_delay", "How long blocks stay in the orphan pool")
        .unwrap()
});

pub(crate) static BLOCK_MISSING_CHUNKS_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_block_missing_chunks_delay",
        "How long blocks stay in the missing chunks pool",
    )
    .unwrap()
});

pub(crate) static STATE_PART_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_part_elapsed_sec",
        "Time needed to create a state part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static NUM_INVALID_BLOCKS: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_num_invalid_blocks", "Number of invalid blocks").unwrap()
});
