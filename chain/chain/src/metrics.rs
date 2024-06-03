use near_o11y::metrics::{
    exponential_buckets, processing_time_buckets, try_create_histogram, try_create_histogram_vec,
    try_create_histogram_with_buckets, try_create_int_counter, try_create_int_gauge,
    try_create_int_gauge_vec, Histogram, HistogramVec, IntCounter, IntGauge, IntGaugeVec,
};
use once_cell::sync::Lazy;

pub static BLOCK_PROCESSING_ATTEMPTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_block_processing_attempts_total",
        "Total number of block processing attempts. The most common reason for aborting block processing is missing chunks",
    )
    .unwrap()
});
pub static BLOCK_PROCESSED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_block_processed_total", "Total number of blocks processed")
        .unwrap()
});
pub static BLOCK_PROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_block_processing_time", 
        "Time taken to process blocks successfully, from when a block is ready to be processed till when the processing is finished. Measures only the time taken by the successful attempts of block processing", 
        processing_time_buckets()
    ).unwrap()
});
pub static BLOCK_PREPROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_preprocessing_time", "Time taken to preprocess blocks, only include the time when the preprocessing is successful")
        .unwrap()
});
pub static BLOCK_POSTPROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_postprocessing_time", "Time taken to postprocess blocks")
        .unwrap()
});
pub static BLOCK_HEIGHT_HEAD: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_block_height_head", "Height of the current head of the blockchain")
        .unwrap()
});
pub static BLOCK_ORDINAL_HEAD: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_block_ordinal_head", "Ordinal of the current head of the blockchain")
        .unwrap()
});
pub static VALIDATOR_AMOUNT_STAKED: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_validators_stake_total",
        "The total stake of all active validators during the last block",
    )
    .unwrap()
});
pub static VALIDATOR_ACTIVE_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_validator_active_total",
        "The total number of validators active after last block",
    )
    .unwrap()
});
pub static NUM_ORPHANS: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_num_orphans", "Number of orphan blocks.").unwrap());
pub static HEADER_HEAD_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_header_head_height", "Height of the header head").unwrap()
});
pub static BOOT_TIME_SECONDS: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_boot_time_seconds",
        "Unix timestamp in seconds of the moment the client was started",
    )
    .unwrap()
});
pub static TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_tail_height", "Height of tail").unwrap());
pub static CHUNK_TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_chunk_tail_height", "Height of chunk tail").unwrap());
pub static FORK_TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_fork_tail_height", "Height of fork tail").unwrap());
pub static GC_STOP_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_gc_stop_height", "Target height of gc").unwrap());
pub static CHUNK_RECEIVED_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_receive_delay_seconds",
        "Delay between requesting and receiving a chunk.",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static BLOCK_ORPHANED_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_orphaned_delay", "How long blocks stay in the orphan pool")
        .unwrap()
});
pub static BLOCK_MISSING_CHUNKS_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_block_missing_chunks_delay",
        "How long blocks stay in the missing chunks pool",
    )
    .unwrap()
});
pub static STATE_PART_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_part_elapsed_sec",
        "Time needed to create a state part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static NUM_INVALID_BLOCKS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_num_invalid_blocks", "Number of invalid blocks", &["error"])
        .unwrap()
});
pub(crate) static SCHEDULED_CATCHUP_BLOCK: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_catchup_scheduled_block_height",
        "Tracks the progress of blocks catching up",
    )
    .unwrap()
});
pub(crate) static LARGEST_TARGET_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_largest_target_height",
        "The largest height for which we sent an approval (or skip)",
    )
    .unwrap()
});
pub(crate) static LARGEST_THRESHOLD_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_largest_threshold_height",
        "The largest height where we got enough approvals",
    )
    .unwrap()
});
pub(crate) static LARGEST_APPROVAL_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_largest_approval_height",
        "The largest height for which we've got at least one approval",
    )
    .unwrap()
});
pub(crate) static LARGEST_FINAL_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_largest_final_height",
        "Largest height for which we saw a block containing 1/2 endorsements in it",
    )
    .unwrap()
});

pub(crate) enum ReshardingStatus {
    /// The ReshardingRequest was send to the SyncJobsActor.
    Scheduled,
    /// The SyncJobsActor is performing the resharding.
    BuildingState,
    /// The resharding is finished.
    Finished,
    /// The resharding failed. Manual recovery is necessary!
    Failed,
}

impl From<ReshardingStatus> for i64 {
    /// Converts status to integer to export to prometheus later.
    /// Cast inside enum does not work because it is not fieldless.
    fn from(value: ReshardingStatus) -> Self {
        match value {
            ReshardingStatus::Scheduled => 0,
            ReshardingStatus::BuildingState => 1,
            ReshardingStatus::Finished => 2,
            ReshardingStatus::Failed => -1,
        }
    }
}

pub(crate) static SHARD_LAYOUT_VERSION: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_shard_layout_version",
        "The version of the shard layout of the current head.",
    )
    .unwrap()
});

pub(crate) static SHARD_LAYOUT_NUM_SHARDS: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_shard_layout_num_shards",
        "The number of shards in the shard layout of the current head.",
    )
    .unwrap()
});

pub(crate) static RESHARDING_BATCH_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_resharding_batch_count",
        "The number of batches committed to the db.",
        &["shard_uid"],
    )
    .unwrap()
});

pub(crate) static RESHARDING_BATCH_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_resharding_batch_size",
        "The size of batches committed to the db.",
        &["shard_uid"],
    )
    .unwrap()
});

pub static RESHARDING_BATCH_PREPARE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_resharding_batch_prepare_time",
        "Time needed to prepare a batch in resharding.",
        &["shard_uid"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub static RESHARDING_BATCH_APPLY_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_resharding_batch_apply_time",
        "Time needed to apply a batch in resharding.",
        &["shard_uid"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub static RESHARDING_BATCH_COMMIT_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_resharding_batch_commit_time",
        "Time needed to commit a batch in resharding.",
        &["shard_uid"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static RESHARDING_STATUS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_resharding_status",
        "The status of the resharding process.",
        &["shard_uid"],
    )
    .unwrap()
});

pub static SAVE_LATEST_WITNESS_GENERATE_UPDATE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_save_latest_witness_generate_update_time",
        "Time taken to generate an update of latest witnesses",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static SAVE_LATEST_WITNESS_COMMIT_UPDATE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_save_latest_witness_commit_update_time",
        "Time taken to commit the update of latest witnesses",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static SAVED_LATEST_WITNESSES_COUNT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_saved_latest_witnesses_count",
        "Total number of saved latest witnesses",
    )
    .unwrap()
});
pub static SAVED_LATEST_WITNESSES_SIZE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_saved_latest_witnesses_size",
        "Total size of saved latest witnesses (in bytes)",
    )
    .unwrap()
});
