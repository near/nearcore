use near_o11y::metrics::{
    exponential_buckets, linear_buckets, processing_time_buckets, try_create_histogram,
    try_create_histogram_vec, try_create_histogram_with_buckets, try_create_int_counter,
    try_create_int_gauge, try_create_int_gauge_vec, Histogram, HistogramVec, IntCounter, IntGauge,
    IntGaugeVec,
};
use near_primitives::stateless_validation::ChunkStateWitness;
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

pub static CHUNK_STATE_WITNESS_ENCODE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_encode_time",
        "State witness encoding (serialization + compression) latency in seconds",
        &["shard_id"],
        Some(linear_buckets(0.025, 0.025, 20).unwrap()),
    )
    .unwrap()
});

pub static SHADOW_CHUNK_VALIDATION_FAILED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_shadow_chunk_validation_failed_total",
        "Shadow chunk validation failures count",
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_VALIDATION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_validation_time",
        "State witness validation latency in seconds",
        &["shard_id"],
        Some(exponential_buckets(0.01, 2.0, 12).unwrap()),
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_TOTAL_SIZE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_total_size",
        "Stateless validation compressed state witness size in bytes",
        &["shard_id"],
        Some(exponential_buckets(100_000.0, 1.2, 32).unwrap()),
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_RAW_SIZE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_raw_size",
        "Stateless validation uncompressed (raw) state witness size in bytes",
        &["shard_id"],
        Some(exponential_buckets(100_000.0, 1.2, 32).unwrap()),
    )
    .unwrap()
});

pub static CHUNK_STATE_WITNESS_DECODE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_decode_time",
        "State witness decoding (decompression + deserialization) latency in seconds",
        &["shard_id"],
        Some(linear_buckets(0.025, 0.025, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_MAIN_STATE_TRANSISTION_SIZE: Lazy<HistogramVec> = Lazy::new(
    || {
        try_create_histogram_vec(
            "near_chunk_state_witness_main_state_transition_size",
            "Size of ChunkStateWitness::main_state_transition (storage proof needed to execute receipts)",
            &["shard_id"],
            Some(buckets_for_witness_field_size()),
        )
            .unwrap()
    },
);

pub(crate) static CHUNK_STATE_WITNESS_NEW_TRANSACTIONS_SIZE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_new_transactions_size",
        "Size of ChunkStateWitness::new_transactions (new proposed transactions)",
        &["shard_id"],
        Some(buckets_for_witness_field_size()),
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_NEW_TRANSACTIONS_STATE_SIZE: Lazy<HistogramVec> = Lazy::new(
    || {
        try_create_histogram_vec(
            "near_chunk_state_witness_new_transactions_state_size",
            "Size of ChunkStateWitness::new_transactions_validation_state (storage proof to validate new proposed transactions)",
            &["shard_id"],
            Some(buckets_for_witness_field_size()),
        )
            .unwrap()
    },
);

pub(crate) static CHUNK_STATE_WITNESS_SOURCE_RECEIPT_PROOFS_SIZE: Lazy<HistogramVec> =
    Lazy::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_source_receipt_proofs_size",
            "Size of ChunkStateWitness::source_receipt_proofs (incoming receipts proofs)",
            &["shard_id"],
            Some(buckets_for_witness_field_size()),
        )
        .unwrap()
    });

pub fn record_witness_size_metrics(
    decoded_size: usize,
    encoded_size: usize,
    witness: &ChunkStateWitness,
) {
    if let Err(err) = record_witness_size_metrics_fallible(decoded_size, encoded_size, witness) {
        tracing::warn!(target:"client", "Failed to record witness size metrics!, error: {}", err);
    }
}

fn record_witness_size_metrics_fallible(
    decoded_size: usize,
    encoded_size: usize,
    witness: &ChunkStateWitness,
) -> Result<(), std::io::Error> {
    let shard_id = witness.chunk_header.shard_id().to_string();
    CHUNK_STATE_WITNESS_RAW_SIZE
        .with_label_values(&[shard_id.as_str()])
        .observe(decoded_size as f64);
    CHUNK_STATE_WITNESS_TOTAL_SIZE
        .with_label_values(&[&shard_id.as_str()])
        .observe(encoded_size as f64);
    CHUNK_STATE_WITNESS_MAIN_STATE_TRANSISTION_SIZE
        .with_label_values(&[shard_id.as_str()])
        .observe(borsh::to_vec(&witness.main_state_transition)?.len() as f64);
    CHUNK_STATE_WITNESS_NEW_TRANSACTIONS_SIZE
        .with_label_values(&[&shard_id.as_str()])
        .observe(borsh::to_vec(&witness.new_transactions)?.len() as f64);
    CHUNK_STATE_WITNESS_NEW_TRANSACTIONS_STATE_SIZE
        .with_label_values(&[&shard_id.as_str()])
        .observe(borsh::to_vec(&witness.new_transactions_validation_state)?.len() as f64);
    CHUNK_STATE_WITNESS_SOURCE_RECEIPT_PROOFS_SIZE
        .with_label_values(&[&shard_id.as_str()])
        .observe(borsh::to_vec(&witness.source_receipt_proofs)?.len() as f64);
    Ok(())
}

/// Buckets from 0 to 10MB
/// Meant for measuring size of a single field inside ChunkSizeWitness.
fn buckets_for_witness_field_size() -> Vec<f64> {
    vec![
        10_000.,
        20_000.,
        50_000.,
        100_000.,
        200_000.,
        300_000.,
        500_000.,
        750_000.,
        1000_000.,
        1500_000.,
        2000_000.,
        2500_000.,
        3000_000.,
        3500_000.,
        4000_000.,
        4500_000.,
        5000_000.,
        6000_000.,
        7000_000.,
        8000_000.,
        9000_000.,
        10_000_000.,
    ]
}
