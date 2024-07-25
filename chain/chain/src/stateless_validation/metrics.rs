use near_o11y::metrics::{
    exponential_buckets, linear_buckets, try_create_histogram_vec, try_create_int_counter,
    try_create_int_gauge, HistogramVec, IntCounter, IntGauge,
};
use near_primitives::stateless_validation::ChunkStateWitness;
use std::sync::LazyLock;

pub static SAVE_LATEST_WITNESS_GENERATE_UPDATE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_save_latest_witness_generate_update_time",
        "Time taken to generate an update of latest witnesses",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static SAVE_LATEST_WITNESS_COMMIT_UPDATE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_save_latest_witness_commit_update_time",
        "Time taken to commit the update of latest witnesses",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static SAVED_LATEST_WITNESSES_COUNT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_saved_latest_witnesses_count",
        "Total number of saved latest witnesses",
    )
    .unwrap()
});
pub static SAVED_LATEST_WITNESSES_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_saved_latest_witnesses_size",
        "Total size of saved latest witnesses (in bytes)",
    )
    .unwrap()
});

pub static CHUNK_STATE_WITNESS_ENCODE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_encode_time",
        "State witness encoding (serialization + compression) latency in seconds",
        &["shard_id"],
        Some(linear_buckets(0.025, 0.025, 20).unwrap()),
    )
    .unwrap()
});

pub static SHADOW_CHUNK_VALIDATION_FAILED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_shadow_chunk_validation_failed_total",
        "Shadow chunk validation failures count",
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_VALIDATION_TIME: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_validation_time",
            "State witness validation latency in seconds",
            &["shard_id"],
            Some(exponential_buckets(0.01, 2.0, 12).unwrap()),
        )
        .unwrap()
    });

pub(crate) static CHUNK_STATE_WITNESS_TOTAL_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_total_size",
        "Stateless validation compressed state witness size in bytes",
        &["shard_id"],
        Some(exponential_buckets(100_000.0, 1.2, 32).unwrap()),
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_RAW_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_raw_size",
        "Stateless validation uncompressed (raw) state witness size in bytes",
        &["shard_id"],
        Some(exponential_buckets(100_000.0, 1.2, 32).unwrap()),
    )
    .unwrap()
});

pub static CHUNK_STATE_WITNESS_DECODE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_decode_time",
        "State witness decoding (decompression + deserialization) latency in seconds",
        &["shard_id"],
        Some(linear_buckets(0.025, 0.025, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_MAIN_STATE_TRANSISTION_SIZE: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_main_state_transition_size",
            "Size of ChunkStateWitness::main_state_transition (storage proof needed to execute receipts)",
            &["shard_id"],
            Some(buckets_for_witness_field_size()),
        )
            .unwrap()
    });

pub(crate) static CHUNK_STATE_WITNESS_NEW_TRANSACTIONS_SIZE: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_new_transactions_size",
            "Size of ChunkStateWitness::new_transactions (new proposed transactions)",
            &["shard_id"],
            Some(buckets_for_witness_field_size()),
        )
        .unwrap()
    });

pub(crate) static CHUNK_STATE_WITNESS_NEW_TRANSACTIONS_STATE_SIZE: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_new_transactions_state_size",
            "Size of ChunkStateWitness::new_transactions_validation_state (storage proof to validate new proposed transactions)",
            &["shard_id"],
            Some(buckets_for_witness_field_size()),
        )
            .unwrap()
    });

pub(crate) static CHUNK_STATE_WITNESS_SOURCE_RECEIPT_PROOFS_SIZE: LazyLock<HistogramVec> =
    LazyLock::new(|| {
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
