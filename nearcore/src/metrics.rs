use near_metrics::{linear_buckets, try_create_histogram_vec, HistogramVec};
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

pub static SECONDS_PER_PETAGAS: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_execution_seconds_per_petagas_ratio",
        "Execution time per unit of gas, measured in seconds per petagas. Ignore label 'label'.",
        &[],
        // Non-linear buckets with higher resolution around 1.0.
        Some(vec![
            0.0, 0.1, 0.2, 0.5, 0.7, 0.8, 0.9, 0.95, 0.97, 0.99, 1.0, 1.01, 1.03, 1.05, 1.1, 1.2,
            1.3, 1.5, 2.0, 5.0, 10.0,
        ]),
    )
    .unwrap()
});
