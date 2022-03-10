use near_metrics::{try_create_histogram_vec, HistogramVec};
use once_cell::sync::Lazy;

pub static APPLY_CHUNK_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_apply_chunk_delay",
        "Time process a chunk. Tgas used by the chunk is a metric label.",
        &["tgas_ceiling"],
        Some(prometheus::linear_buckets(0.0, 0.05, 50).unwrap()),
    )
    .unwrap()
});

pub static SECS_PER_TGAS: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_execution_secs_per_tgas",
        "Execution time per teragas. Ingore label 'label'.",
        &["label"],
        Some(prometheus::linear_buckets(0.0, 0.05, 50).unwrap()),
    )
    .unwrap()
});
