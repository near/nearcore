use near_metrics::{try_create_histogram_vec, HistogramVec};
use once_cell::sync::Lazy;

pub(crate) static DATABASE_OP_LATENCY_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_database_op_latency_by_op_and_column",
        "Database operations latency by operation and column.",
        &["op", "column"],
        Some(vec![0.00002, 0.0001, 0.0002, 0.0005, 0.0008, 0.001, 0.002, 0.004, 0.008, 0.1]),
    )
    .unwrap()
});
