use near_metrics::{try_create_histogram_vec, HistogramVec};
use once_cell::sync::Lazy;

pub(crate) static DATABASE_GET_LATENCY_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_database_get_latency_us",
        "Database read latency in microseconds, as a histogram",
        &["column"],
        Some(vec![20., 100., 200., 500., 800., 1000., 2000., 4000., 8000., 100000.]),
    )
    .unwrap()
});
