use near_o11y::metrics::{exponential_buckets, HistogramVec, IntCounter, IntCounterVec};
use once_cell::sync::Lazy;

pub static RPC_PROCESSING_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    near_o11y::metrics::try_create_histogram_vec(
        "near_rpc_processing_time",
        "Time taken to process rpc queries",
        &["method"],
        Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
    )
    .unwrap()
});
pub static RPC_TIMEOUT_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_counter(
        "near_rpc_timeout_total",
        "Total count of rpc queries that ended on timeout",
    )
    .unwrap()
});
pub static PROMETHEUS_REQUEST_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_counter(
        "near_http_prometheus_requests_total",
        "Total count of Prometheus requests received",
    )
    .unwrap()
});
pub static HTTP_RPC_REQUEST_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_counter_vec(
        "near_rpc_total_count",
        "Total count of HTTP RPC requests received, by method",
        &["method"],
    )
    .unwrap()
});
pub static HTTP_STATUS_REQUEST_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_counter(
        "near_http_status_requests_total",
        "Total count of HTTP Status requests received",
    )
    .unwrap()
});
pub static RPC_ERROR_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_counter_vec(
        "near_rpc_error_count",
        "Total count of errors by method and message",
        &["method", "err_code"],
    )
    .unwrap()
});
pub static RPC_UNREACHABLE_ERROR_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_counter_vec(
        "near_rpc_unreachable_errors_total",
        "Total count of Unreachable RPC errors returned, by target error enum",
        &["target_error_enum"],
    )
    .unwrap()
});
