use near_o11y::metrics::{HistogramVec, IntCounter, IntCounterVec, exponential_buckets};
use near_primitives::views::TxExecutionStatus;
use std::sync::LazyLock;

pub static RPC_PROCESSING_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_histogram_vec(
        "near_rpc_processing_time",
        "Time taken to process rpc queries",
        &["method"],
        Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
    )
    .unwrap()
});
pub static RPC_TIMEOUT_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_counter(
        "near_rpc_timeout_total",
        "Total count of rpc queries that ended on timeout",
    )
    .unwrap()
});
pub static PROMETHEUS_REQUEST_COUNT: LazyLock<IntCounter> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_counter(
        "near_http_prometheus_requests_total",
        "Total count of Prometheus requests received",
    )
    .unwrap()
});
pub static HTTP_RPC_REQUEST_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_counter_vec(
        "near_rpc_total_count",
        "Total count of HTTP RPC requests received, by method",
        &["method"],
    )
    .unwrap()
});
pub static HTTP_STATUS_REQUEST_COUNT: LazyLock<IntCounter> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_counter(
        "near_http_status_requests_total",
        "Total count of HTTP Status requests received",
    )
    .unwrap()
});
pub static RPC_ERROR_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_counter_vec(
        "near_rpc_error_count",
        "Total count of errors by method and message",
        &["method", "err_code"],
    )
    .unwrap()
});
pub static RPC_UNREACHABLE_ERROR_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_counter_vec(
        "near_rpc_unreachable_errors_total",
        "Total count of Unreachable RPC errors returned, by target error enum",
        &["target_error_enum"],
    )
    .unwrap()
});
pub static RPC_TX_STATUS_WAIT_UNTIL_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_counter_vec(
        "near_rpc_wait_until_count",
        "Tracks number of RPC calls using each wait_until transaction finality variant",
        &["method", "wait_until"],
    )
    .unwrap()
});

pub(crate) fn report_wait_until_metric(method: &str, wait_until: &TxExecutionStatus) {
    // Serialize wait until as a json value (json string), then remove the quotation marks around it
    // to get the string itself.
    let wait_until_string = serde_json::to_string(wait_until)
        .expect("Serde serialization of a simple enum shouldn't fail")
        .replace("\"", "");

    RPC_TX_STATUS_WAIT_UNTIL_COUNT.with_label_values(&[method, &wait_until_string]).inc();
}
