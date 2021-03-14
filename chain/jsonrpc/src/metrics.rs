use lazy_static::lazy_static;
use near_metrics::{HistogramVec, IntCounter, IntCounterVec};

lazy_static! {
    pub static ref RPC_PROCESSING_TIME: near_metrics::Result<HistogramVec> =
        near_metrics::try_create_histogram_vec(
            "near_rpc_processing_time",
            "Time taken to process rpc queries",
            &["method"],
            Some(prometheus::exponential_buckets(0.001, 2.0, 16).unwrap())
        );
    pub static ref RPC_TIMEOUT_TOTAL: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "near_rpc_timeout_total",
            "Total count of rpc queries that ended on timeout"
        );
    pub static ref PROMETHEUS_REQUEST_COUNT: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "near_http_prometheus_requests_total",
            "Total count of Prometheus requests received"
        );
    pub static ref HTTP_RPC_REQUEST_COUNT: near_metrics::Result<IntCounterVec> =
        near_metrics::try_create_int_counter_vec(
            "near_rpc_total_count",
            "Total count of HTTP RPC requests received, by method",
            &["method"]
        );
    pub static ref HTTP_STATUS_REQUEST_COUNT: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "near_http_status_requests_total",
            "Total count of HTTP Status requests received"
        );
    pub static ref RPC_ERROR_COUNT: near_metrics::Result<IntCounterVec> =
        near_metrics::try_create_int_counter_vec(
            "near_rpc_error_count",
            "Total count of errors by method and message",
            &["method", "err_code"]
        );
}
