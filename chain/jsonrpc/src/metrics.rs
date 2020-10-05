use lazy_static::lazy_static;
use near_metrics::{Histogram, IntCounter, IntCounterVec};

lazy_static! {
    pub static ref RPC_PROCESSING_TIME: near_metrics::Result<Histogram> =
        near_metrics::try_create_histogram(
            "near_rpc_processing_time",
            "Time taken to process rpc queries"
        );
    pub static ref RPC_TIMEOUT_TOTAL: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "near_rpc_timeout_total",
            "Total count of rpc queries that ended on timeout"
        );
    pub static ref PROMETHEUS_REQUEST_COUNT: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "http_prometheus_requests_total",
            "Total count of Prometheus requests received"
        );
    pub static ref HTTP_RPC_REQUEST_COUNT: near_metrics::Result<IntCounterVec> =
        near_metrics::try_create_int_counter_vec(
            "http_rpc_requests_total",
            "Total count of HTTP RPC requests received, by method",
            &["method"]
        );
    pub static ref HTTP_STATUS_REQUEST_COUNT: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "http_status_requests_total",
            "Total count of HTTP Status requests received"
        );
}
