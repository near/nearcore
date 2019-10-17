use near_metrics::IntCounter;

lazy_static! {
    pub static ref PROMETHEUS_REQUEST_COUNT: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "http_prometheus_requests_total",
            "Total count of Prometheus requests received"
        );
    pub static ref HTTP_RPC_REQUEST_COUNT: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "http_rpc_requests_total",
            "Total count of HTTP RPC requests received"
        );
    pub static ref HTTP_STATUS_REQUEST_COUNT: near_metrics::Result<IntCounter> =
        near_metrics::try_create_int_counter(
            "http_status_requests_total",
            "Total count of HTTP Status requests received"
        );
}
