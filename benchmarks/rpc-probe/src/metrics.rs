use near_o11y::metrics::{
    HistogramVec, IntCounterVec, try_create_histogram_vec, try_create_int_counter_vec,
};
use std::sync::LazyLock;

pub static RPC_PROBE_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_rpc_probe_latency_seconds",
        "Latency of RPC probe requests in seconds",
        &["method"],
        Some(vec![0.01, 0.025, 0.05, 0.1, 0.3, 1.0, 2.5, 5.0]),
    )
    .unwrap()
});

pub static RPC_PROBE_SUCCESS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_rpc_probe_success_total",
        "Total number of successful RPC probe requests",
        &["method"],
    )
    .unwrap()
});

pub static RPC_PROBE_ERROR_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_rpc_probe_error_total",
        "Total number of failed RPC probe requests",
        &["method"],
    )
    .unwrap()
});
