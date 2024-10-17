use near_o11y::metrics::{
    exponential_buckets, try_create_histogram_vec, try_create_int_counter_vec, HistogramVec,
    IntCounterVec,
};
use std::sync::LazyLock;

pub(crate) static PONG_RECEIVED: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_ping_pong_received",
        "Round-trip time of ping-pong",
        &["chain_id", "account_id"],
        Some(exponential_buckets(0.00001, 1.6, 40).unwrap()),
    )
    .unwrap()
});

pub(crate) static PONG_TIMEOUTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_ping_pong_timeout",
        "Number of pongs that were not received",
        &["chain_id", "account_id"],
    )
    .unwrap()
});

pub(crate) static PING_SENT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_ping_ping_sent",
        "Number of pings sent",
        &["chain_id", "account_id"],
    )
    .unwrap()
});
