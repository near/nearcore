use near_o11y::metrics::{
    exponential_buckets, try_create_histogram_vec, try_create_int_counter_vec, HistogramVec,
    IntCounterVec,
};
use once_cell::sync::Lazy;

pub(crate) static PONG_RECEIVED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "ping_pong_received",
        "Round-trip time of ping-pong",
        &["chain_id", "account_id"],
        Some(exponential_buckets(0.00001, 1.6, 40).unwrap()),
    )
    .unwrap()
});

pub(crate) static PONG_TIMEOUTS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "ping_pong_timeout",
        "Number of pongs that were not received",
        &["chain_id", "account_id"],
    )
    .unwrap()
});

pub(crate) static PING_SENT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "ping_ping_sent",
        "Number of pings sent",
        &["chain_id", "account_id"],
    )
    .unwrap()
});
