use near_o11y::metrics::{
    try_create_int_counter, try_create_int_counter_vec, IntCounter, IntCounterVec,
};
use once_cell::sync::Lazy;

pub static ACTION_CALLED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_action_called_count",
        "Number of times given action has been called since starting this node",
        &["action"],
    )
    .unwrap()
});

pub static TRANSACTION_PROCESSED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_total",
        "The number of transactions processed since starting this node",
    )
    .unwrap()
});
pub static TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_successfully_total",
        "The number of transactions processed successfully since starting this node",
    )
    .unwrap()
});
pub static TRANSACTION_PROCESSED_FAILED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_failed_total",
        "The number of transactions processed and failed since starting this node",
    )
    .unwrap()
});
pub static PREFETCH_ENQUEUED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_enqueued",
        "Prefetch requests queued up",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_QUEUE_FULL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_queue_full",
        "Prefetch requests failed to queue up",
        &["shard_id"],
    )
    .unwrap()
});
