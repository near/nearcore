use std::sync::LazyLock;

use near_o11y::metrics::{
    HistogramVec, IntGaugeVec, exponential_buckets, try_create_histogram_vec,
    try_create_int_gauge_vec,
};

pub(crate) static MESSAGE_DEQUEUE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_async_message_dequeue_time",
        "Time spent in the queue before being dequeued, in seconds.",
        &["actor", "message_type"],
        Some(exponential_buckets(0.0001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static MESSAGE_PROCESSING_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_async_message_processing_time",
        "Time spent processing a message, in seconds.",
        &["actor", "message_type"],
        Some(exponential_buckets(0.0001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static QUEUE_PENDING_MESSAGES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_async_queue_pending_messages",
        "Number of pending messages in the queue.",
        &["actor"],
    )
    .unwrap()
});
