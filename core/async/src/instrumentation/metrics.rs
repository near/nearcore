use std::sync::LazyLock;

use prometheus::{HistogramOpts, Opts, Result};
pub use prometheus::{HistogramVec, IntGaugeVec, exponential_buckets};

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

// Note: the functions below are copied from near-o11y to avoid adding a dependency on it.
// Once near-chain-primitives no longer depends on near-async, we can add near-o11y as a dependency
// and remove these functions.
fn try_create_histogram_vec(
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Option<Vec<f64>>,
) -> Result<HistogramVec> {
    let mut opts = HistogramOpts::new(name, help);
    if let Some(buckets) = buckets {
        opts = opts.buckets(buckets);
    }
    let histogram = HistogramVec::new(opts, labels)?;
    prometheus::register(Box::new(histogram.clone()))?;
    Ok(histogram)
}

fn try_create_int_gauge_vec(name: &str, help: &str, labels: &[&str]) -> Result<IntGaugeVec> {
    let opts = Opts::new(name, help);
    let gauge = IntGaugeVec::new(opts, labels)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}
