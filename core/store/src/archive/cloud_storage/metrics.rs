//! Prometheus metrics for the cloud archive.
use near_o11y::metrics::{
    HistogramVec, IntGaugeVec, exponential_buckets, try_create_histogram_vec,
    try_create_int_gauge_vec,
};
use std::sync::LazyLock;

pub static CLOUD_ARCHIVAL_UPLOAD_SIZE_BYTES: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_cloud_archival_upload_size_bytes",
        "Size in bytes of objects uploaded to the cloud archive, by object type",
        &["object_type"],
        Some(exponential_buckets(64.0, 4.0, 12).unwrap()),
    )
    .unwrap()
});

pub static CLOUD_ARCHIVAL_UPLOAD_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_cloud_archival_upload_duration_seconds",
        "Latency of object uploads to the cloud archive, by object type",
        &["object_type"],
        // Cloud uploads run tens of ms; a 10ms floor keeps the low buckets usable.
        Some(exponential_buckets(0.01, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub static CLOUD_ARCHIVAL_HEAD_HEIGHT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_cloud_archival_head_height",
        "Cloud archive head height per archived component",
        &["component"],
    )
    .unwrap()
});
