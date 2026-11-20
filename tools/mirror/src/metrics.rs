use near_o11y::metrics::{
    IntCounter, IntCounterVec, IntGauge, try_create_int_counter, try_create_int_counter_vec,
    try_create_int_gauge,
};
use std::sync::LazyLock;

pub static TRANSACTIONS_SENT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_mirror_transactions_sent",
        "Total number of transactions sent",
        &["status"],
    )
    .unwrap()
});

pub static TRANSACTIONS_INCLUDED: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_mirror_transactions_included",
        "Total number of transactions sent that made it on-chain",
    )
    .unwrap()
});

pub static TRANSACTIONS_REFORWARDED: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_mirror_transactions_reforwarded",
        "Total number of sent transactions re-forwarded because they were not observed on chain in time",
    )
    .unwrap()
});

/// `--source-cloud-archive`: the height the reader has copied the source store through.
pub static SOURCE_READER_HEAD: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_mirror_source_reader_head",
        "Height the cloud archive reader has copied the source store through",
    )
    .unwrap()
});

/// `--source-cloud-archive`: the bucket's block head, as the mirror last read it.
pub static SOURCE_BUCKET_HEAD: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_mirror_source_bucket_head",
        "Block head of the source cloud archive bucket, as the mirror last read it",
    )
    .unwrap()
});
