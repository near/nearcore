use near_o11y::metrics::{
    IntCounter, IntCounterVec, try_create_int_counter, try_create_int_counter_vec,
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
