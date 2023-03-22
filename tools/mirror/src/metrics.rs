use near_o11y::metrics::{
    try_create_int_counter, try_create_int_counter_vec, IntCounter, IntCounterVec,
};
use once_cell::sync::Lazy;

pub static TRANSACTIONS_SENT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_mirror_transactions_sent",
        "Total number of transactions sent",
        &["status"],
    )
    .unwrap()
});

pub static TRANSACTIONS_INCLUDED: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_mirror_transactions_included",
        "Total number of transactions sent that made it on-chain",
    )
    .unwrap()
});
