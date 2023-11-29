use near_o11y::metrics::{try_create_int_counter_vec, IntCounterVec};
use once_cell::sync::Lazy;

pub static SYNC_STATUS_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_sync_status_total",
        "Number of times a state sync was triggered",
        &["sync"],
    )
    .unwrap()
});
