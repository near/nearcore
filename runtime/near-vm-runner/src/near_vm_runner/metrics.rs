use near_o11y::metrics::{try_create_int_counter_vec, IntCounterVec};
use once_cell::sync::Lazy;

pub static COMPILED_CONTRACT_CACHE_HIT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_compiled_contract_cache_hit",
        "The number of times the runtime finds compiled code in cache",
        &["shard_id"],
    )
    .unwrap()
});
pub static COMPILED_CONTRACT_CACHE_MISS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_compiled_contract_cache_miss",
        "The number of times the runtime cannot find compiled code in cache",
        &["shard_id"],
    )
    .unwrap()
});
