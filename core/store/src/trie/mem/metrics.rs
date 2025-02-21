use near_o11y::metrics::{
    IntCounter, IntCounterVec, IntGaugeVec, try_create_int_counter, try_create_int_counter_vec,
    try_create_int_gauge_vec,
};
use std::sync::LazyLock;

pub static MEMTRIE_NUM_ROOTS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_memtrie_num_roots",
        "Number of trie roots currently active in the in-memory trie",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEMTRIE_NUM_NODES_CREATED_FROM_UPDATES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_memtrie_num_nodes_created_from_updates",
        "Number of trie nodes created by applying updates",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEMTRIE_NUM_LOOKUPS: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_memtrie_num_lookups",
        "Number of in-memory trie lookups (number of keys looked up)",
    )
    .unwrap()
});
