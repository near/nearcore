use near_o11y::metrics::{
    try_create_int_counter, try_create_int_counter_vec, try_create_int_gauge_vec, IntCounter,
    IntCounterVec, IntGaugeVec,
};
use std::sync::LazyLock;

pub static MEM_TRIE_NUM_ROOTS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_mem_trie_num_roots",
        "Number of trie roots currently active in the in-memory trie",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEM_TRIE_NUM_NODES_CREATED_FROM_UPDATES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_mem_trie_num_nodes_created_from_updates",
        "Number of trie nodes created by applying updates",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEM_TRIE_NUM_LOOKUPS: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_mem_trie_num_lookups",
        "Number of in-memory trie lookups (number of keys looked up)",
    )
    .unwrap()
});
