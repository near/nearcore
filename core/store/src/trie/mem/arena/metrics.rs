use near_o11y::metrics::{try_create_int_gauge_vec, IntGaugeVec};
use once_cell::sync::Lazy;

pub static MEM_TRIE_ARENA_ACTIVE_ALLOCS_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_mem_trie_arena_active_allocs_bytes",
        "Total size of active allocations on the in-memory trie arena",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEM_TRIE_ARENA_ACTIVE_ALLOCS_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_mem_trie_arena_active_allocs_count",
        "Total number of active allocations on the in-memory trie arena",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEM_TRIE_ARENA_MEMORY_USAGE_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_mem_trie_arena_memory_usage_bytes",
        "Memory usage of the in-memory trie arena",
        &["shard_uid"],
    )
    .unwrap()
});
