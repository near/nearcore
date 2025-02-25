use near_o11y::metrics::{IntGaugeVec, try_create_int_gauge_vec};
use std::sync::LazyLock;

pub static MEMTRIE_ARENA_ACTIVE_ALLOCS_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_memtrie_arena_active_allocs_bytes",
        "Total size of active allocations on the in-memory trie arena",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEMTRIE_ARENA_ACTIVE_ALLOCS_COUNT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_memtrie_arena_active_allocs_count",
        "Total number of active allocations on the in-memory trie arena",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEMTRIE_ARENA_MEMORY_USAGE_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_memtrie_arena_memory_usage_bytes",
        "Memory usage of the in-memory trie arena",
        &["shard_uid"],
    )
    .unwrap()
});
