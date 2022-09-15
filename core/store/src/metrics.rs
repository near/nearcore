use near_metrics::{
    try_create_histogram_vec, try_create_int_counter_vec, try_create_int_gauge_vec, HistogramVec,
    IntCounterVec, IntGaugeVec,
};
use once_cell::sync::Lazy;

pub(crate) static DATABASE_OP_LATENCY_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_database_op_latency_by_op_and_column",
        "Database operations latency by operation and column.",
        &["op", "column"],
        Some(vec![0.00002, 0.0001, 0.0002, 0.0005, 0.0008, 0.001, 0.002, 0.004, 0.008, 0.1]),
    )
    .unwrap()
});

pub static CHUNK_CACHE_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_chunk_cache_hits",
        "Chunk cache hits",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static CHUNK_CACHE_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_chunk_cache_misses",
        "Chunk cache misses",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_hits",
        "Shard cache hits",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_misses",
        "Shard cache misses",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_TOO_LARGE: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_too_large",
        "Number of values to be inserted into shard cache is too large",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_shard_cache_size", "Shard cache size", &["shard_id", "is_view"])
        .unwrap()
});

pub static CHUNK_CACHE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_chunk_cache_size", "Chunk cache size", &["shard_id", "is_view"])
        .unwrap()
});

pub static SHARD_CACHE_CURRENT_TOTAL_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_shard_cache_current_total_size",
        "Shard cache current total size",
        &["shard_id", "is_view"],
    )
    .unwrap()
});

pub static SHARD_CACHE_POP_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_pop_hits",
        "Shard cache pop hits",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static SHARD_CACHE_POP_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_pop_misses",
        "Shard cache pop misses",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static SHARD_CACHE_POP_LRU: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_pop_lru",
        "Shard cache LRU pops",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static SHARD_CACHE_GC_POP_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_shard_cache_gc_pop_misses",
        "Shard cache gc pop misses",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static SHARD_CACHE_DELETIONS_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_shard_cache_deletions_size",
        "Shard cache deletions size",
        &["shard_id", "is_view"],
    )
    .unwrap()
});
pub static APPLIED_TRIE_DELETIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_applied_trie_deletions",
        "Trie deletions applied to store",
        &["shard_id"],
    )
    .unwrap()
});
pub static APPLIED_TRIE_INSERTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_applied_trie_insertions",
        "Trie insertions applied to store",
        &["shard_id"],
    )
    .unwrap()
});
pub static REVERTED_TRIE_INSERTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_reverted_trie_insertions",
        "Trie insertions reverted due to GC of forks",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_SENT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_prefetch_sent", "Prefetch requests sent to DB", &["shard_id"])
        .unwrap()
});
pub static PREFETCH_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_prefetch_hits", "Prefetched trie keys", &["shard_id"]).unwrap()
});
pub static PREFETCH_PENDING: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_pending",
        "Prefetched trie keys that were still pending when main thread needed data",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_FAIL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_fail",
        "Prefetching trie key failed with an error",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_NOT_REQUESTED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_not_requested",
        "Number of values that had to be fetched without having been prefetched",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_MEMORY_LIMIT_REACHED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_memory_limit_reached",
        "Number of values that could not be prefetched due to prefetch staging area size limitations",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_RETRY: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_retries",
        "Main thread was waiting for prefetched value but had to retry fetch afterwards.",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_STAGED_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_prefetch_staged_bytes",
        "Upper bound on memory usage for holding prefetched data.",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_STAGED_SLOTS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_prefetch_staged_slots",
        "Number of slots used in staging area.",
        &["shard_id"],
    )
    .unwrap()
});
