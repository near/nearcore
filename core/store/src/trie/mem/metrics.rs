use near_o11y::metrics::{
    HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, exponential_buckets,
    try_create_histogram_vec, try_create_int_counter, try_create_int_counter_vec,
    try_create_int_gauge_vec,
};
use near_primitives::shard_layout::ShardUId;
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

/// Values of the `near_memtrie_background_load_status` gauge.
#[derive(Clone, Copy)]
#[repr(i64)]
pub enum MemtrieBackgroundLoadStatus {
    /// No background load activity for the shard.
    None = 0,
    /// Background thread is loading the memtrie from flat state.
    Loading = 1,
    /// Load finished, result awaits finalization on the next block postprocessing.
    AwaitingFinalization = 2,
    /// Delta catch-up is being applied to the loaded memtrie.
    CatchingUp = 3,
    /// The memtrie has been finalized and inserted into the active map.
    Done = 4,
    /// Background preload was a no-op because the memtrie was already loaded.
    AlreadyLoaded = 5,
}

pub static MEMTRIE_BACKGROUND_LOAD_STATUS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_memtrie_background_load_status",
        "Status of background memtrie loading: 0 - none, 1 - loading, \
         2 - awaiting finalization, 3 - applying delta catch-up, 4 - done, \
         5 - already loaded (preload no-op)",
        &["shard_uid"],
    )
    .unwrap()
});

pub fn set_background_load_status(shard_uid: &ShardUId, status: MemtrieBackgroundLoadStatus) {
    MEMTRIE_BACKGROUND_LOAD_STATUS.with_label_values(&[&shard_uid.to_string()]).set(status as i64);
}

pub fn reset_background_load_status(shard_uid: &ShardUId) {
    set_background_load_status(shard_uid, MemtrieBackgroundLoadStatus::None);
}

pub static MEMTRIE_BACKGROUND_LOAD_RETRIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_memtrie_background_load_retries_total",
        "Number of failed background memtrie load attempts; the node panics after exceeding \
         the maximum number of retries",
        &["shard_uid"],
    )
    .unwrap()
});

pub static MEMTRIE_BACKGROUND_LOAD_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_memtrie_background_load_duration_seconds",
        "Duration of background memtrie loading stages: \
         'load' - loading from flat state, 'catchup' - applying delta catch-up",
        &["shard_uid", "stage"],
        Some(exponential_buckets(0.5, 2.0, 12).unwrap()),
    )
    .unwrap()
});

pub static MEMTRIE_BACKGROUND_LOAD_DELTAS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_memtrie_background_load_deltas",
        "Number of flat state deltas that accumulated and were applied while loading \
         the memtrie during the last background load",
        &["shard_uid"],
    )
    .unwrap()
});
