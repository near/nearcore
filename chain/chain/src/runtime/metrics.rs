use near_o11y::metrics::{
    HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, exponential_buckets, linear_buckets,
    try_create_histogram_vec, try_create_int_counter_vec, try_create_int_gauge,
    try_create_int_gauge_vec,
};
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_split::TrieSplit;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::LazyLock;

pub(crate) static APPLY_CHUNK_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_apply_chunk_delay_seconds",
        "Time to process a chunk. Gas used by the chunk is a metric label, rounded up to 100 teragas.",
        &["tgas_ceiling"],
        Some(linear_buckets(0.0, 0.05, 50).unwrap()),
    )
        .unwrap()
});

pub(crate) static DELAYED_RECEIPTS_COUNT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_delayed_receipts_count",
        "The count of the delayed receipts. Indicator of congestion.",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static PREPARE_TX_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_prepare_tx_size",
        "Sum of transaction sizes per produced chunk, as a histogram",
        &["shard_id"],
        // Maximum is < 14MB, typical values are unknown right now so buckets
        // might need to be adjusted later when we have collected data
        Some(vec![1_000.0, 10_000., 100_000., 500_000., 1e6, 2e6, 4e6, 8e6, 12e6]),
    )
    .unwrap()
});

pub(crate) static PREPARE_TX_GAS: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_prepare_tx_gas",
        "How much gas was spent for processing new transactions when producing a chunk.",
        &["shard_id"],
        // 100e9 = 100 Ggas
        // A transaction with no actions costs 108 Ggas to process.
        // A typical function call costs ~300 Ggas.
        // The absolute maximum is defined by `max_tx_gas` = 500 Tgas.
        // This ranges from 100 Ggas to 409.6 Tgas as the last bucket boundary.
        Some(exponential_buckets(100e9, 2.0, 12).unwrap()),
    )
    .unwrap()
});

pub(crate) static PREPARE_TX_REJECTED: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_prepare_tx_rejected",
        "The number of transactions rejected when producing a chunk.",
        // possible reasons:
        // - invalid_tx             The tx failed validation or the signer has not enough funds.
        // - invalid_block_hash     The block_hash field on the tx is expired or not on the canonical chain.
        // - congestion             The receiver shard is congested.
        &["shard_id", "reason"],
        // Histogram boundaries are inclusive. Pick the first boundary below 1
        // to have 0 values as a separate bucket.
        // In exclusive boundaries, this would be equivalent to:
        // [0, 10, 100, 1_000, 10_000]
        Some(exponential_buckets(0.99999, 10.0, 6).unwrap()),
    )
    .unwrap()
});

pub(crate) static CONGESTION_PREPARE_TX_GAS_LIMIT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_congestion_prepare_tx_gas_limit",
        "How much gas the shard spends at most per chunk to convert new transactions to receipts.",
        &["shard_id"],
    )
    .unwrap()
});

pub static APPLYING_CHUNKS_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_applying_chunks_time",
        "Time taken to apply chunks per shard",
        &["apply_reason", "shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_OBTAIN_PART_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_state_sync_obtain_part_delay_sec",
        "Latency of applying a state part",
        &["shard_id", "result"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_VALIDATE_PART_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_state_sync_validate_part_delay_sec",
        "Latency of validating a state part",
        &["shard_id", "result"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_APPLY_PART_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_state_sync_apply_part_delay_sec",
        "Latency of applying a state part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static DYNAMIC_RESHARDING_SHARD_MEMORY_USAGE: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_dynamic_resharding_shard_memory_usage",
            "Trie-cost memory usage of the shard's state, i.e. the value compared against \
             the dynamic resharding memory usage threshold. This is an artificial value \
             calculated according to TRIE_COSTS, not actual RAM usage.",
            &["shard_uid"],
        )
        .unwrap()
    });

pub(crate) static DYNAMIC_RESHARDING_MEMORY_USAGE_THRESHOLD: LazyLock<IntGauge> =
    LazyLock::new(|| {
        try_create_int_gauge(
            "near_dynamic_resharding_memory_usage_threshold",
            "Trie-cost memory usage over which a shard is proposed for a split, \
             from the dynamic resharding config",
        )
        .unwrap()
    });

pub(crate) static DYNAMIC_RESHARDING_MIN_CHILD_MEMORY_USAGE: LazyLock<IntGauge> =
    LazyLock::new(|| {
        try_create_int_gauge(
            "near_dynamic_resharding_min_child_memory_usage",
            "Minimum trie-cost memory usage of a child shard for a split to be proposed, \
             from the dynamic resharding config",
        )
        .unwrap()
    });

pub(crate) static DYNAMIC_RESHARDING_MAX_NUMBER_OF_SHARDS: LazyLock<IntGauge> =
    LazyLock::new(|| {
        try_create_int_gauge(
            "near_dynamic_resharding_max_number_of_shards",
            "Maximum number of shards in the network, from the dynamic resharding config",
        )
        .unwrap()
    });

pub(crate) static DYNAMIC_RESHARDING_PROPOSED_SPLIT_LEFT_MEMORY: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_dynamic_resharding_proposed_split_left_memory",
            "Trie-cost memory usage of the left child of the currently proposed shard split, \
             or 0 if no split is proposed",
            &["shard_uid"],
        )
        .unwrap()
    });

pub(crate) static DYNAMIC_RESHARDING_PROPOSED_SPLIT_RIGHT_MEMORY: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_dynamic_resharding_proposed_split_right_memory",
            "Trie-cost memory usage of the right child of the currently proposed shard split, \
             or 0 if no split is proposed",
            &["shard_uid"],
        )
        .unwrap()
    });

pub(crate) static DYNAMIC_RESHARDING_PROPOSED_SPLIT_INFO: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_dynamic_resharding_proposed_split_info",
            "Set to 1 while the boundary account in the label is the current proposed \
             split for the shard",
            &["shard_uid", "boundary_account"],
        )
        .unwrap()
    });

pub(crate) static DYNAMIC_RESHARDING_FIND_SPLIT_ERRORS: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        try_create_int_counter_vec(
            "near_dynamic_resharding_find_split_errors_total",
            "Number of failures to compute the trie split for a shard during chunk application.",
            &["shard_uid"],
        )
        .unwrap()
    });

/// Tracks the `boundary_account` label of the currently exported
/// `near_dynamic_resharding_proposed_split_info` series per shard, so that a stale
/// series can be removed when the proposed boundary changes or the proposal disappears.
static PROPOSED_SPLIT_BOUNDARIES: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(Default::default);

/// Latch the proposed-split gauges for a shard to the most recently proposed
/// `TrieSplit`. A proposal is present only in the single boundary-block chunk that
/// proposes the split, so a `None` (every other block) is ignored rather than
/// resetting the gauges to zero -- otherwise they would read 0 almost always and be
/// useless on a dashboard. The last proposed split therefore persists per shard,
/// mirroring `near_dynamic_resharding_scheduled_epoch_height`.
pub(crate) fn set_proposed_split_metrics(shard_uid: ShardUId, split: Option<&TrieSplit>) {
    let Some(split) = split else {
        return;
    };
    let shard_uid_label = shard_uid.to_string();
    DYNAMIC_RESHARDING_PROPOSED_SPLIT_LEFT_MEMORY
        .with_label_values(&[&shard_uid_label])
        .set(split.left_memory as i64);
    DYNAMIC_RESHARDING_PROPOSED_SPLIT_RIGHT_MEMORY
        .with_label_values(&[&shard_uid_label])
        .set(split.right_memory as i64);

    let new_boundary = split.boundary_account.to_string();
    let mut boundaries = PROPOSED_SPLIT_BOUNDARIES.lock();
    if boundaries.get(&shard_uid_label) == Some(&new_boundary) {
        return;
    }
    // Boundary changed for this shard: drop the stale info series before adding the new one.
    if let Some(old_boundary) = boundaries.get(&shard_uid_label) {
        let _ = DYNAMIC_RESHARDING_PROPOSED_SPLIT_INFO
            .remove_label_values(&[&shard_uid_label, old_boundary]);
    }
    DYNAMIC_RESHARDING_PROPOSED_SPLIT_INFO
        .with_label_values(&[&shard_uid_label, &new_boundary])
        .set(1);
    boundaries.insert(shard_uid_label, new_boundary);
}
