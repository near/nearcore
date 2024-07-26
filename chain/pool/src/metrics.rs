use near_o11y::metrics::IntGaugeVec;
use std::sync::LazyLock;

pub static TRANSACTION_POOL_COUNT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_gauge_vec(
        "near_transaction_pool_entries",
        "Total number of transactions currently tracked by the node in a given shard pool",
        &["shard_id"],
    )
    .unwrap()
});

pub static TRANSACTION_POOL_SIZE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_gauge_vec(
        "near_transaction_pool_size",
        "Total size in bytes of transactions currently tracked by the node in a given shard pool",
        &["shard_id"],
    )
    .unwrap()
});
