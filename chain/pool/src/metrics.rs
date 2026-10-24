use near_o11y::metrics::IntGaugeVec;
use std::sync::LazyLock;

pub static TRANSACTION_POOL_COUNT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_gauge_vec(
        "near_transaction_pool_entries",
        "Total number of signed transactions currently tracked by the node in a given shard pool",
        &["shard_id"],
    )
    .unwrap()
});

pub static TRANSACTION_POOL_SIZE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_gauge_vec(
        "near_transaction_pool_size",
        "Total size in bytes of signed transactions currently tracked by the node in a given shard pool",
        &["shard_id"],
    )
    .unwrap()
});

pub static ECC_POOL_COUNT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_gauge_vec(
        "near_ecc_pool_entries",
        "Total number of ECCs currently tracked by the node in a given shard pool",
        &["shard_id"],
    )
    .unwrap()
});

pub static ECC_POOL_SIZE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    near_o11y::metrics::try_create_int_gauge_vec(
        "near_ecc_pool_size",
        "Total size in bytes of ECCs currently tracked by the node in a given shard pool",
        &["shard_id"],
    )
    .unwrap()
});
