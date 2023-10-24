use near_o11y::metrics::IntGaugeVec;
use once_cell::sync::Lazy;

pub static TRANSACTION_POOL_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_gauge_vec(
        "near_transaction_pool_entries",
        "Total number of transactions currently tracked by the node in a given shard pool",
        &["shard_id"],
    )
    .unwrap()
});

pub static TRANSACTION_POOL_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_gauge_vec(
        "near_transaction_pool_size",
        "Total size in bytes of transactions currently tracked by the node in a given shard pool",
        &["shard_id"],
    )
    .unwrap()
});
