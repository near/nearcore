use near_o11y::metrics::IntGauge;
use once_cell::sync::Lazy;

pub static TRANSACTION_POOL_COUNT: Lazy<IntGauge> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_gauge(
        "near_transaction_pool_entries",
        "Total number of transactions currently in the pools tracked by the node",
    )
    .unwrap()
});

pub static TRANSACTION_POOL_SIZE: Lazy<IntGauge> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_gauge(
        "near_transaction_pool_size",
        "Total size in bytes of transactions currently in the pools tracked by the node",
    )
    .unwrap()
});
