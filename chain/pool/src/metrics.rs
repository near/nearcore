use near_o11y::metrics::IntGauge;
use once_cell::sync::Lazy;

pub static TRANSACTION_POOL_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_gauge(
        "near_transaction_pool_entries",
        "Total number of transactions currently in the pools tracked by the node",
    )
    .unwrap()
});
