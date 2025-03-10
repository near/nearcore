use near_o11y::metrics::{IntCounter, IntGauge, try_create_int_counter, try_create_int_gauge};
use std::sync::LazyLock;

pub static CONFIG_RELOADS: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_config_reloads_total",
        "Number of times the configs were reloaded during the current run of the process",
    )
    .unwrap()
});

pub static CONFIG_RELOAD_TIMESTAMP: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_config_reload_timestamp_seconds",
        "Timestamp of the last reload of the config",
    )
    .unwrap()
});
