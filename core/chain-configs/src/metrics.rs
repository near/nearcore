use near_o11y::metrics::{try_create_int_gauge_vec, IntGaugeVec};
use std::sync::LazyLock;

pub static CONFIG_MUTABLE_FIELD: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_config_mutable_field",
        "Timestamp and value of a mutable config field",
        &["field_name", "timestamp", "value"],
    )
    .unwrap()
});
