use near_o11y::metrics::{IntGauge, IntGaugeVec, try_create_int_gauge, try_create_int_gauge_vec};
use std::sync::LazyLock;

pub(crate) static PROTOCOL_VERSION_VOTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_protocol_version_votes",
        "The percentage of stake voting for each protocol version",
        &["protocol_version"],
    )
    .unwrap()
});

pub(crate) static PROTOCOL_VERSION_NEXT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_protocol_version_next", "The protocol version for the next epoch.")
        .unwrap()
});
