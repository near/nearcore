use near_o11y::metrics::{try_create_int_gauge, try_create_int_gauge_vec, IntGauge, IntGaugeVec};
use once_cell::sync::Lazy;

pub(crate) static PROTOCOL_VERSION_VOTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_protocol_version_votes",
        "The percentage of stake voting for each protocol version",
        &["protocol_version"],
    )
    .unwrap()
});

pub(crate) static PROTOCOL_VERSION_NEXT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_protocol_version_next", "The protocol version for the next epoch.")
        .unwrap()
});
