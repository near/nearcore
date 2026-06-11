use near_o11y::metrics::{
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, try_create_int_counter,
    try_create_int_counter_vec, try_create_int_gauge, try_create_int_gauge_vec,
};
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

pub(crate) static DYNAMIC_RESHARDING_SCHEDULED_EPOCH_HEIGHT: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_dynamic_resharding_scheduled_epoch_height",
            "The epoch height at which the scheduled dynamic resharding takes effect; \
             labels carry the shard being split and the boundary account",
            &["parent_shard_id", "boundary_account"],
        )
        .unwrap()
    });

pub(crate) static DYNAMIC_RESHARDING_SPLIT_MISSING_SHARD: LazyLock<IntCounter> =
    LazyLock::new(|| {
        try_create_int_counter(
            "near_dynamic_resharding_split_missing_shard_total",
            "Number of scheduled shard splits that were skipped because the shard no longer \
             exists in the next epoch's layout. This should never happen given the resharding \
             cooldown invariant.",
        )
        .unwrap()
    });

pub(crate) static RESHARDING_ASSIGNMENT_STRATEGY: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_resharding_assignment_strategy_total",
        "Number of epoch finalizations by the chunk producer assignment strategy chosen: \
         'carry_over' - layout unchanged, 'sticky_resharding' - layout changed with sticky \
         assignment, 'fresh' - assignment from scratch",
        &["strategy"],
    )
    .unwrap()
});
