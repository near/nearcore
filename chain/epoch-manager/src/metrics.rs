use near_o11y::metrics::{
    IntCounterVec, IntGauge, IntGaugeVec, try_create_int_counter_vec, try_create_int_gauge,
    try_create_int_gauge_vec,
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
            &["shard_uid", "boundary_account"],
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

// Only the early-kickout seeding path touches this counter, and that path is
// nightly-only; gate the static so non-nightly builds don't carry a dead symbol.
#[cfg(feature = "nightly")]
pub(crate) static EARLY_KICKOUT_SLOT_REASSIGNED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_early_kickout_slot_reassigned_total",
        "Number of seeded chunk-producer rows where the early-kickout blacklist moved the \
         slot to a different producer than plain sampling would have chosen",
        &["shard_id"],
    )
    .unwrap()
});
