use near_o11y::metrics::{try_create_int_gauge_vec, try_create_int_gauge, IntGauge, IntGaugeVec};
use once_cell::sync::Lazy;

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_check_num_parts_valid",
        "Number of valid state parts dumped for the epoch",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_check_num_parts_invalid",
        "Number of invalid state parts dumped for the epoch",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_PARTS_DUMPED: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_check_num_parts_dumped",
        "Number of total parts required for the epoch",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_PARTS_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_check_num_parts_total",
        "Number of total parts required for the epoch",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_CHECK_EPOCH_HEIGHT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_check_epoch_height",
        "epoch height of the current epoch being checked",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_check_has_skipped_epoch",
        "whether there has been a skip of epoch heights in this check compared with last one",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_CHECK_PROCESS_IS_UP: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_state_sync_dump_check_process_is_up", "whether the state sync dump check process is up and running")
        .unwrap()
});
