use near_o11y::metrics::{IntGaugeVec, try_create_int_gauge_vec};
use std::sync::LazyLock;

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_HEADERS_VALID: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_num_headers_valid",
            "Number of valid state headers dumped for the epoch",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_HEADERS_INVALID: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_num_header_invalid",
            "Number of invalid state header dumped for the epoch",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_HEADERS_DUMPED: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_num_header_dumped",
            "Number of dumped headers for the epoch",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_num_parts_valid",
            "Number of valid state parts dumped for the epoch",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_num_parts_invalid",
            "Number of invalid state parts dumped for the epoch",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_PARTS_DUMPED: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_num_parts_dumped",
            "Number of dumped parts required for the epoch",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_CHECK_NUM_PARTS_TOTAL: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_num_parts_total",
            "Number of total parts required for the epoch",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_CHECK_EPOCH_HEIGHT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_check_epoch_height",
        "epoch height of the current epoch being checked",
        &["shard_id", "chain_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_has_skipped_epoch",
            "whether there has been a skip of epoch heights in this check compared with last one",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_CHECK_PROCESS_IS_UP: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_state_sync_dump_check_process_is_up",
            "whether the state sync dump check process is up and running",
            &["shard_id", "chain_id"],
        )
        .unwrap()
    });
