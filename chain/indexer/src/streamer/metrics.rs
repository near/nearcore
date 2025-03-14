use near_o11y::metrics::{
    Histogram, IntCounter, IntGauge, try_create_histogram, try_create_int_counter,
    try_create_int_gauge,
};
use std::sync::LazyLock;

pub(crate) static START_BLOCK_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_indexer_streaming_start_block_height",
        "Block height from which the indexing iteration started",
    )
    .unwrap()
});

pub(crate) static LATEST_BLOCK_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_indexer_streaming_latest_block_height",
        "Block height to which the indexing iteration runs",
    )
    .unwrap()
});

pub(crate) static CURRENT_BLOCK_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_indexer_streaming_current_block_height",
        "Current height of the block being indexed",
    )
    .unwrap()
});

pub(crate) static NUM_STREAMER_MESSAGES_SENT: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_indexer_num_streamer_messages_sent",
        "Number of Streamer messages sent to",
    )
    .unwrap()
});

pub(crate) static BUILD_STREAMER_MESSAGE_TIME: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram(
        "near_indexer_build_streamer_message_time",
        "Time taken to build a streamer message",
    )
    .unwrap()
});

pub(crate) static LOCAL_RECEIPT_LOOKUP_IN_HISTORY_BLOCKS_BACK: LazyLock<IntGauge> =
    LazyLock::new(|| {
        try_create_int_gauge(
            "near_indexer_local_receipt_lookup_in_history_blocks_back",
            "Time taken to lookup a receipt in history blocks back",
        )
        .unwrap()
    });
