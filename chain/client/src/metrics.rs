use near_metrics::{
    try_create_histogram, try_create_histogram_vec, try_create_int_counter,
    try_create_int_counter_vec, try_create_int_gauge, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec,
};
use once_cell::sync::Lazy;

pub static BLOCK_PRODUCED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_block_produced_total",
        "Total number of blocks produced since starting this node",
    )
    .unwrap()
});
pub static CHUNK_PRODUCED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_chunk_produced_total",
        "Total number of chunks produced since starting this node",
    )
    .unwrap()
});
pub static IS_VALIDATOR: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_is_validator", "Bool to denote if it is currently validating")
        .unwrap()
});
pub static RECEIVED_BYTES_PER_SECOND: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_received_bytes_per_second",
        "Number of bytes per second received over the network overall",
    )
    .unwrap()
});
pub static SENT_BYTES_PER_SECOND: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_sent_bytes_per_second",
        "Number of bytes per second sent over the network overall",
    )
    .unwrap()
});
pub static BLOCKS_PER_MINUTE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_blocks_per_minute", "Blocks produced per minute").unwrap()
});
pub static CHUNKS_PER_BLOCK_MILLIS: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_chunks_per_block_millis",
        "Average number of chunks included in blocks",
    )
    .unwrap()
});
pub static CPU_USAGE: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_cpu_usage_ratio", "Percent of CPU usage").unwrap());
pub static MEMORY_USAGE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_memory_usage_bytes", "Amount of RAM memory usage").unwrap()
});
pub static GC_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_gc_time", "Time taken to do garbage collection").unwrap()
});
pub static AVG_TGAS_USAGE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_chunk_tgas_used",
        "Number of Tgas (10^12 of gas) used by the last processed chunk",
    )
    .unwrap()
});
pub static TGAS_USAGE_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_tgas_used_hist",
        "Number of Tgas (10^12 of gas) used by processed chunks, as a histogram",
        &["shard"],
        Some(vec![
            50., 100., 300., 500., 700., 800., 900., 950., 1000., 1050., 1100., 1150., 1200.,
            1250., 1300.,
        ]),
    )
    .unwrap()
});
pub static CHUNKS_RECEIVING_DELAY_US: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_chunks_receiving_delay_us",
        "Max delay between receiving a block and its chunks for several most recent blocks",
    )
    .unwrap()
});
pub static BLOCKS_AHEAD_OF_HEAD: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_blocks_ahead_of_head",
        "Height difference between the current head and the newest block or chunk received",
    )
    .unwrap()
});
pub static VALIDATORS_CHUNKS_PRODUCED: Lazy<IntGaugeVec> = Lazy::new(|| {
    near_metrics::try_create_int_gauge_vec(
        "near_validators_chunks_produced",
        "Number of chunks produced by a validator",
        &["account_id"],
    )
    .unwrap()
});
pub static VALIDATORS_CHUNKS_EXPECTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    near_metrics::try_create_int_gauge_vec(
        "near_validators_chunks_expected",
        "Number of chunks expected to be produced by a validator",
        &["account_id"],
    )
    .unwrap()
});
pub static VALIDATORS_BLOCKS_PRODUCED: Lazy<IntGaugeVec> = Lazy::new(|| {
    near_metrics::try_create_int_gauge_vec(
        "near_validators_blocks_produced",
        "Number of blocks produced by a validator",
        &["account_id"],
    )
    .unwrap()
});
pub static VALIDATORS_BLOCKS_EXPECTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    near_metrics::try_create_int_gauge_vec(
        "near_validators_blocks_expected",
        "Number of blocks expected to be produced by a validator",
        &["account_id"],
    )
    .unwrap()
});
pub static SYNC_STATUS: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_sync_status", "Node sync status").unwrap());
pub static EPOCH_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_epoch_height", "Height of the epoch at the head of the blockchain")
        .unwrap()
});
pub static PROTOCOL_UPGRADE_BLOCK_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_protocol_upgrade_block_height",
        "Estimated block height of the protocol upgrade",
    )
    .unwrap()
});
pub static NODE_PROTOCOL_VERSION: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_node_protocol_version", "Max protocol version supported by the node")
        .unwrap()
});
pub static NODE_DB_VERSION: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_node_db_version", "DB version used by the node").unwrap()
});
pub static CHUNK_SKIPPED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_chunk_skipped_total",
        "Number of skipped chunks",
        &["shard_id"],
    )
    .unwrap()
});
