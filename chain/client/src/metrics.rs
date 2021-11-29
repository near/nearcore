use near_metrics::{
    try_create_histogram, try_create_int_counter, try_create_int_gauge, Histogram, IntCounter,
    IntGauge,
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
