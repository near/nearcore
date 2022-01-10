use near_metrics::{
    try_create_histogram, try_create_int_counter, try_create_int_gauge, Histogram, IntCounter,
    IntGauge, IntGaugeVec,
};
use near_store::NUM_COLS;
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

// fn create_rocksdb_metric(name: str, help: &str) -> [Lazy<IntGauge>; 1] {
//     [Lazy::new(|| {
//         try_create_int_gauge(&format!("{}_col{}", name, col), &format!("{}_col{}", help, col))
//             .unwrap()
//     })]
//     (0..NUM_COLS)
//         .map(|col: usize| {
//             Lazy::new(|| {
//                 try_create_int_gauge(
//                     &format!("{}_col{}", name, col),
//                     &format!("{}_col{}", help, col),
//                 )
//                 .unwrap()
//             })
//         })
//         .collect()
// }

pub static ROCKSDB_LABELS: [&str; NUM_COLS] = {
    let mut labels = [""; NUM_COLS];
    let mut i = 0;
    while i < NUM_COLS {
        labels[i] = &format!("col{}", i);
        i += 1;
    }
    labels
};

pub static ROCKSDB_COL_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    near_metrics::try_create_int_gauge_vec(
        "near_rocksdb_size",
        "Size in bytes of RocksDB column",
        &ROCKSDB_LABELS,
    )
    .unwrap()
});

// pub static ROCKSDB_COL_SIZE: &[Lazy<IntGauge>] =
//     &create_rocksdb_metric("near_rocksdb_size", "Size in bytes of RocksDB column");
//
// pub static ROCKSDB_ENTRIES: &[Lazy<IntGauge>] =
//     &create_rocksdb_metric("near_rocksdb_entries", "Number of entries in RocksDB column");
//
// pub static ROCKSDB_KEY_SIZE: &[Lazy<IntGauge>] =
//     &create_rocksdb_metric("near_rocksdb_key_size", "Total size of keys in RocksDB column");
//
// pub static ROCKSDB_VALUE_SIZE: &[Lazy<IntGauge>] =
//     &create_rocksdb_metric("near_rocksdb_value_size", "Total size of values in RocksDB column");

pub static ROCKSDB_STATS_PROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_rocksdb_stats_processing_time",
        "Time taken to generate RocksDB stats",
    )
    .unwrap()
});
