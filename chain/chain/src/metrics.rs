use near_metrics::{
    try_create_histogram, try_create_int_counter, try_create_int_gauge, Histogram, IntCounter,
    IntGauge,
};
use near_store::NUM_COLS;
use once_cell::sync::Lazy;

pub static BLOCK_PROCESSED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_block_processed_total", "Total number of blocks processed")
        .unwrap()
});
pub static BLOCK_PROCESSED_SUCCESSFULLY_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_block_processed_successfully_total",
        "Total number of blocks processed successfully",
    )
    .unwrap()
});
pub static BLOCK_PROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_processing_time", "Time taken to process blocks").unwrap()
});
pub static BLOCK_HEIGHT_HEAD: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_block_height_head", "Height of the current head of the blockchain")
        .unwrap()
});
pub static VALIDATOR_AMOUNT_STAKED: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_validators_stake_total",
        "The total stake of all active validators during the last block",
    )
    .unwrap()
});
pub static VALIDATOR_ACTIVE_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_validator_active_total",
        "The total number of validators active after last block",
    )
    .unwrap()
});
pub static NUM_ORPHANS: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_num_orphans", "Number of orphan blocks.").unwrap());

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

pub static ROCKSDB_COL_SIZE: &[Lazy<IntGauge>] = (0..NUM_COLS)
    .map(|col: usize| {
        Lazy::new(|| {
            try_create_int_gauge(
                &format!("near_rocksdb_size_col{}", col),
                &format!("near_rocksdb_size_col{}", col),
            )
            .unwrap()
        })
    })
    .collect();

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
