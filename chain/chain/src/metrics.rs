use near_metrics::{
    try_create_histogram, try_create_int_counter, try_create_int_gauge, Histogram, IntCounter,
    IntGauge,
};

lazy_static! {
    pub static ref BLOCK_PROCESSED_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter("block_processed_total", "Total number of blocks processed");
    pub static ref BLOCK_PROCESSED_SUCCESSFULLY_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "block_processed_successfully_total",
            "Total number of blocks processed successfully"
        );
    pub static ref BLOCK_PROCESSING_TIME: near_metrics::Result<Histogram> =
        try_create_histogram("block_processing_time", "Time taken to process blocks");
    pub static ref BLOCK_INDEX_HEAD: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "block_index_head",
        "Block index of the current head of the blockchain"
    );
    pub static ref VALIDATOR_AMOUNT_STAKED: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "validators_amount_staked",
        "The total stake of all active validators during the last block"
    );
    pub static ref VALIDATOR_ACTIVE_TOTAL: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "validator_active_total",
        "The total number of validators active after last block"
    );
}
