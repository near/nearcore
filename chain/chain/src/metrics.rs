use near_metrics::{
    try_create_histogram, try_create_int_counter, try_create_int_gauge, Histogram, IntCounter,
    IntGauge,
};

lazy_static! {
    pub static ref BLOCK_PROCESSED_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter("near_block_processed_total", "Total number of blocks processed");
    pub static ref BLOCK_PROCESSED_SUCCESSFULLY_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "near_block_processed_successfully_total",
            "Total number of blocks processed successfully"
        );
    pub static ref BLOCK_PROCESSING_TIME: near_metrics::Result<Histogram> =
        try_create_histogram("near_block_processing_time", "Time taken to process blocks");
    pub static ref BLOCK_HEIGHT_HEAD: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "near_block_height_head",
        "Height of the current head of the blockchain"
    );
    pub static ref VALIDATOR_AMOUNT_STAKED: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "near_validators_stake_total",
        "The total stake of all active validators during the last block"
    );
    pub static ref VALIDATOR_ACTIVE_TOTAL: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "near_validator_active_total",
        "The total number of validators active after last block"
    );
}
