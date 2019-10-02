use near_primitives::hash::CryptoHash;
use near_metrics::{Histogram, IntCounter, IntGauge, try_create_int_counter, try_create_int_gauge, try_create_histogram};

lazy_static! {
    pub static ref BLOCKS_PROCESSED: near_metrics::Result<IntCounter> = try_create_int_counter(
        "block_num_processed",
        "Total number of blocks processed"
    );

    pub static ref BLOCKS_PROCESSED_SUCCESSFULLY: near_metrics::Result<IntCounter> = try_create_int_counter(
        "block_num_processed_successfully",
        "Total number of blocks processed successfully"
    );

    pub static ref BLOCK_PROCESSING_TIME: near_metrics::Result<Histogram> = try_create_histogram(
        "block_processing_time",
        "Time taken to process a block"
    );

    pub static ref HEAD_BLOCK_HEIGHT: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "head_block_height",
        "Height of the current head of the blockchain"
    );

    pub static ref HEAD_BLOCK_HASH: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "head_block_hash",
        "Block hash of the current head of the blockchain"
    );
}

// Helper function for converting hash to i64
pub fn hash_to_i64(hash: &CryptoHash) -> i64 {
    // Note: The size of a hash is 256 bits but a gauge is 64 bits.
    let hash_as_vec: Vec<u8> = hash.into();
    let mut number: i64 = 0;
    for (i, byte) in hash_as_vec.iter().enumerate() {
        number += (*byte as i64).wrapping_shl(i as u32 * 8);
    }
    number
}
