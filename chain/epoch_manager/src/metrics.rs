use near_metrics::{try_create_int_counter, IntCounter};

lazy_static::lazy_static! {
    pub static ref BLOCK_PRODUCED_TOTAL: near_metrics::Result<IntCounter> = try_create_int_counter(
        "near_block_produced_total",
        "Total number of blocks produced since starting this node"
    );
    pub static ref BLOCK_EXPECTED_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter("near_block_expected_total", "Expected number of blocks produced by this node since startup");
}
