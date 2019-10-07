use near_metrics::{try_create_int_counter, IntCounter};

lazy_static! {
    pub static ref BLOCK_PRODUCED_TOTAL: near_metrics::Result<IntCounter> = try_create_int_counter(
        "block_produced_total",
        "Total number of blocks produced since starting this node"
    );
}
