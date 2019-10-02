use near_metrics::{IntCounter, try_create_int_counter};

lazy_static! {
    pub static ref BLOCK_PRODUCED: near_metrics::Result<IntCounter> = try_create_int_counter(
        "block_num_produced",
        "Total number of blocks produced"
    );
}
