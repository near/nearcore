use near_metrics::{try_create_int_counter, try_create_int_gauge, IntCounter, IntGauge};

lazy_static! {
    pub static ref BLOCK_PRODUCED_TOTAL: near_metrics::Result<IntCounter> = try_create_int_counter(
        "block_produced_total",
        "Total number of blocks produced since starting this node"
    );
    pub static ref IS_VALIDATOR: near_metrics::Result<IntGauge> =
        try_create_int_gauge("is_validator", "Bool to denote if it is currently validating");
    pub static ref RECEIVED_BYTES_PER_SECOND: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "received_bytes_per_second",
        "Number of bytes per second received over the network overall"
    );
    pub static ref SENT_BYTES_PER_SECOND: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "sent_bytes_per_second",
        "Number of bytes per second sent over the network overall"
    );
    pub static ref BLOCKS_PER_MINUTE: near_metrics::Result<IntGauge> =
        try_create_int_gauge("blocks_per_minute", "Blocks produced per minute");
    pub static ref CPU_USAGE: near_metrics::Result<IntGauge> =
        try_create_int_gauge("cpu_usage", "Percent of CPU usage");
    pub static ref MEMORY_USAGE: near_metrics::Result<IntGauge> =
        try_create_int_gauge("memory_usage", "Amount of RAM memory usage");
}
