use near_metrics::{IntGauge, Histogram, IntCounter, try_create_histogram, try_create_int_counter, try_create_int_gauge};

lazy_static! {
    pub static ref PEER_CONNECTIONS: near_metrics::Result<IntGauge> = try_create_int_gauge(
        "peer_num_connections",
        "Current number of connected peers"
    );

    pub static ref PEER_CONNECTION_DATA: near_metrics::Result<IntCounter> = try_create_int_counter(
        "peer_data_received",
        "Total data received by peers"
    );

    pub static ref PEER_MESSAGE_RECEIVED: near_metrics::Result<IntCounter> = try_create_int_counter(
        "peer_message_received",
        "Total number of messages received from peers"
    );

    pub static ref PEER_BLOCK_RECEIVED: near_metrics::Result<IntCounter> = try_create_int_counter(
        "peer_block_received",
        "Total number of blocks received by peers"
    );

    pub static ref PEER_TRANSACTION_RECEIVED: near_metrics::Result<IntCounter> = try_create_int_counter(
        "peer_data_received",
        "Total number of transactions received by peers"
    );
}
