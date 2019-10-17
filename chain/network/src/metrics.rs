use near_metrics::{try_create_int_counter, try_create_int_gauge, IntCounter, IntGauge};

lazy_static! {
    pub static ref PEER_CONNECTIONS_TOTAL: near_metrics::Result<IntGauge> =
        try_create_int_gauge("peer_connections_total", "Current number of connected peers");
    pub static ref PEER_DATA_RECEIVED_BYTES: near_metrics::Result<IntCounter> =
        try_create_int_counter("peer_data_received_bytes", "Total data received by peers");
    pub static ref PEER_MESSAGE_RECEIVED_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "peer_message_received_total",
            "Total number of messages received from peers"
        );
    pub static ref PEER_BLOCK_RECEIVED_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "peer_block_received_total",
            "Total number of blocks received by peers"
        );
    pub static ref PEER_TRANSACTION_RECEIVED_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "peer_transaction_received_total",
            "Total number of transactions received by peers"
        );
}
