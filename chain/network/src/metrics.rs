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

    // Routing table metrics
    pub static ref ROUTING_TABLE_RECALCULATIONS: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "routing_table_recalculations",
            "Number of times routing table have been recalculated from scratch"
        );
    pub static ref ROUTING_TABLE_RECALCULATION_MILLISECONDS: near_metrics::Result<IntGauge> =
        try_create_int_gauge(
            "routing_table_recalculation_milliseconds",
            "Time spent recalculating routing table"
        );
    pub static ref EDGE_UPDATES: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "edge_updates",
            "Total edge updates received not previously known"
        );
    pub static ref EDGE_ACTIVE: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "edge_active",
            "Total edges active between peers"
        );
    pub static ref EDGE_INACTIVE: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "edge_inactive",
            "Total edges that where active and are currently inactive"
        );
    pub static ref PEER_REACHABLE: near_metrics::Result<IntGauge> =
        try_create_int_gauge(
            "peer_reachable",
            "Total peers such that there is a path potentially through other peers"
        );
}
