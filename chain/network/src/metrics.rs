use near_metrics::{try_create_int_counter, try_create_int_gauge, IntCounter, IntGauge};

macro_rules! type_messages {
    ($name_counter:ident, $name_bytes:ident) => {
        lazy_static! {
            pub static ref $name_counter: near_metrics::Result<IntCounter> = try_create_int_counter(
                stringify!($name_counter),
                concat!("Peer Message ", stringify!($name_counter)),
            );
            pub static ref $name_bytes: near_metrics::Result<IntCounter> = try_create_int_counter(
                stringify!($name_bytes),
                concat!("Peer Message ", stringify!($name_bytes)),
            );
        }
    };
}

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
    pub static ref PEER_CLIENT_MESSAGE_RECEIVED_TOTAL: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "peer_client_message_received_total",
            "Total number of messages for client received from peers"
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
    pub static ref EDGE_ACTIVE: near_metrics::Result<IntGauge> =
        try_create_int_gauge(
            "edge_active",
            "Total edges active between peers"
        );
    pub static ref EDGE_INACTIVE: near_metrics::Result<IntGauge> =
        try_create_int_gauge(
            "edge_inactive",
            "Total edges that where active and are currently inactive"
        );
    pub static ref PEER_REACHABLE: near_metrics::Result<IntGauge> =
        try_create_int_gauge(
            "peer_reachable",
            "Total peers such that there is a path potentially through other peers"
        );
    pub static ref ACCOUNT_KNOWN: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "account_known",
            "Total accounts known"
        );
    pub static ref DROP_MESSAGE_UNKNOWN_ACCOUNT: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "drop_message_unknown_account",
            "Total messages dropped because target account is not known"
        );
    pub static ref DROP_MESSAGE_UNREACHABLE_PEER: near_metrics::Result<IntCounter> =
        try_create_int_counter(
            "drop_message_unreachable_peer",
            "Total messages dropped because target peer is not reachable"
        );
    pub static ref RECEIVED_INFO_ABOUT_ITSELF: near_metrics::Result<IntCounter> = try_create_int_counter("received_info_about_itself", "Number of times a peer tried to connect to itself");
}

type_messages!(HANDSHAKE_RECEIVED_TOTAL, HANDSHAKE_RECEIVED_BYTES);
type_messages!(HANDSHAKE_FAILURE_RECEIVED_TOTAL, HANDSHAKE_FAILURE_RECEIVED_BYTES);
type_messages!(SYNC_RECEIVED_TOTAL, SYNC_RECEIVED_BYTES);
type_messages!(REQUEST_UPDATE_NONCE_RECEIVED_TOTAL, REQUEST_UPDATE_NONCE_RECEIVED_BYTES);
type_messages!(RESPONSE_UPDATE_NONCE_RECEIVED_TOTAL, RESPONSE_UPDATE_NONCE_RECEIVED_BYTES);
type_messages!(LAST_EDGE_RECEIVED_TOTAL, LAST_EDGE_RECEIVED_BYTES);
type_messages!(PEERS_REQUEST_RECEIVED_TOTAL, PEERS_REQUEST_RECEIVED_BYTES);
type_messages!(PEERS_RESPONSE_RECEIVED_TOTAL, PEERS_RESPONSE_RECEIVED_BYTES);
type_messages!(BLOCK_HEADERS_REQUEST_RECEIVED_TOTAL, BLOCK_HEADERS_REQUEST_RECEIVED_BYTES);
type_messages!(BLOCK_HEADERS_RECEIVED_TOTAL, BLOCK_HEADERS_RECEIVED_BYTES);
type_messages!(BLOCK_HEADER_ANNOUNCE_RECEIVED_TOTAL, BLOCK_HEADER_ANNOUNCE_RECEIVED_BYTES);
type_messages!(BLOCK_REQUEST_RECEIVED_TOTAL, BLOCK_REQUEST_RECEIVED_BYTES);
type_messages!(BLOCK_RECEIVED_TOTAL, BLOCK_RECEIVED_BYTES);
type_messages!(TRANSACTION_RECEIVED_TOTAL, TRANSACTION_RECEIVED_BYTES);
type_messages!(ROUTED_STATE_REQUEST_RECEIVED_TOTAL, ROUTED_STATE_REQUEST_RECEIVED_BYTES);
type_messages!(ROUTED_STATE_RESPONSE_RECEIVED_TOTAL, ROUTED_STATE_RESPONSE_RECEIVED_BYTES);
type_messages!(ROUTED_BLOCK_APPROVAL_RECEIVED_TOTAL, ROUTED_BLOCK_APPROVAL_RECEIVED_BYTES);
type_messages!(ROUTED_FORWARD_TX_RECEIVED_TOTAL, ROUTED_FORWARD_TX_RECEIVED_BYTES);
type_messages!(ROUTED_TX_STATUS_REQUEST_RECEIVED_TOTAL, ROUTED_TX_STATUS_REQUEST_RECEIVED_BYTES);
type_messages!(ROUTED_TX_STATUS_RESPONSE_RECEIVED_TOTAL, ROUTED_TX_STATUS_RESPONSE_RECEIVED_BYTES);
type_messages!(ROUTED_QUERY_REQUEST_RECEIVED_TOTAL, ROUTED_QUERY_REQUEST_RECEIVED_BYTES);
type_messages!(ROUTED_QUERY_RESPONSE_RECEIVED_TOTAL, ROUTED_QUERY_RESPONSE_RECEIVED_BYTES);
type_messages!(
    ROUTED_RECEIPT_OUTCOME_REQUEST_RECEIVED_TOTAL,
    ROUTED_RECEIPT_OUTCOME_REQUEST_RECEIVED_BYTES
);
type_messages!(
    ROUTED_RECEIPT_OUTCOME_RESPONSE_RECEIVED_TOTAL,
    ROUTED_RECEIPT_OUTCOME_RESPONSE_RECEIVED_BYTES
);
type_messages!(
    ROUTED_STATE_REQUEST_HEADER_RECEIVED_TOTAL,
    ROUTED_STATE_REQUEST_HEADER_RECEIVED_BYTES
);
type_messages!(ROUTED_STATE_REQUEST_PART_RECEIVED_TOTAL, ROUTED_STATE_REQUEST_PART_RECEIVED_BYTES);
type_messages!(
    ROUTED_PARTIAL_CHUNK_REQUEST_RECEIVED_TOTAL,
    ROUTED_PARTIAL_CHUNK_REQUEST_RECEIVED_BYTES
);
type_messages!(
    ROUTED_PARTIAL_CHUNK_RESPONSE_RECEIVED_TOTAL,
    ROUTED_PARTIAL_CHUNK_RESPONSE_RECEIVED_BYTES
);
type_messages!(ROUTED_PARTIAL_CHUNK_RECEIVED_TOTAL, ROUTED_PARTIAL_CHUNK_RECEIVED_BYTES);
type_messages!(ROUTED_PING_RECEIVED_TOTAL, ROUTED_PING_RECEIVED_BYTES);
type_messages!(ROUTED_PONG_RECEIVED_TOTAL, ROUTED_PONG_RECEIVED_BYTES);
type_messages!(DISCONNECT_RECEIVED_TOTAL, DISCONNECT_RECEIVED_BYTES);
type_messages!(CHALLENGE_RECEIVED_TOTAL, CHALLENGE_RECEIVED_BYTES);
