use crate::types::PeerMessage;
use near_metrics::{
    do_create_int_counter_vec, try_create_histogram, try_create_int_counter,
    try_create_int_counter_vec, try_create_int_gauge, Histogram, IntCounter, IntCounterVec,
    IntGauge,
};
use near_network_primitives::types::RoutedMessageBody;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use strum::VariantNames;

pub static PEER_CONNECTIONS_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_peer_connections_total", "Number of connected peers").unwrap()
});
pub static PEER_DATA_RECEIVED_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_peer_data_received_bytes", "Total data received from peers")
        .unwrap()
});
pub static PEER_MESSAGE_RECEIVED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_peer_message_received_total",
        "Number of messages received from peers",
    )
    .unwrap()
});
pub static PEER_MESSAGE_RECEIVED_BY_TYPE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_peer_message_received_by_type_total",
        "Number of messages received from peers, by message types",
        &["type"],
    )
    .unwrap()
});
pub static PEER_CLIENT_MESSAGE_RECEIVED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_peer_client_message_received_total",
        "Number of messages for client received from peers",
    )
    .unwrap()
});
pub static PEER_CLIENT_MESSAGE_RECEIVED_BY_TYPE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_peer_client_message_received_by_type_total",
        "Number of messages for client received from peers, by message types",
        &["type"],
    )
    .unwrap()
});
pub static REQUEST_COUNT_BY_TYPE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_requests_count_by_type_total",
        "Number of network requests we send out, by message types",
        &["type"],
    )
    .unwrap()
});

// Routing table metrics
pub static ROUTING_TABLE_RECALCULATIONS: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_routing_table_recalculations_total",
        "Number of times routing table have been recalculated from scratch",
    )
    .unwrap()
});
pub static ROUTING_TABLE_RECALCULATION_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_routing_table_recalculation_seconds",
        "Time spent recalculating routing table",
    )
    .unwrap()
});
pub static EDGE_UPDATES: Lazy<IntCounter> =
    Lazy::new(|| try_create_int_counter("near_edge_updates", "Unique edge updates").unwrap());
pub static EDGE_ACTIVE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_edge_active", "Total edges active between peers").unwrap()
});
pub static PEER_REACHABLE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_peer_reachable",
        "Total peers such that there is a path potentially through other peers",
    )
    .unwrap()
});
pub static DROP_MESSAGE_UNKNOWN_ACCOUNT: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_drop_message_unknown_account",
        "Total messages dropped because target account is not known",
    )
    .unwrap()
});
pub static RECEIVED_INFO_ABOUT_ITSELF: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "received_info_about_itself",
        "Number of times a peer tried to connect to itself",
    )
    .unwrap()
});
pub static DROPPED_MESSAGES_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    near_metrics::try_create_int_counter(
        "near_dropped_messages_count",
        "Total count of messages which were dropped, because write buffer was full",
    )
    .unwrap()
});
pub static PARTIAL_ENCODED_CHUNK_REQUEST_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "partial_encoded_chunk_request_delay",
        "Delay between when a partial encoded chunk request is sent from ClientActor and when it is received by PeerManagerActor",
    )
        .unwrap()
});

#[derive(Clone, Debug, actix::MessageResponse)]
pub struct NetworkMetrics {
    // received messages
    peer_messages: HashMap<String, IntCounter>,
    // sent messages (broadcast style)
    pub broadcast_messages: IntCounterVec,
}

impl NetworkMetrics {
    pub fn new() -> Self {
        Self {
            peer_messages: PeerMessage::VARIANTS
                .iter()
                .filter(|&name| *name != "Routed")
                .chain(RoutedMessageBody::VARIANTS.iter())
                .flat_map(|name: &&str| {
                    [
                        NetworkMetrics::peer_message_total_rx,
                        NetworkMetrics::peer_message_bytes_rx,
                        NetworkMetrics::peer_message_dropped,
                    ]
                    .into_iter()
                    .filter_map(|method| {
                        let counter_name = method(name);
                        try_create_int_counter(&counter_name, &counter_name)
                            .ok()
                            .map(|counter| (counter_name, counter))
                    })
                })
                .collect(),
            broadcast_messages: do_create_int_counter_vec(
                "near_broadcast_msg",
                "Broadcasted messages",
                &["type"],
            ),
        }
    }

    pub fn peer_message_total_rx(message_name: &str) -> String {
        format!("near_{}_total", message_name.to_lowercase())
    }

    pub fn peer_message_bytes_rx(message_name: &str) -> String {
        format!("near_{}_bytes", message_name.to_lowercase())
    }

    pub fn peer_message_dropped(message_name: &str) -> String {
        format!("near_{}_dropped", message_name.to_lowercase())
    }

    pub fn inc(&self, message_name: &str) {
        if let Some(counter) = self.peer_messages.get(message_name) {
            counter.inc();
        }
    }

    pub fn inc_by(&self, message_name: &str, value: u64) {
        if let Some(counter) = self.peer_messages.get(message_name) {
            counter.inc_by(value);
        }
    }
    pub fn inc_broadcast(&self, message_name: &str) {
        self.broadcast_messages.with_label_values(&[message_name]).inc();
    }
}
