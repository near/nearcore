use crate::network_protocol::Encoding;
use near_metrics::{
    exponential_buckets, try_create_histogram, try_create_histogram_vec, try_create_int_counter,
    try_create_int_counter_vec, try_create_int_gauge, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec,
};
use near_network_primitives::types::{PeerType, RoutedMessageBody};
use once_cell::sync::Lazy;
use std::collections::HashMap;

static PEER_CONNECTIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    near_metrics::try_create_int_gauge_vec(
        "near_peer_connections",
        "Number of connected peers",
        &["peer_type", "encoding"],
    )
    .unwrap()
});

pub fn set_peer_connections(values: HashMap<(PeerType, Option<Encoding>), i64>) {
    for ((pt, enc), v) in values {
        PEER_CONNECTIONS
            .with_label_values(&[pt.into(), enc.map(|e| e.into()).unwrap_or("unknown")])
            .set(v);
    }
}

#[cfg(feature = "test_features")]
use std::sync::{Arc, Mutex};

pub static PEER_CONNECTIONS_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_peer_connections_total", "Number of connected peers").unwrap()
});
pub static PEER_DATA_RECEIVED_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_peer_data_received_bytes", "Total data received from peers")
        .unwrap()
});
pub static PEER_MESSAGE_RECEIVED_BY_TYPE_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_peer_message_received_by_type_bytes",
        "Total data received from peers by message types",
        &["type"],
    )
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
pub static RECEIVED_INFO_ABOUT_ITSELF: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "received_info_about_itself",
        "Number of times a peer tried to connect to itself",
    )
    .unwrap()
});
static DROPPED_MESSAGE_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    near_metrics::try_create_int_counter_vec(
        "near_dropped_message_by_type_and_reason_count",
        "Total count of messages which were dropped by type of message and \
         reason why the message has been dropped",
        &["type", "reason"],
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

static BROADCAST_MESSAGES: Lazy<IntCounterVec> = Lazy::new(|| {
    near_metrics::try_create_int_counter_vec(
        "near_broadcast_msg",
        "Broadcasted messages",
        &["type"],
    )
    .unwrap()
});

pub(crate) static NETWORK_ROUTED_MSG_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_network_routed_msg_latency",
        "Latency of network messages, assuming clocks are perfectly synchronized",
        &["routed"],
        Some(exponential_buckets(0.0001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

#[derive(Clone, Copy, strum::AsRefStr)]
pub(crate) enum MessageDropped {
    NoRouteFound,
    UnknownAccount,
    InputTooLong,
    MaxCapacityExceeded,
}

impl MessageDropped {
    pub fn inc(self, msg: &RoutedMessageBody) {
        self.inc_msg_type(msg.into())
    }

    pub fn inc_unknown_msg(self) {
        self.inc_msg_type("unknown")
    }

    fn inc_msg_type(self, msg_type: &str) {
        let reason = self.as_ref();
        DROPPED_MESSAGE_COUNT.with_label_values(&[msg_type, reason]).inc();
    }
}

#[derive(Clone, Debug, Default, actix::MessageResponse)]
pub struct NetworkMetrics {
    // sent messages (broadcast style)
    #[cfg(feature = "test_features")]
    broadcast_messages: Arc<Mutex<HashMap<&'static str, u64>>>,
}

impl NetworkMetrics {
    pub fn inc_broadcast(&self, message_name: &'static str) {
        BROADCAST_MESSAGES.with_label_values(&[message_name]).inc();

        #[cfg(feature = "test_features")]
        {
            let mut map = self.broadcast_messages.lock().unwrap();
            let count = map.entry(message_name).or_insert(0);
            *count += 1;
        }
    }

    #[cfg(feature = "test_features")]
    pub fn get_broadcast_count(&self, msg_type: &'static str) -> u64 {
        let hm = self.broadcast_messages.lock().unwrap();
        hm.get(msg_type).map_or(0, |v| *v)
    }
}
