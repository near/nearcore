use crate::network_protocol::Encoding;
use near_metrics::{
    exponential_buckets, try_create_histogram, try_create_histogram_vec,
    try_create_histogram_with_buckets, try_create_int_counter, try_create_int_counter_vec,
    try_create_int_gauge, try_create_int_gauge_vec, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, MetricVec, MetricVecBuilder,
};
use near_network_primitives::time;
use near_network_primitives::types::{PeerType, RoutedMessageBody, RoutedMessageV2};
use once_cell::sync::Lazy;

/// Labels represents a schema of an IntGaugeVec metric.
pub trait Labels: 'static {
    /// Array should be [&'static str;N], where N is the number of labels.
    type Array: AsRef<[&'static str]>;
    /// Names of the gauge vector labels.
    const NAMES: Self::Array;
    /// Converts self to a list of label values.
    /// values().len() should be always equal to names().len().
    fn values(&self) -> Self::Array;
}

/// Type-safe wrapper of IntGaugeVec.
pub struct Gauge<L: Labels> {
    inner: IntGaugeVec,
    _labels: std::marker::PhantomData<L>,
}

pub struct GaugePoint(IntGauge);

impl<L: Labels> Gauge<L> {
    /// Constructs a new prometheus Gauge with schema `L`.
    pub fn new(name: &str, help: &str) -> Result<Self, near_metrics::prometheus::Error> {
        Ok(Self {
            inner: near_metrics::try_create_int_gauge_vec(name, help, L::NAMES.as_ref())?,
            _labels: std::marker::PhantomData,
        })
    }

    /// Adds a point represented by `labels` to the gauge.
    /// Returns a guard of the point - when the guard is dropped
    /// the point is removed from the gauge.
    pub fn new_point(&'static self, labels: &L) -> GaugePoint {
        let point = self.inner.with_label_values(labels.values().as_ref());
        point.inc();
        GaugePoint(point)
    }
}

impl Drop for GaugePoint {
    fn drop(&mut self) {
        self.0.dec();
    }
}

pub struct Connection {
    pub type_: PeerType,
    pub encoding: Option<Encoding>,
}

impl Labels for Connection {
    type Array = [&'static str; 2];
    const NAMES: Self::Array = ["peer_type", "encoding"];
    fn values(&self) -> Self::Array {
        [self.type_.into(), self.encoding.map(|e| e.into()).unwrap_or("unknown")]
    }
}

pub(crate) struct MetricGuard<T: MetricVecBuilder + 'static> {
    metric: T::M,
    metric_vec: &'static MetricVec<T>,
    labels: Vec<String>,
}

impl<T: MetricVecBuilder> MetricGuard<T> {
    pub fn new(metric_vec: &'static MetricVec<T>, labels: Vec<String>) -> Self {
        let labels_str: Vec<_> = labels.iter().map(String::as_str).collect();
        Self { metric: metric_vec.with_label_values(&labels_str[..]), metric_vec, labels }
    }
}

impl<T: MetricVecBuilder> Drop for MetricGuard<T> {
    fn drop(&mut self) {
        let labels: Vec<_> = self.labels.iter().map(String::as_str).collect();
        self.metric_vec.remove_label_values(&labels[..]).unwrap();
    }
}

impl<T: MetricVecBuilder> std::ops::Deref for MetricGuard<T> {
    type Target = T::M;
    fn deref(&self) -> &Self::Target {
        &self.metric
    }
}

pub static PEER_CONNECTIONS: Lazy<Gauge<Connection>> =
    Lazy::new(|| Gauge::new("near_peer_connections", "Number of connected peers").unwrap());

pub(crate) static PEER_CONNECTIONS_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_peer_connections_total", "Number of connected peers").unwrap()
});
pub(crate) static PEER_DATA_RECEIVED_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_peer_data_received_bytes", "Total data received from peers")
        .unwrap()
});

pub(crate) static PEER_MSG_SIZE_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_peer_msg_size",
        "Histogram of message sizes in bytes",
        &["addr"],
        // very coarse buckets, because we keep them for every connection
        // separately.
        // TODO(gprusak): this might get too expensive with TIER1 connections.
        Some(exponential_buckets(100., 10., 6).unwrap()),
    )
    .unwrap()
});

pub(crate) static PEER_MSG_READ_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_peer_msg_read_latency",
        "Time that PeerActor spends on reading a message from a socket",
        exponential_buckets(0.001, 1.3, 35).unwrap(),
    )
    .unwrap()
});

pub(crate) static PEER_DATA_SENT_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_peer_data_sent_bytes", "Total data sent to peers").unwrap()
});
pub(crate) static PEER_DATA_WRITE_BUFFER_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_peer_write_buffer_size",
        "Size of the outgoing buffer for this peer",
        &["addr"],
    )
    .unwrap()
});
pub(crate) static PEER_MESSAGE_RECEIVED_BY_TYPE_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_peer_message_received_by_type_bytes",
        "Total data received from peers by message types",
        &["type"],
    )
    .unwrap()
});
pub(crate) static PEER_MESSAGE_RECEIVED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_peer_message_received_total",
        "Number of messages received from peers",
    )
    .unwrap()
});
pub(crate) static PEER_MESSAGE_RECEIVED_BY_TYPE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_peer_message_received_by_type_total",
        "Number of messages received from peers by message types",
        &["type"],
    )
    .unwrap()
});
pub(crate) static PEER_MESSAGE_SENT_BY_TYPE_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_peer_message_sent_by_type_bytes",
        "Total data sent to peers by message types",
        &["type"],
    )
    .unwrap()
});
pub(crate) static PEER_MESSAGE_SENT_BY_TYPE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_peer_message_sent_by_type_total",
        "Number of messages sent to peers by message types",
        &["type"],
    )
    .unwrap()
});
pub(crate) static PEER_CLIENT_MESSAGE_RECEIVED_BY_TYPE_TOTAL: Lazy<IntCounterVec> =
    Lazy::new(|| {
        try_create_int_counter_vec(
            "near_peer_client_message_received_by_type_total",
            "Number of messages for client received from peers, by message types",
            &["type"],
        )
        .unwrap()
    });
pub(crate) static PEER_VIEW_CLIENT_MESSAGE_RECEIVED_BY_TYPE_TOTAL: Lazy<IntCounterVec> =
    Lazy::new(|| {
        try_create_int_counter_vec(
            "near_peer_view_client_message_received_by_type_total",
            "Number of messages for view client received from peers, by message types",
            &["type"],
        )
        .unwrap()
    });
pub(crate) static REQUEST_COUNT_BY_TYPE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_requests_count_by_type_total",
        "Number of network requests we send out, by message types",
        &["type"],
    )
    .unwrap()
});

// Routing table metrics
pub(crate) static ROUTING_TABLE_RECALCULATIONS: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_routing_table_recalculations_total",
        "Number of times routing table have been recalculated from scratch",
    )
    .unwrap()
});
pub(crate) static ROUTING_TABLE_RECALCULATION_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_routing_table_recalculation_seconds",
        "Time spent recalculating routing table",
    )
    .unwrap()
});
pub(crate) static EDGE_UPDATES: Lazy<IntCounter> =
    Lazy::new(|| try_create_int_counter("near_edge_updates", "Unique edge updates").unwrap());
pub(crate) static EDGE_NONCE: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_edge_nonce", "Edge nonce types", &["type"]).unwrap()
});
pub(crate) static EDGE_ACTIVE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_edge_active", "Total edges active between peers").unwrap()
});
pub(crate) static EDGE_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_edge_total", "Total edges between peers (including removed ones).")
        .unwrap()
});

pub(crate) static EDGE_TOMBSTONE_SENDING_SKIPPED: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_edge_tombstone_sending_skip",
        "Number of times that we didn't send tombstones.",
    )
    .unwrap()
});

pub(crate) static EDGE_TOMBSTONE_RECEIVING_SKIPPED: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_edge_tombstone_receiving_skip",
        "Number of times that we pruned tombstones upon receiving.",
    )
    .unwrap()
});

pub(crate) static PEER_UNRELIABLE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_peer_unreliable",
        "Total peers that are behind and will not be used to route messages",
    )
    .unwrap()
});
pub(crate) static PEER_MANAGER_TRIGGER_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_peer_manager_trigger_time",
        "Time that PeerManagerActor spends on different types of triggers",
        &["trigger"],
        Some(exponential_buckets(0.0001, 2., 15).unwrap()),
    )
    .unwrap()
});
pub(crate) static PEER_MANAGER_MESSAGES_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_peer_manager_messages_time",
        "Time that PeerManagerActor spends on handling different types of messages",
        &["message"],
        Some(exponential_buckets(0.0001, 2., 15).unwrap()),
    )
    .unwrap()
});
pub(crate) static ROUTING_TABLE_MESSAGES_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_routing_actor_messages_time",
        "Time that routing table actor spends on handling different types of messages",
        &["message"],
        Some(exponential_buckets(0.0001, 2., 15).unwrap()),
    )
    .unwrap()
});
pub(crate) static ROUTED_MESSAGE_DROPPED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_routed_message_dropped",
        "Number of messages dropped due to TTL=0, by routed message type",
        &["type"],
    )
    .unwrap()
});

pub(crate) static PEER_REACHABLE: Lazy<IntGauge> = Lazy::new(|| {
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
pub(crate) static PARTIAL_ENCODED_CHUNK_REQUEST_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "partial_encoded_chunk_request_delay",
        "Delay between when a partial encoded chunk request is sent from ClientActor and when it is received by PeerManagerActor",
    )
        .unwrap()
});

pub(crate) static BROADCAST_MESSAGES: Lazy<IntCounterVec> = Lazy::new(|| {
    near_metrics::try_create_int_counter_vec(
        "near_broadcast_msg",
        "Broadcasted messages",
        &["type"],
    )
    .unwrap()
});

static NETWORK_ROUTED_MSG_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_network_routed_msg_latency",
        "Latency of network messages, assuming clocks are perfectly synchronized",
        &["routed"],
        Some(exponential_buckets(0.0001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static CONNECTED_TO_MYSELF: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_connected_to_myself",
        "This node connected to itself, this shouldn't happen",
    )
    .unwrap()
});

// The routed message received its destination. If the timestamp of creation of this message is
// known, then update the corresponding latency metric histogram.
pub(crate) fn record_routed_msg_latency(clock: &time::Clock, msg: &RoutedMessageV2) {
    if let Some(created_at) = msg.created_at {
        let now = clock.now_utc();
        let duration = now - created_at;
        NETWORK_ROUTED_MSG_LATENCY
            .with_label_values(&[msg.body_variant()])
            .observe(duration.as_seconds_f64());
    }
}

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
