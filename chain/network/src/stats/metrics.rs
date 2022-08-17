use crate::network_protocol::Encoding;
use near_metrics::{
    exponential_buckets, try_create_histogram, try_create_histogram_vec, try_create_int_counter,
    try_create_int_counter_vec, try_create_int_gauge, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec,
};
use near_network_primitives::types::{PeerType, RoutedMessageBody};
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

pub static PEER_CONNECTIONS: Lazy<Gauge<Connection>> =
    Lazy::new(|| Gauge::new("near_peer_connections", "Number of connected peers").unwrap());

pub(crate) static PEER_CONNECTIONS_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_peer_connections_total", "Number of connected peers").unwrap()
});
pub(crate) static PEER_DATA_RECEIVED_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_peer_data_received_bytes", "Total data received from peers")
        .unwrap()
});
pub(crate) static PEER_DATA_SENT_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_peer_data_sent_bytes", "Total data sent to peers").unwrap()
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
        "Number of messages received from peers, by message types",
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

pub(crate) static PEER_UNRELIABLE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_peer_unreliable",
        "Total peers that are behind and will not be used to route messages",
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
