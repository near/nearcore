use crate::network_protocol::Encoding;
use crate::network_protocol::{RoutedMessageBody, RoutedMessageV2};
use crate::tcp;
use crate::types::PeerType;
use near_async::time;
use near_o11y::metrics::prometheus;
use near_o11y::metrics::{
    exponential_buckets, try_create_histogram, try_create_histogram_vec,
    try_create_histogram_with_buckets, try_create_int_counter, try_create_int_counter_vec,
    try_create_int_gauge, try_create_int_gauge_vec, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, MetricVec, MetricVecBuilder,
};
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
    pub fn new(name: &str, help: &str) -> Result<Self, near_o11y::metrics::prometheus::Error> {
        Ok(Self {
            inner: try_create_int_gauge_vec(name, help, L::NAMES.as_ref())?,
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

pub(crate) struct MetricGuard<M: prometheus::core::Metric> {
    metric: M,
    drop: Option<Box<dyn Send + Sync + FnOnce()>>,
}

impl<M: prometheus::core::Metric> MetricGuard<M> {
    pub fn new<T: MetricVecBuilder<M = M>>(
        metric_vec: &'static MetricVec<T>,
        labels: Vec<String>,
    ) -> Self {
        let labels_str: Vec<_> = labels.iter().map(String::as_str).collect();
        Self {
            metric: metric_vec.with_label_values(&labels_str[..]),
            drop: Some(Box::new(move || {
                // This can return an error in tests, when multiple PeerManagerActors
                // connect to the same endpoint.
                let labels: Vec<_> = labels.iter().map(String::as_str).collect();
                let _ = metric_vec.remove_label_values(&labels[..]);
            })),
        }
    }
}

impl<M: prometheus::core::Metric> Drop for MetricGuard<M> {
    fn drop(&mut self) {
        self.drop.take().map(|f| f());
    }
}

impl<M: prometheus::core::Metric> std::ops::Deref for MetricGuard<M> {
    type Target = M;
    fn deref(&self) -> &Self::Target {
        &self.metric
    }
}

pub(crate) type IntGaugeGuard = MetricGuard<prometheus::IntGauge>;

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
        "near_peer_msg_size_bytes",
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

pub(crate) static PEER_DATA_READ_BUFFER_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_peer_read_buffer_size",
        "Size of the message that this peer is currently sending to us",
        &["addr"],
    )
    .unwrap()
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
pub(crate) static SYNC_ACCOUNTS_DATA: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_sync_accounts_data",
        "Number of SyncAccountsData messages sent/received",
        &["direction", "incremental", "requesting_full_sync"],
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
static DROPPED_MESSAGE_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_dropped_message_by_type_and_reason_count",
        "Total count of messages which were dropped by type of message and \
         reason why the message has been dropped",
        &["type", "reason"],
    )
    .unwrap()
});
pub(crate) static PARTIAL_ENCODED_CHUNK_REQUEST_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_partial_encoded_chunk_request_delay",
        "Delay between when a partial encoded chunk request is sent from ClientActor and when it is received by PeerManagerActor",
    )
        .unwrap()
});

pub(crate) static BROADCAST_MESSAGES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_broadcast_msg", "Broadcasted messages", &["type"]).unwrap()
});

static NETWORK_ROUTED_MSG_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_network_routed_msg_latency",
        "Latency of network messages, assuming clocks are perfectly synchronized. 'tier' indicates what is the tier of the connection on which the message arrived (TIER1 is expected to be faster than TIER2) and 'fastest' indicates whether this was the first copy of the message to arrive.",
        &["routed","tier","fastest"],
        Some(exponential_buckets(0.0001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
static NETWORK_ROUTED_MSG_NUM_HOPS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_network_routed_msg_hops",
        "Number of peers the routed message travelled through",
        &["routed", "hops"],
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

pub(crate) static ALREADY_CONNECTED_ACCOUNT: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_already_connected_account",
        "A second peer with the same validator key is trying to connect to our node. This means that the validator peer has invalid setup."
    )
    .unwrap()
});

pub(crate) static ACCOUNT_TO_PEER_LOOKUPS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_account_to_peer_lookups",
        "number of lookups of peer_id by account_id (for routed messages)",
        // Source is either "AnnounceAccount" or "AccountData".
        // We want to deprecate AnnounceAccount, so eventually we want all
        // lookups to be done via AccountData. For now AnnounceAccount is
        // used as a fallback.
        &["source"],
    )
    .unwrap()
});

/// Updated the prometheus metrics about the received routed message `msg`.
/// `tier` indicates the network over which the message was transmitted.
/// `fastest` indicates whether this message is the first copy of `msg` received -
/// important messages are sent multiple times over different routing paths
/// simultaneously to improve the chance that the message will be delivered on time.
pub(crate) fn record_routed_msg_metrics(
    clock: &time::Clock,
    msg: &RoutedMessageV2,
    tier: tcp::Tier,
    fastest: bool,
) {
    record_routed_msg_latency(clock, msg, tier, fastest);
    record_routed_msg_hops(msg);
}

pub(crate) fn bool_to_str(b: bool) -> &'static str {
    match b {
        true => "true",
        false => "false",
    }
}

// The routed message reached its destination. If the timestamp of creation of this message is
// known, then update the corresponding latency metric histogram.
fn record_routed_msg_latency(
    clock: &time::Clock,
    msg: &RoutedMessageV2,
    tier: tcp::Tier,
    fastest: bool,
) {
    if let Some(created_at) = msg.created_at {
        let now = clock.now_utc();
        let duration = now - created_at;
        NETWORK_ROUTED_MSG_LATENCY
            .with_label_values(&[msg.body_variant(), tier.as_ref(), bool_to_str(fastest)])
            .observe(duration.as_seconds_f64());
    }
}

// The routed message reached its destination. If the number of hops is known, then update the
// corresponding metric.
fn record_routed_msg_hops(msg: &RoutedMessageV2) {
    const MAX_NUM_HOPS: i32 = 20;
    // We assume that the number of hops is small.
    // As long as the number of hops is below 10, this metric will not consume too much memory.
    if let Some(num_hops) = msg.num_hops {
        if num_hops >= 0 {
            let num_hops = if num_hops > MAX_NUM_HOPS { MAX_NUM_HOPS } else { num_hops };
            NETWORK_ROUTED_MSG_NUM_HOPS
                .with_label_values(&[msg.body_variant(), &num_hops.to_string()])
                .inc();
        }
    }
}

#[derive(Clone, Copy, strum::AsRefStr)]
pub(crate) enum MessageDropped {
    NoRouteFound,
    UnknownAccount,
    InputTooLong,
    MaxCapacityExceeded,
    TransactionsPerBlockExceeded,
    Duplicate,
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
