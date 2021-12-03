# 9. Metrics reported to Prometheus's
`near-network` reports various metrics to Prometheus.
See `chain/network/src/stats/metrics.rs` for details.

Here are metrics we currently use:
```rust
/// Number of connected peers.
pub static PEER_CONNECTIONS_TOTAL: IntGauge;
/// Total data received from peers.
pub static PEER_DATA_RECEIVED_BYTES: IntCounter;
/// Number of messages received from peers.
pub static PEER_MESSAGE_RECEIVED_TOTAL: IntCounter;
/// Number of messages for client received from peers.
pub static PEER_CLIENT_MESSAGE_RECEIVED_TOTAL: IntCounter;
/// Number of blocks received by peers.
pub static PEER_BLOCK_RECEIVED_TOTAL: IntCounter;
/// Number of transactions received by peers.
pub static PEER_TRANSACTION_RECEIVED_TOTAL: IntCounter;
/// ---- Routing table metrics ----
/// Number of times routing table have been recalculated from scratch.
pub static ROUTING_TABLE_RECALCULATIONS: IntCounter;
/// Time spent recalculating routing table
pub static ROUTING_TABLE_RECALCULATION_HISTOGRAM: Histogram;

/// Unique edge updates.
pub static EDGE_UPDATES: IntCounter;
/// Total edges active between peers.
pub static EDGE_ACTIVE: IntGauge;
/// Total peers such that there is a path potentially through other peers.
pub static PEER_REACHABLE: IntGauge;
/// Total messages dropped because target account is not known.
pub static DROP_MESSAGE_UNKNOWN_ACCOUNT: IntCounter;
/// Number of times a peer tried to connect to itself.
pub static RECEIVED_INFO_ABOUT_ITSELF: IntCounter;
/// Total count of messages which were dropped, because write buffer was full.
pub static DROPPED_MESSAGES_COUNT: IntCounter;
```
