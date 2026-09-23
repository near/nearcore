use near_primitives::views::{
    NetworkGraphView, PeerStoreView, RecentOutboundConnectionsView, SnapshotHostsView,
};

// Different debug requests that can be sent by HTML pages, via GET.
#[derive(Debug)]
pub enum GetDebugStatus {
    PeerStore,
    Graph,
    RecentOutboundConnections,
    SnapshotHosts,
}

#[derive(Debug)]
pub enum DebugStatus {
    PeerStore(PeerStoreView),
    Graph(NetworkGraphView),
    RecentOutboundConnections(RecentOutboundConnectionsView),
    SnapshotHosts(SnapshotHostsView),
}
