use near_async::Message;
use near_primitives::views::NetworkRoutesView;
use near_primitives::views::{
    NetworkGraphView, PeerStoreView, RecentOutboundConnectionsView, SnapshotHostsView,
};

// Different debug requests that can be sent by HTML pages, via GET.
#[derive(Message, Debug)]
pub enum GetDebugStatus {
    PeerStore,
    Graph,
    RecentOutboundConnections,
    Routes,
    SnapshotHosts,
}

#[derive(Debug)]
pub enum DebugStatus {
    PeerStore(PeerStoreView),
    Graph(NetworkGraphView),
    RecentOutboundConnections(RecentOutboundConnectionsView),
    Routes(NetworkRoutesView),
    SnapshotHosts(SnapshotHostsView),
}
