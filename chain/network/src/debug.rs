use ::actix::Message;
use near_primitives::views::{
    NetworkGraphView, NetworkRoutesView, PeerStoreView, RecentOutboundConnectionsView,
    SnapshotHostsView,
};

// Different debug requests that can be sent by HTML pages, via GET.
pub enum GetDebugStatus {
    PeerStore,
    Graph,
    RecentOutboundConnections,
    Routes,
    SnapshotHosts,
}

#[derive(actix::MessageResponse, Debug)]
pub enum DebugStatus {
    PeerStore(PeerStoreView),
    Graph(NetworkGraphView),
    RecentOutboundConnections(RecentOutboundConnectionsView),
    Routes(NetworkRoutesView),
    SnapshotHosts(SnapshotHostsView),
}

impl Message for GetDebugStatus {
    type Result = DebugStatus;
}
