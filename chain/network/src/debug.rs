use ::actix::Message;
use near_primitives::views::{NetworkGraphView, PeerStoreView, RecentOutboundConnectionsView};

// Different debug requests that can be sent by HTML pages, via GET.
pub enum GetDebugStatus {
    PeerStore,
    Graph,
    RecentOutboundConnections,
}

#[derive(actix::MessageResponse, Debug)]
pub enum DebugStatus {
    PeerStore(PeerStoreView),
    Graph(NetworkGraphView),
    RecentOutboundConnections(RecentOutboundConnectionsView),
}

impl Message for GetDebugStatus {
    type Result = DebugStatus;
}
