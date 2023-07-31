use ::actix::Message;
use near_primitives::views::{
    NetworkGraphView, NetworkRoutesView, PeerStoreView, RecentOutboundConnectionsView,
};

// Different debug requests that can be sent by HTML pages, via GET.
pub enum GetDebugStatus {
    PeerStore,
    Graph,
    RecentOutboundConnections,
    Routes,
}

#[derive(actix::MessageResponse, Debug)]
pub enum DebugStatus {
    PeerStore(PeerStoreView),
    Graph(NetworkGraphView),
    RecentOutboundConnections(RecentOutboundConnectionsView),
    Routes(NetworkRoutesView),
}

impl Message for GetDebugStatus {
    type Result = DebugStatus;
}
