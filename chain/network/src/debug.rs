use ::actix::Message;
use near_primitives::views::{NetworkGraphView, PeerStoreView};

// Different debug requests that can be sent by HTML pages, via GET.
pub enum GetDebugStatus {
    PeerStore,
    Graph,
}

#[derive(actix::MessageResponse, Debug)]
pub enum DebugStatus {
    PeerStore(PeerStoreView),
    Graph(NetworkGraphView),
}

impl Message for GetDebugStatus {
    type Result = DebugStatus;
}
