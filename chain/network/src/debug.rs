use ::actix::Message;
use near_primitives::views::PeerStoreView;

// Different debug requests that can be sent by HTML pages, via GET.
pub enum GetDebugStatus {
    PeerStore,
}

#[derive(actix::MessageResponse, Debug)]
pub enum DebugStatus {
    PeerStore(PeerStoreView),
}

impl Message for GetDebugStatus {
    type Result = DebugStatus;
}
