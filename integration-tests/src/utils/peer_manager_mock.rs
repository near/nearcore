use near_async::messaging;
use near_network::types::{
    PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo, StateSyncEvent,
    Tier3Request,
};

pub struct PeerManagerMock {
    handle: Box<dyn FnMut(PeerManagerMessageRequest) -> PeerManagerMessageResponse + Send>,
}

impl PeerManagerMock {
    pub(crate) fn new(
        f: impl 'static + FnMut(PeerManagerMessageRequest) -> PeerManagerMessageResponse + Send,
    ) -> Self {
        Self { handle: Box::new(f) }
    }
}

impl messaging::Actor for PeerManagerMock {}

impl messaging::Handler<PeerManagerMessageRequest> for PeerManagerMock {
    fn handle(&mut self, msg: PeerManagerMessageRequest) {
        messaging::Handler::<PeerManagerMessageRequest, PeerManagerMessageResponse>::handle(
            self, msg,
        );
    }
}

impl messaging::Handler<PeerManagerMessageRequest, PeerManagerMessageResponse> for PeerManagerMock {
    fn handle(&mut self, msg: PeerManagerMessageRequest) -> PeerManagerMessageResponse {
        (self.handle)(msg)
    }
}

impl messaging::Handler<SetChainInfo> for PeerManagerMock {
    fn handle(&mut self, _msg: SetChainInfo) {}
}

impl messaging::Handler<StateSyncEvent> for PeerManagerMock {
    fn handle(&mut self, _msg: StateSyncEvent) {}
}

impl messaging::Handler<Tier3Request> for PeerManagerMock {
    fn handle(&mut self, _msg: Tier3Request) {}
}
