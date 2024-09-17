use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo, StateSyncEvent};

pub struct PeerManagerMock {
    handle: Box<
        dyn FnMut(
            PeerManagerMessageRequest,
            &mut actix::Context<Self>,
        ) -> PeerManagerMessageResponse,
    >,
}

impl PeerManagerMock {
    pub(crate) fn new(
        f: impl 'static
            + FnMut(
                PeerManagerMessageRequest,
                &mut actix::Context<Self>,
            ) -> PeerManagerMessageResponse,
    ) -> Self {
        Self { handle: Box::new(f) }
    }
}

impl actix::Actor for PeerManagerMock {
    type Context = actix::Context<Self>;
}

impl actix::Handler<PeerManagerMessageRequest> for PeerManagerMock {
    type Result = PeerManagerMessageResponse;
    fn handle(&mut self, msg: PeerManagerMessageRequest, ctx: &mut Self::Context) -> Self::Result {
        (self.handle)(msg, ctx)
    }
}

impl actix::Handler<SetChainInfo> for PeerManagerMock {
    type Result = ();
    fn handle(&mut self, _msg: SetChainInfo, _ctx: &mut Self::Context) {}
}

impl actix::Handler<StateSyncEvent> for PeerManagerMock {
    type Result = ();
    fn handle(&mut self, _msg: StateSyncEvent, _ctx: &mut Self::Context) {}
}
