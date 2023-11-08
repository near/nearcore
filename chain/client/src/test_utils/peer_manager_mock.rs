use near_network::types::SetChainInfo;
use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse};
use near_o11y::WithSpanContext;

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

impl actix::Handler<WithSpanContext<PeerManagerMessageRequest>> for PeerManagerMock {
    type Result = PeerManagerMessageResponse;
    fn handle(
        &mut self,
        msg: WithSpanContext<PeerManagerMessageRequest>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        (self.handle)(msg.msg, ctx)
    }
}

impl actix::Handler<WithSpanContext<SetChainInfo>> for PeerManagerMock {
    type Result = ();
    fn handle(&mut self, _msg: WithSpanContext<SetChainInfo>, _ctx: &mut Self::Context) {}
}
