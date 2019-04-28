use actix::{
    Actor, ActorContext, ActorFuture, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner,
    Handler, Message, Recipient, StreamHandler, System, SystemService, WrapFuture,
};

use near_chain::RuntimeAdapter;

#[derive(Message)]
pub struct ProduceBlock {
}

/// Actor that produces blocks.
pub struct ValidatorActor {
    runtime_adapter: Arc<RuntimeAdapter>
}

impl ValidatorActor {
    pub fn new(runtime_adapter: Arc<RuntimeAdapter>) -> Self {
        ValidatorActor { runtime_adapter }
    }
}

impl Actor for ValidatorActor {
    type Context = Context<Self>;
}

impl Handler<ProduceBlock> for ValidatorActor {
    type Result = ();

    fn handler(&mut self, msg: ProduceBlock, ctx: &mut Self::Context) {

    }
}
