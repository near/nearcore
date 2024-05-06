use actix::Actor;
use near_o11y::{handler_debug_span, WithSpanContext};

use crate::messaging::Handler;

/// Wrapper on top of a generic actor to make it implement actix::Actor trait. The wrapped actor
/// should implement the Handler trait for all the messages it would like to handle.
/// ActixWrapper is then used to create an actix actor that implements the CanSend trait.
pub struct ActixWrapper<T> {
    actor: T,
}

impl<T> ActixWrapper<T> {
    pub fn new(actor: T) -> Self {
        Self { actor }
    }
}

impl<T> actix::Actor for ActixWrapper<T>
where
    T: Unpin + 'static,
{
    type Context = actix::Context<Self>;
}

impl<M, T> actix::Handler<WithSpanContext<M>> for ActixWrapper<T>
where
    Self: actix::Actor,
    T: Handler<M>,
    M: actix::Message,
    <M as actix::Message>::Result:
        actix::dev::MessageResponse<ActixWrapper<T>, WithSpanContext<M>> + Default,
{
    type Result = M::Result;
    fn handle(&mut self, msg: WithSpanContext<M>, _ctx: &mut Self::Context) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "actix_message_handler", msg);
        self.actor.handle(msg)
    }
}

/// Spawns an actix actor with the given actor. Returns the address of the actor and the arbiter
/// Note that the actor should implement the Handler trait for all the messages it would like to handle.
pub fn spawn_actix_actor<T>(actor: T) -> (actix::Addr<ActixWrapper<T>>, actix::ArbiterHandle)
where
    T: Unpin + Send + 'static,
{
    let actix_wrapper = ActixWrapper::new(actor);
    let arbiter = actix::Arbiter::new().handle();
    let addr = ActixWrapper::<T>::start_in_arbiter(&arbiter, |_| actix_wrapper);
    (addr, arbiter)
}
