use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::futures::DelayedActionRunner;
use crate::messaging::{CanSend, CanSendAsync};
use crate::tokio::traits::{Handler, HandlerWithContext};

// This is what we would use to send the messages to the actors.
// Note that right now this has the parameter DelayedActionRunner, which we would eventually like to get rid of.
pub type MySender<A> =
    mpsc::UnboundedSender<Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send>>;

impl<A, M> CanSend<M> for MySender<A>
where
    A: HandlerWithContext<M> + 'static,
    M: Send + 'static,
{
    fn send(&self, message: M) {
        let function =
            |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| actor.handle(message, ctx);
        // TODO: Figure out what to do with the result
        self.send(Box::new(function)).unwrap();
    }
}

impl<A, M, R> CanSendAsync<M, R> for MySender<A>
where
    A: HandlerWithContext<M, R> + 'static,
    M: Send + 'static,
    R: Send + 'static,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move {
            // TODO: Figure out what to do with the result. Should be fine to unwrap here
            receiver.await.unwrap()
        };
        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message, ctx);
            sender.send(result).ok();
        };
        self.send(Box::new(function)).unwrap();
        future.boxed()
    }
}

/// ********************************************************************************
/// Future implementation without DelayedActionRunner
/// ********************************************************************************
pub type MySender2<A> = mpsc::UnboundedSender<Box<dyn FnOnce(&mut A) + Send>>;

impl<A, M> CanSend<M> for MySender2<A>
where
    A: Handler<M> + 'static,
    M: Send + 'static,
{
    fn send(&self, message: M) {
        let function = |actor: &mut A| actor.handle(message);
        // TODO: Figure out what to do with the result
        self.send(Box::new(function)).unwrap();
    }
}

impl<A, M, R> CanSendAsync<M, R> for MySender2<A>
where
    A: Handler<M, R> + 'static,
    M: Send + 'static,
    R: Send + 'static,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move {
            // TODO: Figure out what to do with the result. Should be fine to unwrap here
            receiver.await.unwrap()
        };
        let function = move |actor: &mut A| {
            let result = actor.handle(message);
            sender.send(result).ok();
        };
        self.send(Box::new(function)).unwrap();
        future.boxed()
    }
}
