use std::fmt::Debug;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::messaging::{
    AsyncSendError, CanSend, CanSendAsync, HandlerWithContext, Message, MessageWithCallback,
};
use crate::tokio::runtime_handle::{TokioRuntimeHandle, TokioRuntimeMessage};
use crate::{next_message_sequence_num, pretty_type_name};

impl<A, M> CanSend<M> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M> + 'static,
    M: Message + Debug + Send + 'static,
{
    fn send(&self, message: M) {
        let seq = next_message_sequence_num();
        let message_type = pretty_type_name::<A>();
        tracing::trace!(target: "tokio_runtime", seq, message_type, ?message, "sending sync message");

        let function = |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            actor.handle(message, ctx);
        };

        let message = TokioRuntimeMessage { seq, function: Box::new(function) };
        if let Err(_) = self.sender.send(message) {
            tracing::info!(target: "tokio_runtime", seq, "Ignoring sync message, receiving actor is being shut down");
        }
    }
}

// Compatibility layer for multi-send style adapters.
impl<A, M, R> CanSend<MessageWithCallback<M, R>> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M, R> + 'static,
    M: Message + Debug + Send + 'static,
    R: Send + 'static,
{
    fn send(&self, message: MessageWithCallback<M, R>) {
        let seq = next_message_sequence_num();
        let message_type = pretty_type_name::<A>();
        tracing::trace!(
            target: "tokio_runtime",
            seq,
            message_type,
            ?message,
            "sending sync message with callback"
        );

        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message.message, ctx);
            (message.callback)(std::future::ready(Ok(result)).boxed());
        };

        let message = TokioRuntimeMessage { seq, function: Box::new(function) };
        if let Err(_) = self.sender.send(message) {
            tracing::info!(target: "tokio_runtime", seq, "Ignoring sync message with callback, receiving actor is being shut down");
        }
    }
}

impl<A, M, R> CanSendAsync<M, R> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M, R> + 'static,
    M: Message + Debug + Send + 'static,
    R: Debug + Send + 'static,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        let seq = next_message_sequence_num();
        let message_type = pretty_type_name::<A>();
        tracing::trace!(target: "tokio_runtime", seq, message_type, ?message, "sending async message");
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move { receiver.await.map_err(|_| AsyncSendError::Dropped) };
        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message, ctx);
            sender.send(result).ok(); // OK if the sender doesn't care about the result anymore.
        };
        let message = TokioRuntimeMessage { seq, function: Box::new(function) };
        if let Err(_) = self.sender.send(message) {
            async { Err(AsyncSendError::Dropped) }.boxed()
        } else {
            future.boxed()
        }
    }
}

impl<A> FutureSpawner for TokioRuntimeHandle<A> {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>) {
        tracing::debug!(target: "tokio_runtime", description, "spawning future");
        self.runtime_handle.spawn(f);
    }
}

impl<A> DelayedActionRunner<A> for TokioRuntimeHandle<A>
where
    A: 'static,
{
    fn run_later_boxed(
        &mut self,
        name: &'static str,
        dur: near_time::Duration,
        f: Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send + 'static>,
    ) {
        let seq = next_message_sequence_num();
        tracing::debug!(target: "tokio_runtime", seq, name, "sending delayed action");
        let sender = self.sender.clone();
        self.runtime_handle.spawn(async move {
            tokio::time::sleep(dur.unsigned_abs()).await;
            let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| f(actor, ctx);
            let message = TokioRuntimeMessage { seq, function: Box::new(function) };
            // It's ok for this to fail; it means the runtime is shutting down already.
            sender.send(message).ok();
        });
    }
}
