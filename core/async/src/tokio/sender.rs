use std::any::type_name;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::messaging::{
    AsyncSendError, CanSend, CanSendAsync, HandlerWithContext, Message, MessageWithCallback,
};
use crate::tokio::runtime_handle::{TokioRuntimeHandle, TokioRuntimeMessage};

static SEQUENCE_NUM: AtomicU64 = AtomicU64::new(0);

impl<A, M> CanSend<M> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M> + 'static,
    M: Message + Debug + Send + 'static,
{
    fn send(&self, message: M) {
        let seq = SEQUENCE_NUM.fetch_add(1, Ordering::Relaxed);
        let handler = pretty_type_name::<A>();
        tracing::trace!(target: "tokio_runtime", seq, handler, ?message, "sending sync message");

        let function =
            |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| actor.handle(message, ctx);

        let message = TokioRuntimeMessage { seq, function: Box::new(function) };
        self.sender.send(message).unwrap();
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
        let seq = SEQUENCE_NUM.fetch_add(1, Ordering::Relaxed);
        let handler = pretty_type_name::<A>();
        tracing::trace!(
            target: "tokio_runtime",
            seq,
            handler,
            ?message,
            "sending sync message with callback"
        );

        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message.message, ctx);
            (message.callback)(std::future::ready(Ok(result)).boxed());
        };

        let message = TokioRuntimeMessage { seq, function: Box::new(function) };
        self.sender.send(message).unwrap();
    }
}

impl<A, M, R> CanSendAsync<M, R> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M, R> + 'static,
    M: Message + Debug + Send + 'static,
    R: Debug + Send + 'static,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        let seq = SEQUENCE_NUM.fetch_add(1, Ordering::Relaxed);
        let handler = pretty_type_name::<A>();
        tracing::trace!(target: "tokio_runtime", seq, handler, ?message, "sending async message");
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move { receiver.await.map_err(|_| AsyncSendError::Dropped) };
        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message, ctx);
            sender.send(result).unwrap();
        };
        let message = TokioRuntimeMessage { seq, function: Box::new(function) };
        self.sender.send(message).unwrap();
        future.boxed()
    }
}

impl<A> FutureSpawner for TokioRuntimeHandle<A> {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>) {
        tracing::debug!(target: "tokio_runtime", description, "spawning future");
        self.runtime.spawn(f);
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
        let seq = SEQUENCE_NUM.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(target: "tokio_runtime", seq, name, "sending delayed action");
        let sender = self.sender.clone();
        self.runtime.spawn(async move {
            tokio::time::sleep(dur.unsigned_abs()).await;
            let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| f(actor, ctx);
            let message = TokioRuntimeMessage { seq, function: Box::new(function) };
            sender.send(message).unwrap();
        });
    }
}

// Quick and dirty way of getting the type name without the module path.
// Does not work for more complex types like std::sync::Arc<std::sync::atomic::AtomicBool<...>>
// example near_chunks::shards_manager_actor::ShardsManagerActor -> ShardsManagerActor
fn pretty_type_name<T>() -> &'static str {
    type_name::<T>().split("::").last().unwrap()
}
