use std::any::type_name;
use std::fmt::Debug;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::messaging::{
    AsyncSendError, CanSend, CanSendAsync, HandlerWithContext, Message, MessageWithCallback,
};
use crate::tokio::runtime_handle::{TokioRuntimeHandle, TokioRuntimeMessage};

impl<A, M> CanSend<M> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M> + 'static,
    M: Message + Debug + Send + 'static,
{
    fn send(&self, message: M) {
        let description = format!("{}({:?})", pretty_type_name::<A>(), &message);
        tracing::debug!(target: "tokio_runtime", "Sending sync message: {}", description);

        let function =
            |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| actor.handle(message, ctx);

        let message = TokioRuntimeMessage { description, function: Box::new(function) };
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
        let description = format!("{}({:?})", pretty_type_name::<A>(), &message);
        tracing::debug!(target: "tokio_runtime", "Sending sync message with callback: {}", description);

        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message.message, ctx);
            (message.callback)(std::future::ready(Ok(result)).boxed());
        };

        let message = TokioRuntimeMessage { description, function: Box::new(function) };
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
        let description = format!("{}({:?})", pretty_type_name::<A>(), &message);
        tracing::debug!(target: "tokio_runtime", "Sending async message: {}", description);

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move { receiver.await.map_err(|_| AsyncSendError::Dropped) };
        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message, ctx);
            sender.send(result).unwrap();
        };

        let message = TokioRuntimeMessage { description, function: Box::new(function) };
        self.sender.send(message).unwrap();
        future.boxed()
    }
}

impl<A> FutureSpawner for TokioRuntimeHandle<A> {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>) {
        tracing::debug!(target: "tokio_runtime", "Spawning future: FutureSpawn({})", description);
        self.runtime.spawn(f);
    }
}

impl<A> DelayedActionRunner<A> for TokioRuntimeHandle<A>
where
    A: 'static,
{
    fn run_later_boxed(
        &mut self,
        name: &str,
        dur: near_time::Duration,
        f: Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send + 'static>,
    ) {
        let description = format!("DelayedAction {}({:?})", pretty_type_name::<A>(), name);
        tracing::debug!(target: "tokio_runtime", "Sending delayed action: {}", description);

        let sender = self.sender.clone();
        self.runtime.spawn(async move {
            tokio::time::sleep(dur.unsigned_abs()).await;
            let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| f(actor, ctx);
            let message = TokioRuntimeMessage { description, function: Box::new(function) };
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
