use std::fmt::Debug;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::messaging::{
    AsyncSendError, CanSend, CanSendAsync, Handler, Message, MessageWithCallback,
};
use crate::multithread::runtime_handle::{MultithreadRuntimeHandle, MultithreadRuntimeMessage};
use crate::pretty_type_name;

impl<A, M> CanSend<M> for MultithreadRuntimeHandle<A>
where
    A: Handler<M> + 'static,
    M: Message + Debug + Send + 'static,
{
    fn send(&self, message: M) {
        let description = format!("{}({:?})", pretty_type_name::<A>(), &message);
        tracing::debug!(target: "multithread_runtime", "Sending sync message: {}", description);

        let function = |actor: &mut A| {
            actor.handle(message);
        };

        let message = MultithreadRuntimeMessage { description, function: Box::new(function) };
        self.sender.send(message).unwrap();
    }
}

// Compatibility layer for multi-send style adapters.
impl<A, M, R> CanSend<MessageWithCallback<M, R>> for MultithreadRuntimeHandle<A>
where
    A: Handler<M, R> + 'static,
    M: Message + Debug + Send + 'static,
    R: Send + 'static,
{
    fn send(&self, message: MessageWithCallback<M, R>) {
        let description = format!("{}({:?})", pretty_type_name::<A>(), &message);
        tracing::debug!(target: "multithread_runtime", "Sending sync message with callback: {}", description);

        let function = move |actor: &mut A| {
            let result = actor.handle(message.message);
            (message.callback)(std::future::ready(Ok(result)).boxed());
        };

        let message = MultithreadRuntimeMessage { description, function: Box::new(function) };
        self.sender.send(message).unwrap();
    }
}

impl<A, M, R> CanSendAsync<M, R> for MultithreadRuntimeHandle<A>
where
    A: Handler<M, R> + 'static,
    M: Message + Debug + Send + 'static,
    R: Debug + Send + 'static,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        let description = format!("{}({:?})", pretty_type_name::<A>(), &message);
        tracing::debug!(target: "multithread_runtime", "Sending async message: {}", description);

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move { receiver.await.map_err(|_| AsyncSendError::Dropped) };
        let function = move |actor: &mut A| {
            let result = actor.handle(message);
            sender.send(result).unwrap();
        };

        let message = MultithreadRuntimeMessage { description, function: Box::new(function) };
        self.sender.send(message).unwrap();
        future.boxed()
    }
}
