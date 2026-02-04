use std::fmt::Debug;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::messaging::{AsyncSendError, CanSend, CanSendAsync, Handler};
use crate::multithread::runtime_handle::{MultithreadRuntimeHandle, MultithreadRuntimeMessage};
use crate::{next_message_sequence_num, pretty_type_name};

impl<A, M> CanSend<M> for MultithreadRuntimeHandle<A>
where
    A: Handler<M> + 'static,
    M: Debug + Send + 'static,
{
    fn send(&self, message: M) {
        let seq = next_message_sequence_num();
        let message_type = pretty_type_name::<M>();
        tracing::trace!(seq, message_type, "sending sync message");

        let function = |actor: &mut A| {
            actor.handle(message);
        };

        let message = MultithreadRuntimeMessage {
            seq,
            enqueued_time_ns: self.instrumentation.current_time(),
            name: message_type,
            function: Box::new(function),
        };
        if let Err(_) = self.send_message(message) {
            tracing::info!(seq, "ignoring sync message, receiving actor is being shut down");
        }
    }
}

impl<A, M, R> CanSendAsync<M, R> for MultithreadRuntimeHandle<A>
where
    A: Handler<M, R> + 'static,
    M: Debug + Send + 'static,
    R: Send + 'static,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        let seq = next_message_sequence_num();
        let message_type = pretty_type_name::<M>();
        tracing::trace!(seq, message_type, ?message, "sending async message");

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move { receiver.await.map_err(|_| AsyncSendError::Dropped) };
        let function = move |actor: &mut A| {
            let result = actor.handle(message);
            sender.send(result).ok(); // OK if the sender doesn't care about the result anymore.
        };

        let message = MultithreadRuntimeMessage {
            seq,
            enqueued_time_ns: self.instrumentation.current_time(),
            name: message_type,
            function: Box::new(function),
        };
        if let Err(_) = self.send_message(message) {
            async { Err(AsyncSendError::Dropped) }.boxed()
        } else {
            future.boxed()
        }
    }
}
