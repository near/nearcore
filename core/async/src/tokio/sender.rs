use std::fmt::Debug;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::instrumentation::InstrumentedThreadWriterSharedPart;
use crate::messaging::{AsyncSendError, CanSend, CanSendAsync, HandlerWithContext};
use crate::tokio::runtime_handle::{TokioRuntimeHandle, TokioRuntimeMessage};
use crate::{next_message_sequence_num, pretty_type_name};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

impl<A, M> CanSend<M> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M> + 'static,
    M: Debug + Send + 'static,
{
    fn send(&self, message: M) {
        let seq = next_message_sequence_num();
        let message_type = pretty_type_name::<M>();
        tracing::trace!(seq, message_type, ?message, "sending sync message");

        let function = |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            actor.handle(message, ctx);
        };

        let message = TokioRuntimeMessage {
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

impl<A, M, R> CanSendAsync<M, R> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M, R> + 'static,
    M: Debug + Send + 'static,
    R: Send + 'static,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        let seq = next_message_sequence_num();
        let message_type = pretty_type_name::<M>();
        tracing::trace!(seq, message_type, ?message, "sending async message");
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move { receiver.await.map_err(|_| AsyncSendError::Dropped) };
        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message, ctx);
            sender.send(result).ok(); // OK if the sender doesn't care about the result anymore.
        };
        let message = TokioRuntimeMessage {
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

impl<A> FutureSpawner for TokioRuntimeHandle<A> {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>) {
        tracing::trace!(description, "spawning future");
        self.runtime_handle.spawn(InstrumentingFuture::new(
            description,
            f,
            self.instrumentation.clone(),
        ));
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
        tracing::debug!(seq, name, "sending delayed action");
        let handle = self.clone();
        self.runtime_handle.spawn(async move {
            tokio::time::sleep(dur.unsigned_abs()).await;
            let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| f(actor, ctx);
            let message = TokioRuntimeMessage {
                seq,
                enqueued_time_ns: handle.instrumentation.current_time(),
                name,
                function: Box::new(function),
            };
            // It's ok for this to fail; it means the runtime is shutting down already.
            handle.send_message(message).ok();
        });
    }
}

/// Instruments the future, recording executions and manages its existence in the queue.
struct InstrumentingFuture {
    description: &'static str,
    future: futures::future::BoxFuture<'static, ()>,
    instrumentation: Arc<InstrumentedThreadWriterSharedPart>,
}

impl InstrumentingFuture {
    pub fn new(
        description: &'static str,
        future: futures::future::BoxFuture<'static, ()>,
        shared_instrumentation: Arc<InstrumentedThreadWriterSharedPart>,
    ) -> Self {
        shared_instrumentation.queue().enqueue(description);
        Self { description, future, instrumentation: shared_instrumentation }
    }
}

impl Future for InstrumentingFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.instrumentation.with_thread_local_writer(|writer| {
            writer.start_event(
                self.description,
                0, /* we don't know the dequeue time unfortunately */
            )
        });
        let result = Pin::new(&mut self.future).poll(cx);
        self.instrumentation.with_thread_local_writer(|writer| writer.end_event(&self.description));
        if let Poll::Ready(()) = result {
            self.instrumentation.queue().dequeue(self.description);
        }
        result
    }
}
