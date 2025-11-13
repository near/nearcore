use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::instrumentation::queue::InstrumentedQueue;
use crate::instrumentation::writer::InstrumentedThreadWriterSharedPart;
use crate::messaging::Actor;
use crate::pretty_type_name;
use crate::tokio::runtime::AsyncDroppableRuntime;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// TokioRuntimeMessage is a type alias for a boxed function that can be sent to the Tokio runtime.
pub(super) struct TokioRuntimeMessage<A> {
    pub(super) seq: u64,
    pub(super) enqueued_time_ns: u64,
    pub(super) name: &'static str,
    pub(super) function: Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send>,
}

/// TokioRuntimeHandle is a handle to a Tokio runtime that can be used to send messages to an actor.
/// It allows for sending messages and spawning futures into the Tokio runtime.
pub struct TokioRuntimeHandle<A> {
    /// The sender is used to send messages to the actor running in the Tokio runtime.
    sender: mpsc::UnboundedSender<TokioRuntimeMessage<A>>,
    sender_high: mpsc::UnboundedSender<TokioRuntimeMessage<A>>,
    pub(super) instrumentation: Arc<InstrumentedThreadWriterSharedPart>,
    /// The runtime_handle used to post futures to the Tokio runtime.
    /// This is a handle, meaning it does not prevent the runtime from shutting down.
    /// Runtime shutdown is governed by cancellation only (either via TokioRuntimeHandle::stop() or
    /// ActorSystem::stop()).
    pub(super) runtime_handle: tokio::runtime::Handle,
    /// Cancellation token used to signal shutdown of this specific Tokio runtime.
    /// There is also a global shutdown signal in the ActorSystem. These are separate
    /// shutdown mechanisms that can both be used to shut down the actor.
    cancel: CancellationToken,
}

impl<A> Clone for TokioRuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            sender_high: self.sender_high.clone(),
            instrumentation: self.instrumentation.clone(),
            runtime_handle: self.runtime_handle.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

impl<A> TokioRuntimeHandle<A>
where
    A: 'static,
{
    pub fn sender(&self) -> Arc<TokioRuntimeHandle<A>> {
        Arc::new(self.clone())
    }

    pub fn sender_high(&self) -> Arc<TokioRuntimeHandle<A>> {
        Arc::new(self.clone())
    }

    pub fn future_spawner(&self) -> Box<dyn FutureSpawner> {
        Box::new(self.clone())
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl<A> TokioRuntimeHandle<A> {
    pub(super) fn send_message(
        &self,
        message: TokioRuntimeMessage<A>,
    ) -> Result<(), mpsc::error::SendError<TokioRuntimeMessage<A>>> {
        let name = message.name;
        self.sender.send(message).map(|_| {
            // Only increment the queue if the message was successfully sent.
            self.instrumentation.queue().enqueue(name);
        })
    }

    pub(super) fn send_message_high(
        &self,
        message: TokioRuntimeMessage<A>,
    ) -> Result<(), mpsc::error::SendError<TokioRuntimeMessage<A>>> {
        let name = message.name;
        self.sender_high.send(message).map(|_| {
            // Only increment the queue if the message was successfully sent.
            self.instrumentation.queue().enqueue(name);
        })
    }
}

/// See ActorSystem::spawn_tokio_actor.
pub(crate) fn spawn_tokio_actor<A>(
    actor: A,
    actor_name: String,
    system_cancellation_signal: CancellationToken,
) -> TokioRuntimeHandle<A>
where
    A: Actor + Send + 'static,
{
    let runtime_builder = TokioRuntimeBuilder::new(actor_name, system_cancellation_signal);
    let handle = runtime_builder.handle();
    runtime_builder.spawn_tokio_actor(actor);
    handle
}

struct CallStopWhenDropping<A: Actor> {
    actor: A,
}

impl<A: Actor> Drop for CallStopWhenDropping<A> {
    fn drop(&mut self) {
        self.actor.stop_actor();
    }
}

/// A more granular way to build a tokio runtime. It allows spawning futures and getting a handle
/// before the actor is constructed (so that the actor can be constructed with the handle,
/// for sending messages to itself).
pub struct TokioRuntimeBuilder<A: Actor + Send + 'static> {
    handle: TokioRuntimeHandle<A>,
    receiver: Option<mpsc::UnboundedReceiver<TokioRuntimeMessage<A>>>,
    receiver_high: Option<mpsc::UnboundedReceiver<TokioRuntimeMessage<A>>>,
    shared_instrumentation: Arc<InstrumentedThreadWriterSharedPart>,
    system_cancellation_signal: CancellationToken,
    runtime: Option<Runtime>,
}

impl<A: Actor + Send + 'static> TokioRuntimeBuilder<A> {
    pub fn new(actor_name: String, system_cancellation_signal: CancellationToken) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        let (sender, receiver) = mpsc::unbounded_channel::<TokioRuntimeMessage<A>>();
        let (sender_high, receiver_high) = mpsc::unbounded_channel::<TokioRuntimeMessage<A>>();
        let instrumented_queue = InstrumentedQueue::new(&actor_name);
        let shared_instrumentation =
            InstrumentedThreadWriterSharedPart::new(actor_name, instrumented_queue);

        // Start a background task that just initializes the thread-local writer.
        // This ensures the instrumentation is aware of the actor's existence even if
        // it doesn't process any messages for a while.
        runtime.spawn({
            let shared_instrumentation = shared_instrumentation.clone();
            async move {
                shared_instrumentation.with_thread_local_writer(|_writer| {});
            }
        });

        let cancel = CancellationToken::new();
        let handle = TokioRuntimeHandle {
            sender,
            sender_high,
            runtime_handle: runtime.handle().clone(),
            cancel,
            instrumentation: shared_instrumentation.clone(),
        };

        Self {
            handle,
            receiver: Some(receiver),
            receiver_high: Some(receiver_high),
            shared_instrumentation,
            system_cancellation_signal,
            runtime: Some(runtime),
        }
    }

    pub fn handle(&self) -> TokioRuntimeHandle<A> {
        self.handle.clone()
    }

    pub fn spawn_tokio_actor(mut self, mut actor: A) {
        let mut runtime_handle = self.handle.clone();
        let inner_runtime_handle = runtime_handle.runtime_handle.clone();
        let runtime = self.runtime.take().unwrap();
        let mut receiver = self.receiver.take().unwrap();
        let mut receiver_high = self.receiver_high.take().unwrap();
        let shared_instrumentation = self.shared_instrumentation.clone();
        let actor_name = pretty_type_name::<A>();
        inner_runtime_handle.spawn(async move {
            actor.start_actor(&mut runtime_handle);
            // The runtime gets dropped as soon as this loop exits, cancelling all other futures on
            // the same tokio runtime.
            let _runtime = AsyncDroppableRuntime::new(runtime);
            let mut actor = CallStopWhenDropping { actor };
            let mut window_update_timer = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    biased; 

                    _ = self.system_cancellation_signal.cancelled() => {
                        tracing::info!(target: "tokio_runtime", actor_name, "shutting down Tokio runtime due to ActorSystem shutdown");
                        break;
                    }
                    _ = runtime_handle.cancel.cancelled() => {
                        tracing::info!(target: "tokio_runtime", actor_name, "shutting down Tokio runtime due to targeted cancellation");
                        break;
                    }
                    _ = window_update_timer.tick() => {
                        tracing::trace!(target: "tokio_runtime", "advancing instrumentation window");
                        shared_instrumentation.with_thread_local_writer(|writer| writer.advance_window_if_needed());
                    }
                    Some(message) = receiver_high.recv() => {
                        let seq = message.seq;
                        shared_instrumentation.queue().dequeue(message.name);
                        tracing::debug!(target: "tokio_runtime", seq, actor_name, "Executing message");
                        let dequeue_time_ns = shared_instrumentation.current_time().saturating_sub(message.enqueued_time_ns);
                        shared_instrumentation.with_thread_local_writer(|writer| writer.start_event(message.name, dequeue_time_ns));
                        (message.function)(&mut actor.actor, &mut runtime_handle);
                        shared_instrumentation.with_thread_local_writer(|writer| writer.end_event(message.name));
                    }
                    Some(message) = receiver.recv() => {
                        let seq = message.seq;
                        shared_instrumentation.queue().dequeue(message.name);
                        tracing::trace!(target: "tokio_runtime", seq, actor_name, "executing message");
                        let dequeue_time_ns = shared_instrumentation.current_time().saturating_sub(message.enqueued_time_ns);
                        shared_instrumentation.with_thread_local_writer(|writer| writer.start_event(message.name, dequeue_time_ns));
                        (message.function)(&mut actor.actor, &mut runtime_handle);
                        shared_instrumentation.with_thread_local_writer(|writer| writer.end_event(message.name));
                    }
                    // Note: If the sender is closed, that stops being a selectable option.
                    // This is valid: we can spawn a tokio runtime without a handle, just to keep
                    // some futures running.
                }
            }
        });
    }
}

impl<A> Drop for TokioRuntimeBuilder<A>
where
    A: Actor + Send + 'static,
{
    fn drop(&mut self) {
        if self.runtime.is_some() {
            panic!(
                "TokioRuntimeBuilder must be built before dropping. Did you forget to call spawn_tokio_actor?"
            );
        }
    }
}
