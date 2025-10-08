use std::sync::Arc;
use std::time::{Duration, Instant};

use near_time::Clock;
use tokio::sync::mpsc;

use crate::futures::{DelayedActionRunner, FutureSpawner, WrappedFuture};
use crate::instrumentation::queue::InstrumentedQueue;
use crate::instrumentation::writer::InstrumentedThreadWriter;
use crate::messaging::Actor;
use crate::tokio::runtime::AsyncDroppableRuntime;
use tokio::runtime::Runtime;
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
    pending_counts: Arc<InstrumentedQueue>,
    /// The runtime_handle used to post futures to the Tokio runtime.
    /// This is a handle, meaning it does not prevent the runtime from shutting down.
    /// Runtime shutdown is governed by cancellation only (either via TokioRuntimeHandle::stop() or
    /// ActorSystem::stop()).
    pub(super) runtime_handle: tokio::runtime::Handle,
    /// Cancellation token used to signal shutdown of this specific Tokio runtime.
    /// There is also a global shutdown signal in the ActorSystem. These are separate
    /// shutdown mechanisms that can both be used to shut down the actor.
    cancel: CancellationToken,
    clock: Clock,
    reference_instant: Instant,
}

impl<A> Clone for TokioRuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            pending_counts: self.pending_counts.clone(),
            runtime_handle: self.runtime_handle.clone(),
            cancel: self.cancel.clone(),
            clock: self.clock.clone(),
            reference_instant: self.reference_instant,
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

    pub fn future_spawner(&self) -> Box<dyn FutureSpawner> {
        Box::new(self.clone())
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }

    pub(super) fn get_time(&self) -> u64 {
        self.clock.now().duration_since(self.reference_instant).as_nanos() as u64
    }
}

impl<A> TokioRuntimeHandle<A> {
    pub(super) fn send_message(
        &self,
        message: TokioRuntimeMessage<A>,
    ) -> Result<(), mpsc::error::SendError<TokioRuntimeMessage<A>>> {
        let name = message.name;
        self.pending_counts.enqueue(name);
        let result = self.sender.send(message);
        if result.is_ok() {
            Ok(())
        } else {
            self.pending_counts.dequeue(name);
            result
        }
    }

    pub fn spawn_future(
        &self,
        future: futures::future::BoxFuture<'static, ()>,
        description: &'static str,
    ) {
        self.pending_counts.enqueue(description);
        let pending_counts = self.pending_counts.clone();
        let on_ready = Box::new(move || {
            pending_counts.dequeue(description);
        });
        let wrapped_future = WrappedFuture::new(future, on_ready);
        self.runtime_handle.spawn(wrapped_future);
    }
}

/// See ActorSystem::spawn_tokio_actor.
pub(crate) fn spawn_tokio_actor<A>(
    actor: A,
    system_cancellation_signal: CancellationToken,
) -> TokioRuntimeHandle<A>
where
    A: Actor + Send + 'static,
{
    let runtime_builder = TokioRuntimeBuilder::new(system_cancellation_signal);
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
    pending_counts: Arc<InstrumentedQueue>,
    system_cancellation_signal: CancellationToken,
    runtime: Option<Runtime>,
}

impl<A: Actor + Send + 'static> TokioRuntimeBuilder<A> {
    pub fn new(system_cancellation_signal: CancellationToken) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        let (sender, receiver) = mpsc::unbounded_channel::<TokioRuntimeMessage<A>>();
        let pending_counts = InstrumentedQueue::register_new(std::any::type_name::<A>());
        let cancel = CancellationToken::new();

        let handle = TokioRuntimeHandle {
            sender,
            pending_counts: pending_counts.clone(),
            runtime_handle: runtime.handle().clone(),
            cancel,
            clock: Clock::real(),
            reference_instant: Instant::now(),
        };

        Self {
            handle,
            receiver: Some(receiver),
            pending_counts,
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
        let pending_counts = self.pending_counts.clone();
        inner_runtime_handle.spawn(async move {
            actor.start_actor(&mut runtime_handle);
            // The runtime gets dropped as soon as this loop exits, cancelling all other futures on
            // the same tokio runtime.
            let _runtime = AsyncDroppableRuntime::new(runtime);
            let actor_name = std::any::type_name::<A>();
            let mut actor = CallStopWhenDropping { actor };
            let mut instrumentation = InstrumentedThreadWriter::new_from_global(
                actor_name.to_string(),
                actor_name.to_string(),
            );
            let mut window_update_timer = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = self.system_cancellation_signal.cancelled() => {
                        tracing::info!(target: "tokio_runtime", "Shutting down Tokio runtime due to ActorSystem shutdown");
                        break;
                    }
                    _ = runtime_handle.cancel.cancelled() => {
                        tracing::debug!(target: "tokio_runtime", "Shutting down Tokio runtime due to targeted cancellation");
                        break;
                    }
                    _ = window_update_timer.tick() => {
                        instrumentation.advance_window_if_needed();
                    }
                    Some(message) = receiver.recv() => {
                        let seq = message.seq;
                        pending_counts.dequeue(message.name);
                        tracing::debug!(target: "tokio_runtime", seq, "Executing message");
                        let dequeue_time_ns = runtime_handle.get_time().saturating_sub(message.enqueued_time_ns);
                        instrumentation.start_event(message.name, dequeue_time_ns);
                        (message.function)(&mut actor.actor, &mut runtime_handle);
                        instrumentation.end_event(message.name);
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
