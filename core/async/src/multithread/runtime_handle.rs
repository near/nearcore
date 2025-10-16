use crate::instrumentation::queue::InstrumentedQueue;
use crate::instrumentation::writer::InstrumentedThreadWriterSharedPart;
use crate::messaging::Actor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// MultithreadRuntimeMessage is a type alias for a boxed function that can be sent to the multithread runtime,
/// as well as a description for debugging purposes.
pub(super) struct MultithreadRuntimeMessage<A> {
    pub(super) seq: u64,
    pub(super) enqueued_time_ns: u64,
    pub(super) name: &'static str,
    pub(super) function: Box<dyn FnOnce(&mut A) + Send>,
}

/// Allows sending messages to a multithreaded actor runtime. Implements CanSend and CanSendAsync traits
/// for the messages that the actor can handle.
pub struct MultithreadRuntimeHandle<A> {
    pub(super) sender: crossbeam_channel::Sender<MultithreadRuntimeMessage<A>>,
    /// This is used in the case where the handle controls the lifetime of the runtime,
    /// dropping (all of) which automatically stops the runtime, as an alterative of having
    /// the ActorSystem control it.
    cancellation_signal_holder: Option<crossbeam_channel::Sender<()>>,
    pub(super) instrumentation: Arc<InstrumentedThreadWriterSharedPart>,
}

impl<A> MultithreadRuntimeHandle<A> {
    fn new(
        sender: crossbeam_channel::Sender<MultithreadRuntimeMessage<A>>,
        cancellation_signal_holder: Option<crossbeam_channel::Sender<()>>,
        instrumentation: Arc<InstrumentedThreadWriterSharedPart>,
    ) -> Self {
        Self { sender, cancellation_signal_holder, instrumentation }
    }
}

impl<A> Clone for MultithreadRuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            cancellation_signal_holder: self.cancellation_signal_holder.clone(),
            instrumentation: self.instrumentation.clone(),
        }
    }
}

impl<A> MultithreadRuntimeHandle<A>
where
    A: 'static,
{
    pub fn sender(&self) -> Arc<MultithreadRuntimeHandle<A>> {
        Arc::new(self.clone())
    }
}

impl<A> MultithreadRuntimeHandle<A> {
    pub(super) fn send_message(
        &self,
        message: MultithreadRuntimeMessage<A>,
    ) -> Result<(), crossbeam_channel::SendError<MultithreadRuntimeMessage<A>>> {
        let name = message.name;
        self.sender.send(message).map(|_| {
            // Only increment the queue if the message was successfully sent.
            self.instrumentation.queue().enqueue(name);
        })
    }
}

/// See ActorSystem::spawn_multithread_actor.
///
/// The `cancellation_signal_holder` is an optional sender that can be used to disable
/// system-wide cancellation. If this sender is used, it is just the other side of the
/// `cancellation_signal`.
pub(crate) fn spawn_multithread_actor<A>(
    num_threads: usize,
    make_actor_fn: impl Fn() -> A + Sync + Send + 'static,
    cancellation_signal: crossbeam_channel::Receiver<()>,
    cancellation_signal_holder: Option<crossbeam_channel::Sender<()>>,
) -> MultithreadRuntimeHandle<A>
where
    A: Actor + Send + 'static,
{
    tracing::info!(
        "Starting multithread actor of type {} with {} threads",
        std::any::type_name::<A>(),
        num_threads
    );
    let threads =
        Arc::new(rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap());
    let (sender, receiver) = crossbeam_channel::unbounded::<MultithreadRuntimeMessage<A>>();
    let instrumented_queue = InstrumentedQueue::new(std::any::type_name::<A>());
    let actor_name = std::any::type_name::<A>();
    let shared_instrumentation =
        InstrumentedThreadWriterSharedPart::new(actor_name.to_string(), instrumented_queue.clone());
    let handle =
        MultithreadRuntimeHandle::new(sender, cancellation_signal_holder, shared_instrumentation);
    let threads_clone = threads.clone();
    let thread_index = Arc::new(AtomicUsize::new(0));
    let handle_clone = handle.clone();
    threads.spawn_broadcast(move |_| {
        let _threads = threads_clone.clone();
        let thread_id = thread_index.fetch_add(1, Ordering::Relaxed);
        let mut instrumentation = handle_clone.instrumentation.new_writer_with_global_registration(Some(thread_id));
        let mut actor = make_actor_fn();
        let actor_name = actor.description();
        let window_update_ticker = crossbeam_channel::tick(Duration::from_secs(1));
        loop {
            crossbeam_channel::select! {
                recv(cancellation_signal) -> _ => {
                    tracing::info!(target: "multithread_runtime", actor_name, "cancellation received, exiting loop.");
                    return;
                }
                recv(window_update_ticker) -> _ => {
                    instrumentation.advance_window_if_needed();
                }
                recv(receiver) -> message => {
                    let Ok(message) = message else {
                        tracing::warn!(target: "multithread_runtime", actor_name, "message queue closed, exiting event loop.");
                        return;
                    };
                    instrumented_queue.dequeue(message.name);
                    let seq = message.seq;
                    let dequeue_time_ns = handle_clone.instrumentation.current_time().saturating_sub(message.enqueued_time_ns);
                    instrumentation.start_event(message.name, dequeue_time_ns);
                    tracing::debug!(target: "multithread_runtime", seq, "Executing message");
                    (message.function)(&mut actor);
                    instrumentation.end_event(message.name);
                }
            }
        }
    });
    handle
}
