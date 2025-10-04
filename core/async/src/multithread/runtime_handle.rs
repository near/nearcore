use near_time::Clock;

use crate::instrumentation::writer::InstrumentedThreadWriter;
use crate::messaging::Actor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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
    clock: Clock,
    reference_instant: Instant,
}

impl<A> From<crossbeam_channel::Sender<MultithreadRuntimeMessage<A>>>
    for MultithreadRuntimeHandle<A>
{
    fn from(sender: crossbeam_channel::Sender<MultithreadRuntimeMessage<A>>) -> Self {
        Self { sender, clock: Clock::real(), reference_instant: Instant::now() }
    }
}

impl<A> MultithreadRuntimeHandle<A> {
    pub(super) fn get_time(&self) -> u64 {
        self.clock.now().duration_since(self.reference_instant).as_nanos() as u64
    }
}

impl<A> Clone for MultithreadRuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            clock: self.clock.clone(),
            reference_instant: self.reference_instant,
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

/// See ActorSystem::spawn_multithread_actor.
pub(crate) fn spawn_multithread_actor<A>(
    num_threads: usize,
    make_actor_fn: impl Fn() -> A + Sync + Send + 'static,
    system_cancellation_signal: crossbeam_channel::Receiver<()>,
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
    let handle = MultithreadRuntimeHandle::from(sender);
    let threads_clone = threads.clone();
    let thread_index = Arc::new(AtomicUsize::new(0));
    let handle_clone = handle.clone();
    threads.spawn_broadcast(move |_| {
        let _threads = threads_clone.clone();
        let thread_id = thread_index.fetch_add(1, Ordering::Relaxed);
        let mut actor = make_actor_fn();
        let mut instrumentation = InstrumentedThreadWriter::new_from_global
            (format!("{}-{}", std::any::type_name::<A>(), thread_id));
        let window_update_ticker = crossbeam_channel::tick(Duration::from_secs(1));
        loop {
            crossbeam_channel::select! {
                recv(system_cancellation_signal) -> _ => {
                    tracing::info!(target: "multithread_runtime", "cancellation received, exiting loop.");
                    return;
                }
                recv(window_update_ticker) -> _ => {
                    instrumentation.advance_window_if_needed();
                }
                recv(receiver) -> message => {
                    let Ok(message) = message else {
                        tracing::warn!(target: "multithread_runtime", "message queue closed, exiting event loop.");
                        return;
                    };
                    let seq = message.seq;
                    let dequeue_time_ns = handle_clone.get_time().saturating_sub(message.enqueued_time_ns);
                    instrumentation.start_event(message.name, dequeue_time_ns);
                    tracing::debug!(target: "multithread_runtime", seq, "Executing message");
                    (message.function)(&mut actor);
                    instrumentation.end_event();
                }
            }
        }
    });
    handle
}
