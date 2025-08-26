use crate::messaging::Actor;
use std::sync::Arc;

/// MultithreadRuntimeMessage is a type alias for a boxed function that can be sent to the multithread runtime,
/// as well as a description for debugging purposes.
pub(super) struct MultithreadRuntimeMessage<A> {
    pub(super) seq: u64,
    pub(super) function: Box<dyn FnOnce(&mut A) + Send>,
}

/// Allows sending messages to a multithreaded actor runtime. Implements CanSend and CanSendAsync traits
/// for the messages that the actor can handle.
pub struct MultithreadRuntimeHandle<A> {
    pub(super) sender: crossbeam_channel::Sender<MultithreadRuntimeMessage<A>>,
}

impl<A> Clone for MultithreadRuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
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
    let handle = MultithreadRuntimeHandle { sender };
    let threads_clone = threads.clone();
    threads.spawn_broadcast(move |_| {
        let _threads = threads_clone.clone();
        let mut actor = make_actor_fn();
        loop {
            crossbeam_channel::select! {
                recv(system_cancellation_signal) -> _ => {
                    tracing::info!(target: "multithread_runtime", "cancellation received, exiting loop.");
                    return;
                }
                recv(receiver) -> message => {
                    let Ok(message) = message else {
                        tracing::warn!(target: "multithread_runtime", "message queue closed, exiting event loop.");
                        return;
                    };
                    let seq = message.seq;
                    tracing::debug!(target: "multithread_runtime", seq, "Executing message");
                    (message.function)(&mut actor);
                }
            }
        }
    });
    handle
}
