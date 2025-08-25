use crate::ActorSystem;
use crate::messaging::Actor;
use std::sync::Arc;

/// MultithreadRuntimeMessage is a type alias for a boxed function that can be sent to the multithread runtime,
/// as well as a description for debugging purposes.
pub(super) struct MultithreadRuntimeMessage<A> {
    pub(super) description: String,
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

pub fn spawn_multithread_actor<A>(
    actor_system: ActorSystem,
    num_threads: usize,
    make_actor_fn: impl Fn() -> A + Sync + Send + 'static,
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
    let cancellation_receiver = actor_system.multithread_cancellation_receiver;
    let handle = MultithreadRuntimeHandle { sender };
    let threads_clone = threads.clone();
    threads.spawn_broadcast(move |_| {
        let _threads = threads_clone.clone();
        let mut actor = make_actor_fn();
        loop {
            crossbeam_channel::select! {
                recv(cancellation_receiver) -> _ => {
                    tracing::info!(target: "multithread_runtime", "Cancellation received, exiting loop.");
                    return;
                }
                recv(receiver) -> message => {
                    let Ok(message) = message else {
                        tracing::warn!(target: "multithread_runtime", "Message queue closed, exiting event loop.");
                        return;
                    };
                    tracing::debug!(target: "multithread_runtime", "Executing message: {}", message.description);
                    (message.function)(&mut actor);
                }
            }
        }
    });
    handle
}
