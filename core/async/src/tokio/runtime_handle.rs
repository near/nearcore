use std::sync::Arc;

use tokio::sync::mpsc;

use crate::ActorSystem;
use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::messaging::Actor;

/// TokioRuntimeMessage is a type alias for a boxed function that can be sent to the Tokio runtime.
pub(super) struct TokioRuntimeMessage<A> {
    pub(super) seq: u64,
    pub(super) function: Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send>,
}

/// TokioRuntimeHandle is a handle to a Tokio runtime that can be used to send messages to an actor.
/// It allows for sending messages and spawning futures into the Tokio runtime.
pub struct TokioRuntimeHandle<A> {
    /// The sender is used to send messages to the actor running in the Tokio runtime.
    pub(super) sender: mpsc::UnboundedSender<TokioRuntimeMessage<A>>,
    /// The runtime is the Tokio runtime that runs the actor and processes messages.
    pub(super) runtime: Arc<tokio::runtime::Runtime>,
}

impl<A> Clone for TokioRuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone(), runtime: self.runtime.clone() }
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
}

/// Spawns an actor in the Tokio runtime and returns a handle to it.
/// The handle can be used to get the sender and future spawner for the actor.
///
/// ```rust, ignore
///
/// struct MyActor;
///
/// impl Actor for MyActor {}
///
/// impl Handler<MyMessage> for MyActor {
///     fn handle(&mut self, msg: MyMessage) {}
/// }
///
/// // We can use the actor_handle to create senders and future spawners.
/// let actor_handle = spawn_tokio_actor(MyActor);
///
/// let sender: MyAdapter = actor_handle.sender();
/// let future_spawner = actor_handle.future_spawner();
/// ```
///
/// The sender and future spawner can then be passed onto other components that need to send messages
/// to the actor or spawn futures in the runtime of the actor.
pub fn spawn_tokio_actor<A>(actor_system: ActorSystem, mut actor: A) -> TokioRuntimeHandle<A>
where
    A: Actor + Send + 'static,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    let (sender, mut receiver) = mpsc::unbounded_channel::<TokioRuntimeMessage<A>>();

    let runtime_handle = TokioRuntimeHandle { sender, runtime: Arc::new(runtime) };

    // Spawn the actor in the runtime
    let mut runtime_handle_clone = runtime_handle.clone();
    runtime_handle.runtime.spawn(async move {
        actor.start_actor(&mut runtime_handle_clone);
        loop {
            tokio::select! {
                _ = actor_system.tokio_cancellation_signal.cancelled() => {
                    tracing::info!(target: "tokio_runtime", "shutting down Tokio runtime");
                    break;
                }
                message = receiver.recv() => {
                    let Some(message) = message else {
                        tracing::warn!(target: "tokio_runtime", "exiting event loop");
                        break;
                    };
                    let seq = message.seq;
                    tracing::debug!(target: "tokio_runtime", seq, "executing message");
                    (message.function)(&mut actor, &mut runtime_handle_clone);
                }
            }
        }
    });

    runtime_handle
}
