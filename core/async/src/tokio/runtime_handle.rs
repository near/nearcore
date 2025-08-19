use std::sync::Arc;

use tokio::sync::mpsc;

use crate::ActorSystem;
use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::messaging::Actor;
use tokio_util::sync::CancellationToken;

/// TokioRuntimeMessage is a type alias for a boxed function that can be sent to the Tokio runtime.
pub(super) struct TokioRuntimeMessage<A> {
    pub(super) description: String,
    pub(super) function: Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send>,
}

/// TokioRuntimeHandle is a handle to a Tokio runtime that can be used to send messages to an actor.
/// It allows for sending messages and spawning futures into the Tokio runtime.
pub struct TokioRuntimeHandle<A> {
    /// The sender is used to send messages to the actor running in the Tokio runtime.
    pub(super) sender: mpsc::UnboundedSender<TokioRuntimeMessage<A>>,
    /// The runtime is the Tokio runtime that runs the actor and processes messages.
    pub(super) runtime: Arc<tokio::runtime::Runtime>,
    /// Cancellation token used to signal shutdown of this specific Tokio runtime.
    /// There is also a global shutdown signal in the ActorSystem. These are separate
    /// shutdown mechanisms that can both be used to shut down the actor.
    cancel: CancellationToken,
}

impl<A> Clone for TokioRuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            runtime: self.runtime.clone(),
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

    pub fn future_spawner(&self) -> Box<dyn FutureSpawner> {
        Box::new(self.clone())
    }

    pub fn stop(&self) {
        self.cancel.cancel();
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
pub fn spawn_tokio_actor<A>(actor_system: ActorSystem, actor: A) -> TokioRuntimeHandle<A>
where
    A: Actor + Send + 'static,
{
    let runtime_builder = TokioRuntimeBuilder::new(actor_system);
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
    receiver: mpsc::UnboundedReceiver<TokioRuntimeMessage<A>>,
    actor_system: ActorSystem,
}

impl<A: Actor + Send + 'static> TokioRuntimeBuilder<A> {
    pub fn new(actor_system: ActorSystem) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        let (sender, receiver) = mpsc::unbounded_channel::<TokioRuntimeMessage<A>>();
        let cancel = CancellationToken::new();

        let handle = TokioRuntimeHandle { sender, runtime: Arc::new(runtime), cancel };

        Self { handle, receiver, actor_system }
    }

    pub fn handle(&self) -> TokioRuntimeHandle<A> {
        self.handle.clone()
    }

    pub fn spawn_tokio_actor(mut self, mut actor: A) {
        let mut runtime_handle = self.handle.clone();
        let runtime = runtime_handle.runtime.clone();
        runtime.spawn(async move {
            actor.start_actor(&mut runtime_handle);
            let mut actor = CallStopWhenDropping { actor };
            loop {
                tokio::select! {
                    _ = self.actor_system.tokio_cancellation_signal.cancelled() => {
                        tracing::info!(target: "tokio_runtime", "Shutting down Tokio runtime due to ActorSystem shutdown");
                        break;
                    }
                    _ = runtime_handle.cancel.cancelled() => {
                        tracing::debug!(target: "tokio_runtime", "Shutting down Tokio runtime due to targeted cancellation");
                        break;
                    }
                    message = self.receiver.recv() => {
                        let Some(message) = message else {
                            tracing::warn!(target: "tokio_runtime", "Exiting event loop");
                            break;
                        };
                        tracing::debug!(target: "tokio_runtime", "Executing message: {}", message.description);
                        (message.function)(&mut actor.actor, &mut runtime_handle);
                    }
                }
            }
        });
    }
}
