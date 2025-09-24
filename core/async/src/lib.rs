pub use near_async_derive::{MultiSend, MultiSendMessage, MultiSenderFrom};

pub mod actix;
pub mod break_apart;
mod functional;
pub mod futures;
pub mod messaging;
pub mod multithread;
pub mod test_loop;
pub mod tokio;

use crate::futures::FutureSpawner;
use crate::messaging::Actor;
use crate::multithread::runtime_handle::{MultithreadRuntimeHandle, spawn_multithread_actor};
use crate::tokio::TokioRuntimeHandle;
use crate::tokio::runtime_handle::{TokioRuntimeBuilder, spawn_tokio_actor};
pub use near_time as time;
use parking_lot::Mutex;
use std::any::type_name;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio_util::sync::CancellationToken;

/// Sequence number to be shared for all messages, to distinguish messages when logging.
static MESSAGE_SEQUENCE_NUM: AtomicU64 = AtomicU64::new(0);

pub(crate) fn next_message_sequence_num() -> u64 {
    MESSAGE_SEQUENCE_NUM.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

// Quick and dirty way of getting the type name without the module path.
// Does not work for more complex types like std::sync::Arc<std::sync::atomic::AtomicBool<...>>
// example near_chunks::shards_manager_actor::ShardsManagerActor -> ShardsManagerActor
fn pretty_type_name<T>() -> &'static str {
    type_name::<T>().split("::").last().unwrap()
}

/// Actor that doesn't handle any messages and does nothing. It's used to host a runtime that can
/// run futures only.
struct EmptyActor;
impl Actor for EmptyActor {}

/// Represents a collection of actors, so that they can be shutdown together.
#[derive(Clone)]
pub struct ActorSystem {
    /// Cancellation token used to signal shutdown of Tokio runtimes spawned with this actor system.
    tokio_cancellation_signal: CancellationToken,
    /// Cancellation signal used to signal shutdown of multithread actors spawned with this actor
    /// system. To send the cancellation signal, the sender is dropped, which causes the receivers
    /// to error.
    multithread_cancellation_signal: Arc<Mutex<Option<crossbeam_channel::Sender<()>>>>,
    multithread_cancellation_receiver: crossbeam_channel::Receiver<()>,
}

impl ActorSystem {
    pub fn new() -> Self {
        let mut systems = ACTOR_SYSTEMS.lock();
        let (multithread_cancellation_sender, multithread_cancellation_receiver) =
            crossbeam_channel::bounded(0);
        let ret = Self {
            tokio_cancellation_signal: CancellationToken::new(),
            multithread_cancellation_signal: Arc::new(Mutex::new(Some(
                multithread_cancellation_sender,
            ))),
            multithread_cancellation_receiver,
        };
        systems.push(ret.clone());
        ret
    }

    pub fn stop(&self) {
        tracing::info!("Stopping all actors in ActorSystem");
        self.tokio_cancellation_signal.cancel();
        self.multithread_cancellation_signal.lock().take();
    }

    /// Spawns an actor in a single threaded Tokio runtime and returns a handle to it.
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
    /// let actor_handle = actor_system.spawn_tokio_actor(MyActor);
    ///
    /// let sender: MyAdapter = actor_handle.sender();
    /// let future_spawner = actor_handle.future_spawner();
    /// ```
    ///
    /// The sender and future spawner can then be passed onto other components that need to send messages
    /// to the actor or spawn futures in the runtime of the actor.
    pub fn spawn_tokio_actor<A: messaging::Actor + Send + 'static>(
        &self,
        actor: A,
    ) -> TokioRuntimeHandle<A> {
        spawn_tokio_actor(actor, self.tokio_cancellation_signal.clone())
    }

    /// A more granular way to build a tokio runtime. It allows spawning futures and getting a handle
    /// before the actor is constructed (so that the actor can be constructed with the handle,
    /// for sending messages to itself).
    pub fn new_tokio_builder<A: messaging::Actor + Send + 'static>(
        &self,
    ) -> TokioRuntimeBuilder<A> {
        TokioRuntimeBuilder::new(self.tokio_cancellation_signal.clone())
    }

    /// Spawns a multi-threaded actor which handles messages in a synchronous thread pool.
    /// Used similarly to `spawn_tokio_actor`, but this actor is intended for CPU-bound tasks,
    /// can run multiple threads, and does not support futures, timers, or delayed messages.
    pub fn spawn_multithread_actor<A: messaging::Actor + Send + 'static>(
        &self,
        num_threads: usize,
        make_actor_fn: impl Fn() -> A + Sync + Send + 'static,
    ) -> MultithreadRuntimeHandle<A> {
        spawn_multithread_actor(
            num_threads,
            make_actor_fn,
            self.multithread_cancellation_receiver.clone(),
        )
    }

    /// Returns a future spawner for the actor system on an independent Tokio runtime.
    /// Note: For typical actors, it is recommended we use the future spawner of the
    /// actor instead.
    ///
    /// This is useful for keeping track of spawned futures and their lifetimes.
    /// Behind the scenes, this builds a new EmptyActor each time.
    pub fn new_future_spawner(&self) -> Box<dyn FutureSpawner> {
        let handle = self.spawn_tokio_actor(EmptyActor);
        handle.future_spawner()
    }
}

/// Used to determine whether shutdown_all_actors is being used properly. If there are multiple
/// ActorSystems, shutdown_all_actors shall not be used, but instead the test needs to manage
/// the shutdown of each ActorSystem individually.
static ACTOR_SYSTEMS: Mutex<Vec<ActorSystem>> = Mutex::new(Vec::new());

/// Shutdown all actors, assuming at most one ActorSystem.
/// TODO(#14005): Ideally, shutting down actors should not be done by calling a global function.
pub fn shutdown_all_actors() {
    {
        let systems = ACTOR_SYSTEMS.lock();
        if systems.len() > 1 {
            panic!("shutdown_all_actors should not be used when there are multiple ActorSystems");
        }
        if let Some(system) = systems.first() {
            system.stop();
        }
    }
}
