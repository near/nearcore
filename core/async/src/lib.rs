pub use near_async_derive::{MultiSend, MultiSendMessage, MultiSenderFrom};

pub mod actix;
pub mod break_apart;
mod functional;
pub mod futures;
pub mod messaging;
pub mod test_loop;
pub mod tokio;

// FIXME: near_time re-export is not optimal solution, but it would require to change time in many places
use crate::tokio::TokioRuntimeHandle;
use crate::tokio::runtime_handle::spawn_tokio_actor;
pub use near_time as time;
use tokio_util::sync::CancellationToken;

/// Represents a collection of actors, so that they can be shutdown together.
#[derive(Clone)]
pub struct ActorSystem {
    /// Cancellation token used to signal shutdown of Tokio runtimes spawned with this actor system.
    tokio_cancellation_signal: CancellationToken,
}

impl ActorSystem {
    pub fn new() -> Self {
        let mut systems = ACTOR_SYSTEMS.lock();
        let ret = Self { tokio_cancellation_signal: CancellationToken::new() };
        systems.push(ret.clone());
        ret
    }

    pub fn stop(&self) {
        self.tokio_cancellation_signal.cancel();
    }

    pub fn spawn_tokio_actor<A: messaging::Actor + Send + 'static>(
        &self,
        actor: A,
    ) -> TokioRuntimeHandle<A> {
        spawn_tokio_actor(self.clone(), actor)
    }
}

/// Used to determine whether shutdown_all_actors is being used properly. If there are multiple
/// ActorSystems, shutdown_all_actors shall not be used, but instead the test needs to manage
/// the shutdown of each ActorSystem individually.
static ACTOR_SYSTEMS: parking_lot::Mutex<Vec<ActorSystem>> = parking_lot::Mutex::new(Vec::new());

/// Shutdown all actors in all supported actor runtimes, assuming there is only up to one actix::System
/// and only up to one ActorSystem.
/// TODO(#14005): Ideally, shutting down actors should not be done by calling a global function.
pub fn shutdown_all_actors() {
    ::actix::System::current().stop();
    {
        let systems = ACTOR_SYSTEMS.lock();
        if systems.len() > 1 {
            panic!("shutdown_all_actors should not be used when there are multiple ActorSystems");
        }
        if let Some(system) = systems.first() {
            system.tokio_cancellation_signal.cancel();
        }
    }
}
