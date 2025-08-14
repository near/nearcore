pub use near_async_derive::{MultiSend, MultiSendMessage, MultiSenderFrom};

pub mod actix;
pub mod break_apart;
mod functional;
pub mod futures;
pub mod messaging;
pub mod test_loop;
pub mod tokio;

use crate::tokio::runtime_handle::GLOBAL_TOKIO_RUNTIME_SHUTDOWN_SIGNAL;

// FIXME: near_time re-export is not optimal solution, but it would require to change time in many places
pub use near_time as time;

/// Shutdown all actors in all supported actor runtimes.
/// TODO(#14005): Ideally, shutting down actors should not be done by calling a global function.
pub fn shutdown_all_actors() {
    ::actix::System::current().stop();
    GLOBAL_TOKIO_RUNTIME_SHUTDOWN_SIGNAL.cancel();
}
