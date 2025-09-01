mod runtime;
pub(crate) mod runtime_handle;
mod sender;
#[cfg(test)]
mod test;

use crate::messaging::Actor;
pub use runtime_handle::TokioRuntimeHandle;

/// Actor that doesn't handle any messages and does nothing. It's used to host a runtime that can
/// run futures only.
pub struct EmptyActor;
impl Actor for EmptyActor {}
