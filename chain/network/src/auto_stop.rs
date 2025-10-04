use near_async::messaging;
use near_async::tokio::TokioRuntimeHandle;
use std::ops::Deref;

/// Just a simple structure that when dropped, stops the actor. It's for testing only.
pub struct AutoStopActor<A: messaging::Actor + 'static>(pub TokioRuntimeHandle<A>);

impl<A: messaging::Actor> Deref for AutoStopActor<A> {
    type Target = TokioRuntimeHandle<A>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: messaging::Actor + 'static> Drop for AutoStopActor<A> {
    fn drop(&mut self) {
        self.0.stop();
    }
}
