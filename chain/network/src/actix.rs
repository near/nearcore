use near_async::messaging;
use near_async::tokio::TokioRuntimeHandle;
use std::ops::Deref;

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
