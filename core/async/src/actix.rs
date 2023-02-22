use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use near_o11y::{WithSpanContext, WithSpanContextExt};

use crate::messaging::{CanSend, CanSendAsync};

/// An actix Addr implements CanSend for any message type that the actor handles.
impl<M, A> CanSend<M> for actix::Addr<A>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    A: actix::Actor + actix::Handler<M>,
    A::Context: actix::dev::ToEnvelope<A, M>,
{
    fn send(&self, message: M) {
        actix::Addr::do_send(self, message)
    }
}

/// An actix Addr implements CanSendAsync for any message type that the actor handles.
/// Here, the future output of send_async is a Result, because Actix may return an
/// error. The error is converted to (), so that the caller does not need to be aware of
/// Actix-specific error messages.
impl<M, A> CanSendAsync<M, Result<M::Result, ()>> for actix::Addr<A>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    A: actix::Actor + actix::Handler<M>,
    A::Context: actix::dev::ToEnvelope<A, M>,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<M::Result, ()>> {
        self.send(message).map_err(|_| ()).boxed()
    }
}

/// Allows an actix Addr<WithSpanContext<T>> to act as if it were an Addr<T>,
/// by automatically wrapping any message sent with .with_span_context().
///
/// This way, the sender side does not need to be concerned about attaching span contexts, e.g.
///
///   impl SomeOtherComponent {
///     pub fn new(sender: Sender<Message>) -> Self {...}
///   }
///
///   impl actix::Handler<WithSpanContext<Message>> for SomeActor {...}
///
///   let addr = SomeActor::spawn(...);
///   let other = SomeOtherComponent::new(
///       addr.with_auto_span_context().into_sender()  // or .clone() on the addr if needed
///   );
pub struct AddrWithAutoSpanContext<T: actix::Actor> {
    inner: actix::Addr<T>,
}

/// Extension function to convert an Addr<WithSpanContext<T>> to an AddrWithAutoSpanContext<T>.
pub trait AddrWithAutoSpanContextExt<T: actix::Actor> {
    fn with_auto_span_context(self) -> AddrWithAutoSpanContext<T>;
}

impl<T: actix::Actor> AddrWithAutoSpanContextExt<T> for actix::Addr<T> {
    fn with_auto_span_context(self) -> AddrWithAutoSpanContext<T> {
        AddrWithAutoSpanContext { inner: self }
    }
}

impl<M, S> CanSend<M> for AddrWithAutoSpanContext<S>
where
    M: actix::Message + 'static,
    S: actix::Actor,
    actix::Addr<S>: CanSend<WithSpanContext<M>>,
{
    fn send(&self, message: M) {
        CanSend::send(&self.inner, message.with_span_context());
    }
}

impl<M, S> CanSendAsync<M, Result<M::Result, ()>> for AddrWithAutoSpanContext<S>
where
    M: actix::Message + 'static,
    M::Result: Send,
    S: actix::Actor,
    actix::Addr<S>: CanSendAsync<WithSpanContext<M>, Result<M::Result, ()>>,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<M::Result, ()>> {
        self.inner.send_async(message.with_span_context())
    }
}
