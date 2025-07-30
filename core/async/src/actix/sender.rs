use crate::messaging::{AsyncSendError, CanSend, MessageWithCallback};
use futures::FutureExt;
use near_o11y::{WithSpanContext, WithSpanContextExt};

/// An actix Addr implements CanSend for any message type that the actor handles.
impl<M, A> CanSend<M> for actix::Addr<A>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    A: actix::Actor + actix::Handler<M>,
    A::Context: actix::dev::ToEnvelope<A, M>,
{
    fn send(&self, message: M) {
        match self.try_send(message) {
            Ok(_) => {}
            Err(err) => match err {
                actix::dev::SendError::Full(message) => {
                    self.do_send(message);
                }
                actix::dev::SendError::Closed(_) => {
                    near_o11y::tracing::warn!(
                        "Tried to send {} message to closed actor",
                        std::any::type_name::<M>()
                    );
                }
            },
        }
    }
}

pub type ActixResult<T> = <T as actix::Message>::Result;

impl<M, A> CanSend<MessageWithCallback<M, M::Result>> for actix::Addr<A>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    A: actix::Actor + actix::Handler<M>,
    A::Context: actix::dev::ToEnvelope<A, M>,
{
    fn send(&self, message: MessageWithCallback<M, M::Result>) {
        let MessageWithCallback { message, callback: responder } = message;
        let future = self.send(message);

        let transformed_future = async move {
            match future.await {
                Ok(result) => Ok(result),
                Err(actix::MailboxError::Closed) => Err(AsyncSendError::Closed),
                Err(actix::MailboxError::Timeout) => Err(AsyncSendError::Timeout),
            }
        };
        responder(transformed_future.boxed());
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

impl<T: actix::Actor> Clone for AddrWithAutoSpanContext<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
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

impl<M, S> CanSend<MessageWithCallback<M, M::Result>> for AddrWithAutoSpanContext<S>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    S: actix::Actor,
    actix::Addr<S>: CanSend<MessageWithCallback<WithSpanContext<M>, M::Result>>,
{
    fn send(&self, message: MessageWithCallback<M, M::Result>) {
        let MessageWithCallback { message, callback: responder } = message;
        CanSend::send(
            &self.inner,
            MessageWithCallback { message: message.with_span_context(), callback: responder },
        );
    }
}
