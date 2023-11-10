use crate::messaging::{CanSend, MessageExpectingResponse};
use near_o11y::{WithSpanContext, WithSpanContextExt};
use std::fmt::Display;

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

/// Generic failure for async send. We don't use MailboxError because that is Actix-specific.
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum AsyncSendError {
    #[default]
    Closed,
    Timeout,
}

impl Display for AsyncSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsyncSendError::Closed => write!(f, "Closed"),
            AsyncSendError::Timeout => write!(f, "Timeout"),
        }
    }
}

pub type ActixResult<T> = <T as actix::Message>::Result;

impl<M, A> CanSend<MessageExpectingResponse<M, Result<M::Result, AsyncSendError>>>
    for actix::Addr<A>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    A: actix::Actor + actix::Handler<M>,
    A::Context: actix::dev::ToEnvelope<A, M>,
{
    fn send(&self, message: MessageExpectingResponse<M, Result<M::Result, AsyncSendError>>) {
        let MessageExpectingResponse { message, responder } = message;
        let future = self.send(message);
        actix::spawn(async move {
            responder(future.await.map_err(|err| match err {
                actix::MailboxError::Closed => AsyncSendError::Closed,
                actix::MailboxError::Timeout => AsyncSendError::Timeout,
            }));
        });
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

impl<M, S> CanSend<MessageExpectingResponse<M, Result<M::Result, AsyncSendError>>>
    for AddrWithAutoSpanContext<S>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    S: actix::Actor,
    actix::Addr<S>:
        CanSend<MessageExpectingResponse<WithSpanContext<M>, Result<M::Result, AsyncSendError>>>,
{
    fn send(&self, message: MessageExpectingResponse<M, Result<M::Result, AsyncSendError>>) {
        let MessageExpectingResponse { message, responder } = message;
        CanSend::send(
            &self.inner,
            MessageExpectingResponse { message: message.with_span_context(), responder },
        );
    }
}
