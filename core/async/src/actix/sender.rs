use crate::messaging::{AsyncSendError, CanSend, MessageWithCallback};
use futures::FutureExt;

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
