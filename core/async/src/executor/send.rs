use crate::executor::ExecutorHandle;
use crate::executor::envelope::Envelope;
use crate::messaging::{self, AsyncSendError, CanSend, MessageWithCallback};
use futures::FutureExt;

impl<M, T> CanSend<M> for ExecutorHandle<T>
where
    T: messaging::Handler<M> + messaging::Actor + 'static,
    M: actix::Message + Send + 'static,
    M::Result: Send,
{
    fn send(&self, message: M) {
        let envelope = Envelope::from_sync_message(message);
        self.sender.send(envelope).ok();
    }
}

impl<T, M> CanSend<MessageWithCallback<M, M::Result>> for ExecutorHandle<T>
where
    T: messaging::HandlerWithContext<M> + messaging::Actor + 'static,
    M: actix::Message + Send + 'static,
    M::Result: Send,
{
    fn send(&self, message: MessageWithCallback<M, M::Result>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let MessageWithCallback { message, callback } = message;
        let envelope = Envelope::from_async_message(message, sender);
        match self.sender.send(envelope) {
            Ok(_) => {
                let fut = async move { receiver.await.map_err(|_| AsyncSendError::Dropped) };
                callback(fut.boxed());
            }
            Err(_) => {
                callback(async move { Err(AsyncSendError::Closed) }.boxed());
            }
        }
    }
}
