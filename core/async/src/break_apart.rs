use crate::messaging::{self, AsyncMessage, AsyncSendError, CanSend, CanSendAsync, Sender};
use futures::future::BoxFuture;

/// Allows a Sender<M> to be used like a Sender<S> as long as S can be converted to M.
pub struct BreakApart<M: 'static> {
    pub(crate) sender: Sender<M>,
}

impl<S, M: From<S> + 'static> CanSend<S> for BreakApart<M> {
    fn send(&self, message: S) {
        self.sender.send(M::from(message))
    }
}

impl<S, R, M> CanSendAsync<S, R> for BreakApart<M>
where
    S: Send + 'static,
    R: Send + 'static,
    M: From<AsyncMessage<S, R>> + 'static,
{
    fn send_async(&self, message: S) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        let sender = self.sender.clone();
        messaging::send_async_via_message_with_callback(message, move |mwc| {
            let async_message: AsyncMessage<S, R> = mwc.into();
            sender.send(M::from(async_message));
        })
    }
}
