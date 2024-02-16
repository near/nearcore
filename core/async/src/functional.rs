use crate::messaging::{CanSend, MessageWithCallback};
use std::marker::PhantomData;

/// Allows a Sender to be created from a raw function.
pub struct SendFunction<M: 'static, F: Fn(M) + Send + Sync + 'static> {
    send: F,
    _phantom: PhantomData<fn(M)>,
}

impl<M: 'static, F: Fn(M) + Send + Sync + 'static> SendFunction<M, F> {
    pub fn new(send: F) -> Self {
        Self { send, _phantom: PhantomData }
    }
}

impl<M: 'static, F: Fn(M) + Send + Sync + 'static> CanSend<M> for SendFunction<M, F> {
    fn send(&self, message: M) {
        (self.send)(message)
    }
}

/// Allows an AsyncSender to be created from a raw (synchronous) function.
pub struct SendAsyncFunction<M: 'static, R: 'static, F: Fn(M) -> R + Send + Sync + 'static> {
    f: F,
    _phantom: PhantomData<fn(M, R)>,
}

impl<M: 'static, R: 'static, F: Fn(M) -> R + Send + Sync + 'static> SendAsyncFunction<M, R, F> {
    pub fn new(f: F) -> Self {
        Self { f, _phantom: PhantomData }
    }
}

impl<M: 'static, R: 'static, F: Fn(M) -> R + Send + Sync + 'static>
    CanSend<MessageWithCallback<M, R>> for SendAsyncFunction<M, R, F>
{
    fn send(&self, message: MessageWithCallback<M, R>) {
        let MessageWithCallback { message, callback: responder } = message;
        responder(Ok((self.f)(message)));
    }
}
