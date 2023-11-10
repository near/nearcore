use crate::messaging::CanSend;
use std::marker::PhantomData;

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
