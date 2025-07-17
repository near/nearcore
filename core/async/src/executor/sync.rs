use crate::messaging::{self, AsyncSendError, CanSend, MessageWithCallback};
use futures::FutureExt;
use std::sync::Arc;

pub struct SyncExecutorRuntime {
    _threads: Arc<rayon::ThreadPool>,
}

pub trait HandledBy<T> {
    fn handle(&mut self, handler: &mut T);
    fn describe(&self) -> String;
}

pub struct SyncMessage<M> {
    message: Option<M>,
}
pub struct AsyncMessage<M, R> {
    message: Option<M>,
    result_sender: Option<tokio::sync::oneshot::Sender<R>>,
}

impl<T, M> HandledBy<T> for SyncMessage<M>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    T: messaging::Actor + messaging::Handler<M> + 'static,
{
    fn handle(&mut self, handler: &mut T) {
        let SyncMessage { message } = self;
        handler.handle(message.take().unwrap());
    }
    fn describe(&self) -> String {
        format!("SyncMessage({})", std::any::type_name::<M>())
    }
}

impl<T, M> HandledBy<T> for AsyncMessage<M, M::Result>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    T: messaging::Actor + messaging::Handler<M> + 'static,
{
    fn handle(&mut self, handler: &mut T) {
        let AsyncMessage { message, result_sender } = self;
        let result = handler.handle(message.take().unwrap());
        result_sender.take().unwrap().send(result).ok();
    }
    fn describe(&self) -> String {
        format!("AsyncMessage({})", std::any::type_name::<M>())
    }
}

pub struct Envelope<T> {
    inner: Box<dyn HandledBy<T> + Send>,
}

impl<T> Envelope<T> {
    pub fn new(inner: impl HandledBy<T> + Send + 'static) -> Self {
        Self { inner: Box::new(inner) }
    }

    pub fn from_sync_message<M>(message: M) -> Self
    where
        T: messaging::Actor + messaging::Handler<M> + 'static,
        M: actix::Message + Send + 'static,
        M::Result: Send,
    {
        Self { inner: Box::new(SyncMessage { message: Some(message) }) }
    }

    pub fn from_async_message<M>(
        message: M,
        result_sender: tokio::sync::oneshot::Sender<M::Result>,
    ) -> Self
    where
        T: messaging::Actor + messaging::Handler<M> + 'static,
        M: actix::Message + Send + 'static,
        M::Result: Send,
    {
        Self {
            inner: Box::new(AsyncMessage {
                message: Some(message),
                result_sender: Some(result_sender),
            }),
        }
    }

    pub fn handle_by(mut self, handler: &mut T) {
        self.inner.handle(handler);
    }

    pub fn describe(&self) -> String {
        self.inner.describe()
    }
}

pub struct SyncExecutorHandle<T> {
    sender: crossbeam::channel::Sender<Envelope<T>>,
}

impl<T> Clone for SyncExecutorHandle<T> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

pub fn start_sync_actors<T: messaging::Actor + Send + 'static>(
    num_threads: usize,
    make_actor_fn: impl Fn() -> T + Sync + Send + 'static,
) -> (SyncExecutorRuntime, SyncExecutorHandle<T>) {
    tracing::info!(
        "Starting sync actor of type {} with {} threads",
        std::any::type_name::<T>(),
        num_threads
    );
    let threads =
        Arc::new(rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap());
    let (sender, receiver) = crossbeam::channel::unbounded::<Envelope<T>>();
    let runtime = SyncExecutorRuntime { _threads: threads.clone() };
    let threads_clone = threads.clone();
    threads.spawn_broadcast(move |_| {
        let _threads = threads_clone.clone();
        let mut actor = make_actor_fn();
        while let Ok(envelope) = receiver.recv() {
            tracing::debug!("Handling message: {}", envelope.describe());
            envelope.handle_by(&mut actor);
        }
    });
    (runtime, SyncExecutorHandle { sender })
}

impl<M, T> CanSend<M> for SyncExecutorHandle<T>
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

impl<T, M> CanSend<MessageWithCallback<M, M::Result>> for SyncExecutorHandle<T>
where
    T: messaging::Handler<M> + messaging::Actor + 'static,
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
            Err(e) => {
                tracing::warn!("Failed to send message {}", e.0.describe());
                callback(async move { Err(AsyncSendError::Closed) }.boxed());
            }
        }
    }
}
