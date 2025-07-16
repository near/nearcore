use crate::futures::DelayedActionRunner;
use crate::messaging;

pub trait HandledBy<T> {
    fn handle(&mut self, handler: &mut T, delayed_action_runner: &mut dyn DelayedActionRunner<T>);
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
    T: messaging::Actor + messaging::HandlerWithContext<M> + 'static,
{
    fn handle(&mut self, handler: &mut T, delayed_action_runner: &mut dyn DelayedActionRunner<T>) {
        let SyncMessage { message } = self;
        handler.wrap_handler(
            message.take().unwrap(),
            delayed_action_runner,
            |handler, message, delayed_action_runner| {
                handler.handle(message, delayed_action_runner)
            },
        );
    }
    fn describe(&self) -> String {
        format!("SyncMessage({})", std::any::type_name::<M>())
    }
}

impl<T, M> HandledBy<T> for AsyncMessage<M, M::Result>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    T: messaging::Actor + messaging::HandlerWithContext<M> + 'static,
{
    fn handle(&mut self, handler: &mut T, delayed_action_runner: &mut dyn DelayedActionRunner<T>) {
        let AsyncMessage { message, result_sender } = self;
        let result = handler.wrap_handler(
            message.take().unwrap(),
            delayed_action_runner,
            |handler, message, delayed_action_runner| {
                handler.handle(message, delayed_action_runner)
            },
        );
        result_sender.take().unwrap().send(result).ok();
    }
    fn describe(&self) -> String {
        format!("AsyncMessage({})", std::any::type_name::<M>())
    }
}

struct FnHandler<F> {
    name: String,
    f: Option<F>,
}
impl<T, F: FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static> HandledBy<T>
    for FnHandler<F>
{
    fn handle(&mut self, handler: &mut T, delayed_action_runner: &mut dyn DelayedActionRunner<T>) {
        self.f.take().unwrap()(handler, delayed_action_runner);
    }
    fn describe(&self) -> String {
        format!("FnHandler({})", self.name)
    }
}

pub struct Envelope<T> {
    inner: Box<dyn HandledBy<T> + Send>,
}

impl<T> Envelope<T> {
    pub fn new(inner: impl HandledBy<T> + Send + 'static) -> Self {
        Self { inner: Box::new(inner) }
    }

    pub fn from_fn(
        name: String,
        f: impl FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static,
    ) -> Self {
        Self { inner: Box::new(FnHandler { name, f: Some(f) }) }
    }

    pub fn from_sync_message<M>(message: M) -> Self
    where
        T: messaging::Actor + messaging::HandlerWithContext<M> + 'static,
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
        T: messaging::Actor + messaging::HandlerWithContext<M> + 'static,
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

    pub fn handle_by(
        mut self,
        handler: &mut T,
        delayed_action_runner: &mut dyn DelayedActionRunner<T>,
    ) {
        self.inner.handle(handler, delayed_action_runner);
    }

    pub fn describe(&self) -> String {
        self.inner.describe()
    }
}
