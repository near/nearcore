use crate::break_apart::BreakApart;
use crate::functional::{SendAsyncFunction, SendFunction};
use crate::futures::DelayedActionRunner;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::fmt::{Debug, Display};
use std::sync::{Arc, OnceLock};
use tokio::sync::oneshot;

/// Trait for an actor. An actor is a struct that can handle messages and implements the Handler or
/// HandlerWithContext trait.
///
/// IMPORTANT NOTE: For the Multithread actor runtime, NONE of the below methods are ever called.
/// These are only applicable for the async tokio runtime.
pub trait Actor {
    /// This is automatically called by the actor runtime when the actor is started.
    /// It is called before any messages are processed.
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {}

    /// This is called for every message that the actor handles, so that the actor can do something
    /// else before and after handling the message.
    fn wrap_handler<M, R>(
        &mut self,
        msg: M,
        ctx: &mut dyn DelayedActionRunner<Self>,
        f: impl FnOnce(&mut Self, M, &mut dyn DelayedActionRunner<Self>) -> R,
    ) -> R {
        f(self, msg, ctx)
    }

    /// Called by the actor runtime right before the actor is dropped.
    fn stop_actor(&mut self) {}
}

/// All handled messages shall implement this trait.
/// TODO(#14005): The reason this exists is to allow CanSend<M> to be
/// implemented at the same time as CanSend<MessageWithCallback<M, R>>. Once we
/// remove MessageWithCallback, we should not need this anymore.
pub trait Message {}

/// Trait for handling a message.
/// This works in unison with the [`CanSend`] trait. An actor implements the Handler trait for all
/// messages it would like to handle, while the CanSend trait implements the logic to send the
/// message to the actor. Handle and CanSend are typically not both implemented by the same struct.
pub trait Handler<M, R = ()>
where
    M: Message,
    R: Send,
{
    fn handle(&mut self, msg: M) -> R;
}

/// Trait for handling a message with context.
/// This is similar to the [`Handler`] trait, but it allows the handler to access the delayed action
/// runner that is used to schedule actions to be run in the future. For tokio actors, the
/// TokioRuntimeHandle<A> implements DelayedActionRunner<T>.
/// Note that the implementer for handler of a message only needs to implement either of Handler or
/// HandlerWithContext, not both.
pub trait HandlerWithContext<M, R = ()>
where
    M: Message,
    R: Send,
{
    fn handle(&mut self, msg: M, ctx: &mut dyn DelayedActionRunner<Self>) -> R;
}

impl<A, M, R> HandlerWithContext<M, R> for A
where
    A: Actor + Handler<M, R>,
    M: Message,
    R: Send,
{
    fn handle(&mut self, msg: M, ctx: &mut dyn DelayedActionRunner<Self>) -> R {
        self.wrap_handler(msg, ctx, |this, msg, _| Handler::handle(this, msg))
    }
}

/// Trait for sending a typed message. The sent message is then handled by the Handler trait.
/// See [`Handler<M>`] trait for more details.
pub trait CanSend<M>: Send + Sync + 'static {
    fn send(&self, message: M);
}

/// Trait for sending a typed message async. The sent message is then handled by the Handler trait.
/// See [`Handler<M, R>`] trait for more details.
pub trait CanSendAsync<M, R>: Send + Sync + 'static {
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>>;
}

/// Wraps a CanSend. This should be used to pass around an Arc<dyn CanSend<M>>, instead
/// of spelling out that type. Using a wrapper struct allows us to define more flexible
/// APIs.
pub struct Sender<M: 'static> {
    sender: Arc<dyn CanSend<M>>,
}

impl<M> Clone for Sender<M> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

/// Extension functions to wrap a CanSend as a Sender.
pub trait IntoSender<M> {
    /// This allows conversion of an owned CanSend into a Sender.
    fn into_sender(self) -> Sender<M>;
    /// This allows conversion of a reference-counted CanSend into a Sender.
    fn as_sender(self: &Arc<Self>) -> Sender<M>;
}

impl<M, T: CanSend<M>> IntoSender<M> for T {
    fn into_sender(self) -> Sender<M> {
        Sender::from_impl(self)
    }
    fn as_sender(self: &Arc<Self>) -> Sender<M> {
        Sender::from_arc(self.clone())
    }
}

impl<M> Sender<M> {
    /// Sends a message. It's the responsibility of the underlying CanSend
    /// implementation to decide how to handle the message.
    pub fn send(&self, message: M) {
        self.sender.send(message)
    }

    fn from_impl(sender: impl CanSend<M> + 'static) -> Self {
        Self { sender: Arc::new(sender) }
    }

    fn from_arc<T: CanSend<M> + 'static>(arc: Arc<T>) -> Self {
        Self { sender: arc }
    }

    /// Creates a sender that handles messages using the given function.
    pub fn from_fn(send: impl Fn(M) + Send + Sync + 'static) -> Self {
        Self::from_impl(SendFunction::new(send))
    }

    /// Creates an object that implements `CanSend<Inner>` for any message `Inner`
    /// that can be converted to `M`.
    pub fn break_apart(self) -> BreakApart<M> {
        BreakApart { sender: self }
    }
}

/// Wraps a `CanSendAsync`. Similar to [`Sender<M>`] but for asynchronous messaging.
pub struct AsyncSender<M: 'static, R: Send + 'static> {
    sender: Arc<dyn CanSendAsync<M, R>>,
}

impl<M, R: Send + 'static> Clone for AsyncSender<M, R> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

/// Extension functions to wrap a `CanSendAsync` as an [`AsyncSender`].
pub trait IntoAsyncSender<M, R: Send> {
    /// Convert an owned implementer into an [`AsyncSender`].
    fn into_async_sender(self) -> AsyncSender<M, R>;
    /// Convert a reference-counted implementer into an [`AsyncSender`].
    fn as_async_sender(self: &Arc<Self>) -> AsyncSender<M, R>;
}

impl<M, R: Send + 'static, T: CanSendAsync<M, R>> IntoAsyncSender<M, R> for T {
    fn into_async_sender(self) -> AsyncSender<M, R> {
        AsyncSender::from_impl(self)
    }

    fn as_async_sender(self: &Arc<Self>) -> AsyncSender<M, R> {
        AsyncSender::from_arc(self.clone())
    }
}

impl<M, R: Send + 'static> AsyncSender<M, R> {
    /// Sends a message asynchronously, forwarding to the underlying [`CanSendAsync`].
    pub fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        self.sender.send_async(message)
    }

    fn from_impl(sender: impl CanSendAsync<M, R> + 'static) -> Self {
        Self { sender: Arc::new(sender) }
    }

    fn from_arc<T: CanSendAsync<M, R> + 'static>(arc: Arc<T>) -> Self {
        Self { sender: arc }
    }

    /// Creates an async sender backed by a synchronous function that returns the response.
    pub fn from_async_fn(send_async: impl Fn(M) -> R + Send + Sync + 'static) -> Self {
        Self::from_impl(SendAsyncFunction::new(send_async))
    }
}

/// Convenience trait allowing callers to invoke `send_async` without naming [`CanSendAsync`].
/// See [`AsyncSendError`] for reasons that the returned future may resolve to an error.
pub trait SendAsync<M, R: Send + 'static> {
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>>;
}

impl<M, R: Send + 'static, A: CanSendAsync<M, R> + ?Sized> SendAsync<M, R> for A {
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        CanSendAsync::send_async(self, message)
    }
}

/// Generic failure for async send.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AsyncSendError {
    // The receiver side could not accept the message.
    Closed,
    // The receiver side could not process the message in time.
    Timeout,
    // The message was ignored entirely.
    Dropped,
}

impl std::error::Error for AsyncSendError {}

impl Display for AsyncSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

/// Response handle allowing tests and adapters to resolve an async message manually.
pub struct AsyncResponseSender<R: Send + 'static> {
    callback: Option<Box<dyn FnOnce(BoxFuture<'static, Result<R, AsyncSendError>>) + Send>>,
}

impl<R: Send + 'static> AsyncResponseSender<R> {
    fn new(
        callback: Box<dyn FnOnce(BoxFuture<'static, Result<R, AsyncSendError>>) + Send>,
    ) -> Self {
        Self { callback: Some(callback) }
    }

    /// Respond with a future that will be awaited by the sender.
    pub fn respond_with(
        mut self,
        response: impl std::future::Future<Output = Result<R, AsyncSendError>> + Send + 'static,
    ) {
        if let Some(callback) = self.callback.take() {
            callback(response.boxed());
        }
    }

    /// Respond with an immediate result.
    pub fn respond(self, result: Result<R, AsyncSendError>) {
        self.respond_with(async move { result });
    }
}

/// Message plus responder used by [`MultiSendMessage`] to expose async traffic.
pub struct AsyncMessage<M, R: Send + 'static> {
    pub message: M,
    pub responder: AsyncResponseSender<R>,
}

impl<M, R: Send + 'static> AsyncMessage<M, R> {
    /// Create a new async message together with a future that resolves when the responder is used.
    pub fn new(message: M) -> (Self, BoxFuture<'static, Result<R, AsyncSendError>>) {
        let (sender, receiver) =
            oneshot::channel::<BoxFuture<'static, Result<R, AsyncSendError>>>();
        let responder = AsyncResponseSender::new(Box::new(move |future| {
            sender.send(future).ok();
        }));
        let msg = AsyncMessage { message, responder };
        let future = async move {
            match receiver.await {
                Ok(result_future) => result_future.await,
                Err(_) => Err(AsyncSendError::Dropped),
            }
        }
        .boxed();
        (msg, future)
    }

    /// Respond with a future result.
    pub fn respond_with(
        self,
        response: impl std::future::Future<Output = Result<R, AsyncSendError>> + Send + 'static,
    ) {
        self.responder.respond_with(response);
    }

    /// Respond with an immediate result.
    pub fn respond(self, result: Result<R, AsyncSendError>) {
        self.responder.respond(result);
    }
}

impl<T: Debug, R: Send + 'static> Debug for AsyncMessage<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AsyncMessage").field(&self.message).finish()
    }
}

/// Sometimes we want to be able to pass in a sender that has not yet been fully constructed.
/// LateBoundSender can act as a placeholder to pass CanSend and CanSendAsync capabilities
/// through to the inner object. bind() should be called when the inner object is ready.
/// All calls to send() and send_async() through this wrapper will block until bind() is called.
///
/// Example:
///   let late_bound = LateBoundSender::new();
///   let something_else = SomethingElse::new(late_bound.as_sender());
///   let implementation = Implementation::new(something_else);
///   late_bound.bind(implementation);
pub struct LateBoundSender<S> {
    sender: OnceLock<S>,
}

impl<S> LateBoundSender<S> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { sender: OnceLock::new() })
    }

    pub fn bind(&self, sender: S) {
        self.sender.set(sender).map_err(|_| ()).expect("cannot set sender twice");
    }
}

/// Allows LateBoundSender to be convertible to a Sender as long as the inner object could be.
impl<M, S: CanSend<M>> CanSend<M> for LateBoundSender<S> {
    fn send(&self, message: M) {
        self.sender.wait().send(message);
    }
}

impl<M, R, S: CanSendAsync<M, R>> CanSendAsync<M, R> for LateBoundSender<S> {
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        self.sender.wait().send_async(message)
    }
}

pub struct Noop;

impl<M> CanSend<M> for Noop {
    fn send(&self, _message: M) {}
}

impl<M> CanSend<M> for Arc<Noop> {
    fn send(&self, _message: M) {}
}

impl<M, R: Send + 'static> CanSendAsync<M, R> for Noop {
    fn send_async(&self, _message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        async { Err(AsyncSendError::Dropped) }.boxed()
    }
}

impl<M, R: Send + 'static> CanSendAsync<M, R> for Arc<Noop> {
    fn send_async(&self, _message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        async { Err(AsyncSendError::Dropped) }.boxed()
    }
}

/// Creates a no-op sender that does nothing with the message.
///
/// Returns a type that can be converted to any type of sender,
/// sync or async, including multi-senders.
pub fn noop() -> Arc<Noop> {
    Arc::new(Noop)
}

/// A trait for converting something that implements individual senders into
/// a multi-sender.
pub trait IntoMultiSender<A> {
    fn as_multi_sender(self: &Arc<Self>) -> A;
    fn into_multi_sender(self) -> A;
}

pub trait MultiSenderFrom<A> {
    fn multi_sender_from(a: Arc<A>) -> Self;
}

impl<A, B: MultiSenderFrom<A>> IntoMultiSender<B> for A {
    fn as_multi_sender(self: &Arc<Self>) -> B {
        B::multi_sender_from(self.clone())
    }
    fn into_multi_sender(self) -> B {
        B::multi_sender_from(Arc::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    #[derive(Clone)]
    struct DroppingAsyncSender;

    impl CanSendAsync<u32, u32> for DroppingAsyncSender {
        fn send_async(&self, _message: u32) -> BoxFuture<'static, Result<u32, AsyncSendError>> {
            async { Err(AsyncSendError::Dropped) }.boxed()
        }
    }

    #[tokio::test]
    async fn test_async_send_sender_dropped() {
        // The handler drops the callback, making the response will never resolve.
        let sender = Arc::new(DroppingAsyncSender).as_async_sender();
        let result = sender.send_async(4).await;
        assert_eq!(result, Err(AsyncSendError::Dropped));
    }

    #[derive(Clone)]
    struct BackgroundResponder {
        result_blocker: CancellationToken,
        callback_done: CancellationToken,
    }

    impl CanSendAsync<u32, u32> for BackgroundResponder {
        fn send_async(&self, message: u32) -> BoxFuture<'static, Result<u32, AsyncSendError>> {
            let blocker = self.result_blocker.clone();
            let done = self.callback_done.clone();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                blocker.cancelled().await;
                sender.send(Ok(message)).ok();
                done.cancel();
            });
            async move {
                match receiver.await {
                    Ok(result) => result,
                    Err(_) => Err(AsyncSendError::Dropped),
                }
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn test_async_send_receiver_dropped() {
        // Test that if the receiver is dropped, the sending side will not panic.
        let result_blocker = CancellationToken::new();
        let callback_done = CancellationToken::new();

        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::abort();
        }));

        let sender = {
            let result_blocker = result_blocker.clone();
            let callback_done = callback_done.clone();
            Arc::new(BackgroundResponder { result_blocker, callback_done }).as_async_sender()
        };

        drop(sender.send_async(4)); // drops the resulting future
        result_blocker.cancel();
        callback_done.cancelled().await;
    }
}
