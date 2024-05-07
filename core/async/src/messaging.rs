use crate::break_apart::BreakApart;
use crate::functional::{SendAsyncFunction, SendFunction};
use crate::futures::DelayedActionRunner;
use futures::future::BoxFuture;
use futures::FutureExt;
use once_cell::sync::OnceCell;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use tokio::sync::oneshot;

pub trait Actor {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {}
}

/// Trait for handling a message.
/// This works in unison with the [`CanSend`] trait. An actor implements the Handler trait for all
/// messages it would like to handle, while the CanSend trait implements the logic to send the
/// message to the actor. Handle and CanSend are typically not both implemented by the same struct.
/// Note that the actor is any struct that implements the Handler trait, not just actix actors.
pub trait Handler<M: actix::Message> {
    fn handle(&mut self, msg: M) -> M::Result;
}

/// Trait for handling a message with context.
/// This is similar to the [`Handler`] trait, but it allows the handler to access the delayed action
/// runner that is used to schedule actions to be run in the future. For actix::Actor, the context
/// defined as actix::Context<Self> implements DelayedActionRunner<T>.
/// Note that the implementer for hander of a message only needs to implement either of Handler or
/// HandlerWithContext, not both.
pub trait HandlerWithContext<M: actix::Message> {
    fn handle(&mut self, msg: M, ctx: &mut dyn DelayedActionRunner<Self>) -> M::Result;
}

impl<A, M> HandlerWithContext<M> for A
where
    M: actix::Message,
    A: Handler<M>,
{
    fn handle(&mut self, msg: M, _ctx: &mut dyn DelayedActionRunner<Self>) -> M::Result {
        Handler::handle(self, msg)
    }
}

/// Trait for sending a typed message. The sent message is then handled by the Handler trait.
/// actix::Addr, which is derived from actix::Actor is an example of a struct that implements CanSend.
/// See [`Handler`] trait for more details.
pub trait CanSend<M>: Send + Sync + 'static {
    fn send(&self, message: M);
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

/// Extension trait (not for anyone to implement), that allows a
/// Sender<MessageWithCallback<M, R>> to be used to send a message and then
/// getting a future of the response.
///
/// See `AsyncSendError` for reasons that the future may resolve to an error result.
pub trait SendAsync<M, R: Send + 'static> {
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>>;
}

impl<M, R: Send + 'static, A: CanSend<MessageWithCallback<M, R>> + ?Sized> SendAsync<M, R> for A {
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        // To send a message and get a future of the response, we use a oneshot
        // channel that is resolved when the responder function is called. It's
        // possible that someone implementing the Sender would just drop the
        // message without calling the responder, in which case we return a
        // Dropped error.
        let (sender, receiver) = oneshot::channel::<Result<R, AsyncSendError>>();
        let future = async move { receiver.await.unwrap_or_else(|_| Err(AsyncSendError::Dropped)) };
        let responder = Box::new(move |r| {
            // It's ok for the send to return an error, because that means the receiver is dropped
            // therefore the sender does not care about the result.
            sender.send(r).ok();
        });
        self.send(MessageWithCallback { message, callback: responder });
        future.boxed()
    }
}

impl<M, R: Send + 'static> Sender<MessageWithCallback<M, R>> {
    /// Same as the above, but for a concrete Sender type.
    pub fn send_async(&self, message: M) -> BoxFuture<'static, Result<R, AsyncSendError>> {
        self.sender.send_async(message)
    }

    /// Creates a sender that would handle send_async messages by producing a
    /// result synchronously. Note that the provided function is NOT async.
    ///
    /// (Implementing the similar functionality with async function is possible
    /// but requires deciding who drives the async function; a FutureSpawner
    /// can be a good idea.)
    pub fn from_async_fn(send_async: impl Fn(M) -> R + Send + Sync + 'static) -> Self {
        Self::from_impl(SendAsyncFunction::new(send_async))
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

impl Display for AsyncSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

/// Used to implement an async sender. An async sender is just a Sender whose
/// message is `MessageWithCallback<M, R>`, which is a message plus a
/// callback function (which resolves the future that send_async returns).
pub struct MessageWithCallback<T, R> {
    pub message: T,
    pub callback: Box<dyn FnOnce(Result<R, AsyncSendError>) + Send>,
}

impl<T: Debug, R> Debug for MessageWithCallback<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MessageWithCallback").field(&self.message).finish()
    }
}

pub type AsyncSender<M, R> = Sender<MessageWithCallback<M, R>>;

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
    sender: OnceCell<S>,
}

impl<S> LateBoundSender<S> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { sender: OnceCell::new() })
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

pub struct Noop;

impl<M> CanSend<M> for Noop {
    fn send(&self, _message: M) {}
}

/// Creates a no-op sender that does nothing with the message.
///
/// Returns a type that can be converted to any type of sender,
/// sync or async, including multi-senders.
pub fn noop() -> Noop {
    Noop
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
    use crate::messaging::{AsyncSendError, MessageWithCallback, Sender};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_async_send_sender_dropped() {
        // The handler drops the callback, making the response will never resolve.
        let sender: Sender<MessageWithCallback<u32, u32>> = Sender::from_fn(|_| {});
        let result = sender.send_async(4).await;
        assert_eq!(result, Err(AsyncSendError::Dropped));
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
            Sender::<MessageWithCallback<u32, u32>>::from_fn(move |msg| {
                let MessageWithCallback { message, callback } = msg;
                let result_blocker = result_blocker.clone();
                let callback_done = callback_done.clone();
                tokio::spawn(async move {
                    result_blocker.cancelled().await;
                    callback(Ok(message));
                    callback_done.cancel();
                });
            })
        };

        drop(sender.send_async(4)); // drops the resulting future
        result_blocker.cancel();
        callback_done.cancelled().await;
    }
}
