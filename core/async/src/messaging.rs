use crate::break_apart::BreakApart;
use crate::functional::SendFunction;
use futures::future::BoxFuture;
use futures::FutureExt;
use once_cell::sync::OnceCell;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Trait for sending a typed message.
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
    pub fn send(&self, message: M) {
        self.sender.send(message)
    }

    fn from_impl(sender: impl CanSend<M> + 'static) -> Self {
        Self { sender: Arc::new(sender) }
    }

    fn from_arc<T: CanSend<M> + 'static>(arc: Arc<T>) -> Self {
        Self { sender: arc }
    }

    pub fn from_fn(send: impl Fn(M) + Send + Sync + 'static) -> Self {
        Self::from_impl(SendFunction::new(send))
    }

    pub fn break_apart(self) -> BreakApart<M> {
        BreakApart { sender: self }
    }
}

pub trait SendAsync<M, R: Send + 'static> {
    fn send_async(&self, message: M) -> BoxFuture<'static, R>;
}

impl<M, R: Send + 'static, A: CanSend<MessageExpectingResponse<M, R>> + ?Sized> SendAsync<M, R>
    for A
{
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        let (sender, receiver) = oneshot::channel::<R>();
        let future = async move { receiver.await.expect("Future was cancelled") };
        let responder = Box::new(move |r| sender.send(r).ok().unwrap());
        self.send(MessageExpectingResponse { message, responder });
        future.boxed()
    }
}

impl<M, R: Send + 'static> Sender<MessageExpectingResponse<M, R>> {
    pub fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        self.sender.send_async(message)
    }
}

pub struct MessageExpectingResponse<T, R> {
    pub message: T,
    pub responder: Box<dyn FnOnce(R) + Send>,
}

impl<T: Debug, R> Debug for MessageExpectingResponse<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MessageWithResponder").field(&self.message).finish()
    }
}

pub type AsyncSender<M, R> = Sender<MessageExpectingResponse<M, R>>;

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
