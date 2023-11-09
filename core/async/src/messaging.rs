use futures::future::BoxFuture;
use futures::FutureExt;
use once_cell::sync::OnceCell;
use std::sync::Arc;

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
}

/// Allows the sending of a message while expecting a response.
pub trait CanSendAsync<M, R>: Send + Sync + 'static {
    fn send_async(&self, message: M) -> BoxFuture<'static, R>;
}

pub struct AsyncSender<M: 'static, R: 'static> {
    sender: Arc<dyn CanSendAsync<M, R>>,
}

impl<M, R> Clone for AsyncSender<M, R> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

/// Extension functions to wrap a CanSendAsync as an AsyncSender.
pub trait IntoAsyncSender<M, R> {
    /// This allows conversion of an owned CanSendAsync into an AsyncSender.
    fn into_async_sender(self) -> AsyncSender<M, R>;
    /// This allows conversion of a reference-counted CanSendAsync into an AsyncSender.
    fn as_async_sender(self: &Arc<Self>) -> AsyncSender<M, R>;
}

impl<M, R, T: CanSendAsync<M, R>> IntoAsyncSender<M, R> for T {
    fn into_async_sender(self) -> AsyncSender<M, R> {
        AsyncSender::from_impl(self)
    }
    fn as_async_sender(self: &Arc<Self>) -> AsyncSender<M, R> {
        AsyncSender::from_arc(self.clone())
    }
}

impl<M, R> AsyncSender<M, R> {
    pub fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        self.sender.send_async(message)
    }

    fn from_impl(sender: impl CanSendAsync<M, R> + 'static) -> Self {
        Self { sender: Arc::new(sender) }
    }

    fn from_arc<T: CanSendAsync<M, R> + 'static>(arc: Arc<T>) -> Self {
        Self { sender: arc }
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

/// Allows LateBoundSender to be convertible to an AsyncSender as long as the inner object could
/// be.
impl<M, R, S: CanSendAsync<M, R>> CanSendAsync<M, R> for LateBoundSender<S> {
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        self.sender.wait().send_async(message)
    }
}

pub struct Noop;

impl<M> CanSend<M> for Noop {
    fn send(&self, _message: M) {}
}

impl<M: Send, R: Send + 'static, E: Default + Send + 'static> CanSendAsync<M, Result<R, E>>
    for Noop
{
    fn send_async(&self, _message: M) -> BoxFuture<'static, Result<R, E>> {
        futures::future::ready(Err(E::default())).boxed()
    }
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
