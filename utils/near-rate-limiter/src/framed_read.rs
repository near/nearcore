use std::borrow::BorrowMut;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures_core::{ready, Stream};
use log::trace;

use pin_project_lite::pin_project;
use tokio::io::AsyncRead;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::io::poll_read_buf;

// Initial capacity of read buffer.
const INITIAL_CAPACITY: usize = 8 * 1024;

// Max number of messages we received from peer, and they are in progress, before we start throttling.
const MAX_MESSAGES_COUNT: usize = 20;
// Max total size of all messages that are in progress, before we start throttling.
const MAX_MESSAGES_TOTAL_SIZE: usize = 500_000_000;

pin_project! {
    pub struct ThrottledFrameRead<T, D> {
        #[pin]
        inner: FramedImpl<T, D, ReadFrame>,
    }
}

pin_project! {
    #[derive(Debug)]
    pub(crate) struct FramedImpl<T, U, State> {
        #[pin]
        pub(crate) inner: T,
        pub(crate) state: State,
        pub(crate) codec: U,
        pub(crate) rate_limiter: RateLimiterHelper,
        pub(crate) receiver: UnboundedReceiver<()>,
    }
}

// Wrapper around Actix messages, used to track size of all messages sent to PeerManager.
// TODO(#5155) Finish implementation of this.

#[allow(unused)]
/// TODO - Once we start using this `ActixMessageWrapper` we will need to make following changes
/// to get this struct to work
/// - Add needed decorators. Probably `Debug`, `Message` from Actix, etc.
/// - Add two rate limiters (local per peer, global one)
/// - Any other metadata we need debugging, etc.
pub struct ActixMessageWrapper<T> {
    msg: T,
    rate_limiter: RateLimiterHelper,
    msg_len: usize,
}

impl<T> ActixMessageWrapper<T> {
    #[allow(unused)]
    pub fn new(msg: T, rate_limiter: RateLimiterHelper) -> Self {
        // TODO(#5155) Add decorator like SizeOf
        let msg_len = 0; // TODO msg.sizeof()
        rate_limiter.add_msg(msg_len);
        Self { msg, rate_limiter, msg_len }
    }

    #[allow(unused)]
    pub fn take(mut self) -> (T, RateLimiterHelper) {
        self.rate_limiter.remove_msg(self.msg_len);

        return (self.msg, self.rate_limiter);
    }
}

/// Each RateLimiterHelper is associated with exactly one `Peer`. It keeps track total size of all
/// messages / their count that were received from given peer. We are going to keep tracking them
/// as long as their exist in transit in inside `Actix` queues. For example from `Peer` to
/// `PeerManager` and back to `Peer`.
///
/// TODO (#5155) Add throttling by bandwidth.
#[derive(Clone, Debug)]
pub struct RateLimiterHelper {
    // Total count of all messages that are tracked by `RateLimiterHelper`
    pub num_messages_in_progress: Arc<AtomicUsize>,
    // Total size of all messages that are tracked by `RateLimiterHelper`
    pub total_sizeof_messages_in_progress: Arc<AtomicUsize>,
    // We have an unbounded queue of messages, that we use to wake `ThrottledRateLimiter`.
    // This is the sender part, which is used to notify `ThrottledRateLimiter` to try to
    // read again from queue.
    pub tx: UnboundedSender<()>,
}

/// `RateLimiterHelper` is a helper data structure that stores message count/total memory
/// limits per Peer. It controls whenever associated `ThrottleFramedReader` is able to read
/// from TcpSocket. It accepts `tx` as argument, which is used to notify `ThrottleFramedReader`
/// to wake up and consider reading again. That happens when a message is removed, and limits
/// are increased.
impl RateLimiterHelper {
    // Initialize `RateLimiterHelper`.
    pub fn new(tx: UnboundedSender<()>) -> Self {
        Self {
            num_messages_in_progress: Arc::new(AtomicUsize::new(0)),
            total_sizeof_messages_in_progress: Arc::new(AtomicUsize::new(0)),
            tx,
        }
    }

    // Check whenever
    pub fn is_ready(&self) -> bool {
        self.num_messages_in_progress.load(Ordering::SeqCst) <= MAX_MESSAGES_COUNT
            && self.total_sizeof_messages_in_progress.load(Ordering::SeqCst)
                <= MAX_MESSAGES_TOTAL_SIZE
    }

    // Increase limits by size of the message
    pub fn add_msg(&self, msg_size: usize) {
        self.num_messages_in_progress.fetch_add(1, Ordering::SeqCst);
        self.total_sizeof_messages_in_progress.fetch_add(msg_size, Ordering::SeqCst);
    }

    // Decrease limits by size of the message and notify ThrottledFramedReader to try to
    // read again
    pub fn remove_msg(&mut self, msg_size: usize) {
        self.num_messages_in_progress.fetch_sub(1, Ordering::SeqCst);
        self.total_sizeof_messages_in_progress.fetch_sub(msg_size, Ordering::SeqCst);

        // Notify throttled framed reader
        self.tx.send(()).expect("sending msg to tx failed");
    }
}

impl<T, D> ThrottledFrameRead<T, D>
where
    T: AsyncRead,
    D: Decoder,
{
    /// Creates a new `NearFramedRead` with the given `decoder`.
    pub fn new(
        inner: T,
        decoder: D,
        rate_limiter: RateLimiterHelper,
        receiver: UnboundedReceiver<()>,
    ) -> ThrottledFrameRead<T, D> {
        ThrottledFrameRead {
            inner: FramedImpl {
                inner,
                codec: decoder,
                state: Default::default(),
                rate_limiter,
                receiver,
            },
        }
    }
}

impl<T, D> ThrottledFrameRead<T, D> {
    pub fn get_ref(&self) -> &T {
        &self.inner.inner
    }

    pub fn into_inner(self) -> T {
        self.inner.inner
    }

    pub fn decoder(&self) -> &D {
        &self.inner.codec
    }

    pub fn read_buffer(&self) -> &BytesMut {
        &self.inner.state.buffer
    }
}

// This impl just defers to the underlying FramedImpl
impl<T, D> Stream for ThrottledFrameRead<T, D>
where
    T: AsyncRead,
    D: Decoder,
{
    type Item = Result<D::Item, D::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pub(crate) struct ReadFrame {
    pub(crate) eof: bool,
    pub(crate) is_readable: bool,
    pub(crate) buffer: BytesMut,
}

impl Default for ReadFrame {
    fn default() -> Self {
        Self { eof: false, is_readable: false, buffer: BytesMut::with_capacity(INITIAL_CAPACITY) }
    }
}

impl<T, U, R> Stream for FramedImpl<T, U, R>
where
    T: AsyncRead,
    U: Decoder,
    R: BorrowMut<ReadFrame>,
{
    type Item = Result<U::Item, U::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut pinned = self.project();
        let state: &mut ReadFrame = pinned.state.borrow_mut();
        loop {
            if !pinned.rate_limiter.is_ready() {
                // Poll on receiver
                ready!(pinned.receiver.poll_recv(cx));
            }

            // Check condition again
            if !pinned.rate_limiter.is_ready() {
                return Poll::Pending;
            }

            // Repeatedly call `decode` or `decode_eof` as long as it is
            // "readable". Readable is defined as not having returned `None`. If
            // the upstream has returned EOF, and the decoder is no longer
            // readable, it can be assumed that the decoder will never become
            // readable again, at which point the stream is terminated.
            if state.is_readable {
                if state.eof {
                    let frame =
                        pinned.codec.decode_eof(&mut state.buffer, &mut pinned.rate_limiter)?;
                    return Poll::Ready(frame.map(Ok));
                }
                trace!("attempting to decode a frame");

                if let Some(frame) =
                    pinned.codec.decode(&mut state.buffer, &mut pinned.rate_limiter)?
                {
                    trace!("frame decoded from buffer");
                    return Poll::Ready(Some(Ok(frame)));
                }

                state.is_readable = false;
            }

            assert!(!state.eof);

            // Otherwise, try to read more data and try again. Make sure we've
            // got room for at least one byte to read to ensure that we don't
            // get a spurious 0 that looks like EOF
            state.buffer.reserve(1);

            let bytect = match poll_read_buf(pinned.inner.as_mut(), cx, &mut state.buffer)? {
                Poll::Ready(ct) => ct,
                Poll::Pending => return Poll::Pending,
            };

            if bytect == 0 {
                state.eof = true;
            }

            state.is_readable = true;
        }
    }
}

pub trait Decoder {
    type Item;

    type Error: From<io::Error>;

    fn decode(
        &mut self,
        src: &mut BytesMut,
        read_limiter: &mut RateLimiterHelper,
    ) -> Result<Option<Self::Item>, Self::Error>;

    fn decode_eof(
        &mut self,
        buf: &mut BytesMut,
        read_limiter: &mut RateLimiterHelper,
    ) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(buf, read_limiter)? {
            Some(frame) => Ok(Some(frame)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else {
                    Err(io::Error::new(io::ErrorKind::Other, "bytes remaining on stream").into())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::framed_read::MAX_MESSAGES_COUNT;
    use crate::RateLimiterHelper;
    use std::sync::atomic::Ordering::SeqCst;
    use tokio::sync::mpsc;

    #[test]
    fn test_rate_limiter_helper() {
        let (tx, _rx) = mpsc::unbounded_channel::<()>();
        let mut rate_limiter = RateLimiterHelper::new(tx);

        for _ in 0..MAX_MESSAGES_COUNT {
            assert_eq!(rate_limiter.is_ready(), true);
            rate_limiter.add_msg(100);
        }
        assert_eq!(rate_limiter.is_ready(), false);

        for _ in 0..MAX_MESSAGES_COUNT {
            rate_limiter.add_msg(100);
        }

        assert_eq!(rate_limiter.num_messages_in_progress.load(SeqCst), 2 * MAX_MESSAGES_COUNT);
        assert_eq!(
            rate_limiter.total_sizeof_messages_in_progress.load(SeqCst),
            2 * MAX_MESSAGES_COUNT * 100
        );

        for _ in 0..MAX_MESSAGES_COUNT {
            assert_eq!(rate_limiter.is_ready(), false);
            rate_limiter.remove_msg(100);
        }

        assert_eq!(rate_limiter.is_ready(), true);

        for _ in 0..MAX_MESSAGES_COUNT {
            rate_limiter.remove_msg(100);
            assert_eq!(rate_limiter.is_ready(), true);
        }

        assert_eq!(rate_limiter.num_messages_in_progress.load(SeqCst), 0);
        assert_eq!(rate_limiter.total_sizeof_messages_in_progress.load(SeqCst), 0);
    }
}
