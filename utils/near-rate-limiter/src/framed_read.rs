use std::borrow::BorrowMut;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures_core::{ready, Stream};
use log::trace;

use pin_project_lite::pin_project;
use tokio::io::AsyncRead;
use tokio_util::codec::Decoder;
use tokio_util::io::poll_read_buf;
use tokio_util::sync::PollSemaphore;

// Initial capacity of read buffer.
const INITIAL_CAPACITY: usize = 8 * 1024;

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
        pub(crate) throttle_controller: ThrottleController,
        pub(crate) semaphore: PollSemaphore,
    }
}

/// Each RateLimiterHelper is associated with exactly one `Peer`. It keeps track total size of all
/// messages / their count that were received from given peer. We are going to keep tracking them
/// as long as their exist in transit in inside `Actix` queues. For example from `Peer` to
/// `PeerManager` and back to `Peer`.
///
/// TODO (#5155) Add throttling by bandwidth.
#[derive(Clone, Debug)]
pub struct ThrottleController {
    /// Total count of all messages that are tracked by `RateLimiterHelper`
    num_messages_in_progress: Arc<AtomicUsize>,
    /// Total size of all messages that are tracked by `RateLimiterHelper`
    total_sizeof_messages_in_progress: Arc<AtomicUsize>,
    /// We have an unbounded queue of messages, that we use to wake `ThrottledRateLimiter`.
    /// This is the sender part, which is used to notify `ThrottledRateLimiter` to try to
    /// read again from queue.
    /// max size of num_messages_in_progress
    max_num_messages_in_progress: usize,
    /// max size of max_total_sizeof_messages_in_progress
    max_total_sizeof_messages_in_progress: usize,
    semaphore: PollSemaphore,
}

/// Stores metadata about size of message, which is currently tracked by `ThrottleController`.
/// Only one instance of this struct should exist, that's why there is no close.
pub struct ThrottleToken {
    /// Represents limits for `PeerActorManager`
    throttle_controller: ThrottleController,
    /// Size of message tracked.
    msg_len: usize,
}

impl ThrottleToken {
    pub fn new(throttle_controller: ThrottleController, msg_len: usize) -> Self {
        throttle_controller.add_msg(msg_len);
        Self { throttle_controller, msg_len }
    }

    pub fn into_inner(&self) -> ThrottleController {
        self.throttle_controller.clone()
    }
}

impl Drop for ThrottleToken {
    fn drop(&mut self) {
        self.throttle_controller.remove_msg(self.msg_len)
    }
}

impl ThrottleController {
    /// Initialize `ThrottleController`.
    ///
    /// Arguments:
    /// - `semaphore` - `semaphore` is used to notify `ThrottleController` to wake up and
    ///        consider reading again.
    ///        That happens when a message is removed, and limits are increased.
    /// - `max_num_messages_in_progress` - maximum number of messages count before throttling starts.
    /// - `max_total_sizeof_messages_in_progress` - maximum total size of messages count
    ///        before throttling starts.
    pub fn new(
        semaphore: PollSemaphore,
        max_num_messages_in_progress: usize,
        max_total_sizeof_messages_in_progress: usize,
    ) -> Self {
        Self {
            num_messages_in_progress: Arc::new(AtomicUsize::new(0)),
            total_sizeof_messages_in_progress: Arc::new(AtomicUsize::new(0)),
            max_num_messages_in_progress,
            max_total_sizeof_messages_in_progress,
            semaphore,
        }
    }

    /// Check whenever `ThrottleFrameRead` is allowed to read from socket.
    /// That is, we didn't exceed limits yet.
    fn is_ready(&self) -> bool {
        (self.num_messages_in_progress.load(Ordering::SeqCst) < self.max_num_messages_in_progress)
            && (self.total_sizeof_messages_in_progress.load(Ordering::SeqCst)
                < self.max_total_sizeof_messages_in_progress)
    }

    /// Tracks the message and increase limits by size of the message.
    pub fn add_msg(&self, msg_size: usize) {
        self.num_messages_in_progress.fetch_add(1, Ordering::SeqCst);
        if msg_size != 0 {
            self.total_sizeof_messages_in_progress.fetch_add(msg_size, Ordering::SeqCst);
        }
    }

    /// Un-tracks the message and decreases limits by size of the message and notifies
    /// `ThrottledFramedReader` to try to read again
    pub fn remove_msg(&mut self, msg_size: usize) {
        self.num_messages_in_progress.fetch_sub(1, Ordering::SeqCst);
        if msg_size != 0 {
            self.total_sizeof_messages_in_progress.fetch_sub(msg_size, Ordering::SeqCst);
        }

        // If `ThrottledFramedReader` is not scheduled to read.
        if self.semaphore.available_permits() == 0 {
            // Notify throttled framed reader to start readin
            self.semaphore.add_permits(1);
        }
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
        throttle_controller: ThrottleController,
        semaphore: PollSemaphore,
    ) -> ThrottledFrameRead<T, D> {
        ThrottledFrameRead {
            inner: FramedImpl {
                inner,
                codec: decoder,
                state: Default::default(),
                throttle_controller,
                semaphore,
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
            while !pinned.throttle_controller.is_ready() {
                // This will cause us to subscribe to notifier when something gets pushed to
                // `pinned.receiver`. If there is an element in the queue, we will check again.
                ready!(PollSemaphore::poll_next(Pin::new(pinned.semaphore), cx));
            }

            // Repeatedly call `decode` or `decode_eof` as long as it is
            // "readable". Readable is defined as not having returned `None`. If
            // the upstream has returned EOF, and the decoder is no longer
            // readable, it can be assumed that the decoder will never become
            // readable again, at which point the stream is terminated.
            if state.is_readable {
                if state.eof {
                    let frame = pinned.codec.decode_eof(&mut state.buffer)?;
                    return Poll::Ready(frame.map(Ok));
                }
                trace!("attempting to decode a frame");

                if let Some(frame) = pinned.codec.decode(&mut state.buffer)? {
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

            let bytect = ready!(poll_read_buf(pinned.inner.as_mut(), cx, &mut state.buffer)?);

            if bytect == 0 {
                state.eof = true;
            }

            state.is_readable = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ThrottleController;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio_util::sync::PollSemaphore;

    #[tokio::test]
    async fn test_rate_limiter_helper_by_count() {
        let semaphore = PollSemaphore::new(Arc::new(Semaphore::new(0)));
        let max_messages_count = 20;
        let mut throttle_controller =
            ThrottleController::new(semaphore.clone(), max_messages_count, 2000000);
        for _ in 0..max_messages_count {
            assert_eq!(throttle_controller.is_ready(), true);
            throttle_controller.add_msg(100);
        }
        assert_eq!(throttle_controller.is_ready(), false);

        for _ in 0..max_messages_count {
            throttle_controller.add_msg(100);
        }

        assert_eq!(
            throttle_controller.num_messages_in_progress.load(SeqCst),
            2 * max_messages_count
        );
        assert_eq!(
            throttle_controller.total_sizeof_messages_in_progress.load(SeqCst),
            2 * max_messages_count * 100
        );

        for _ in 0..max_messages_count {
            assert_eq!(throttle_controller.is_ready(), false);
            throttle_controller.remove_msg(100);
        }

        assert_eq!(throttle_controller.is_ready(), false);

        for _ in 0..max_messages_count {
            throttle_controller.remove_msg(100);
            assert_eq!(throttle_controller.is_ready(), true);
        }

        assert_eq!(throttle_controller.num_messages_in_progress.load(SeqCst), 0);
        assert_eq!(throttle_controller.total_sizeof_messages_in_progress.load(SeqCst), 0);

        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn test_rate_limiter_helper_by_size() {
        let max_messages_total_size = 500_000_000;
        let semaphore = PollSemaphore::new(Arc::new(Semaphore::new(0)));
        let mut throttle_controller =
            ThrottleController::new(semaphore.clone(), 1000, max_messages_total_size);

        for _ in 0..8 {
            assert_eq!(throttle_controller.is_ready(), true);
            throttle_controller.add_msg(max_messages_total_size / 8);
        }
        assert_eq!(throttle_controller.is_ready(), false);

        for _ in 0..8 {
            throttle_controller.add_msg(max_messages_total_size / 8);
        }

        assert_eq!(throttle_controller.num_messages_in_progress.load(SeqCst), 2 * 8);
        assert_eq!(
            throttle_controller.total_sizeof_messages_in_progress.load(SeqCst),
            2 * max_messages_total_size
        );

        for _ in 0..8 {
            assert_eq!(throttle_controller.is_ready(), false);
            throttle_controller.remove_msg(max_messages_total_size / 8);
        }

        assert_eq!(throttle_controller.is_ready(), false);

        for _ in 0..8 {
            throttle_controller.remove_msg(max_messages_total_size / 8);
            assert_eq!(throttle_controller.is_ready(), true);
        }

        assert_eq!(throttle_controller.num_messages_in_progress.load(SeqCst), 0);
        assert_eq!(throttle_controller.total_sizeof_messages_in_progress.load(SeqCst), 0);

        assert_eq!(semaphore.available_permits(), 1);
    }
}
