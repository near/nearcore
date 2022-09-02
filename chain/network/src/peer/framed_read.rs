use bytes::BytesMut;
use futures_core::{ready, Stream};
use pin_project_lite::pin_project;
use std::borrow::BorrowMut;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;
use tokio::sync::Semaphore;
use tokio_util::codec::Decoder;
use tokio_util::io::poll_read_buf;
use tokio_util::sync::PollSemaphore;
use tracing::trace;
use near_network_primitives::types::ReasonForBan;
use std::io;
use crate::peer::codec::NETWORK_MESSAGE_MAX_SIZE_BYTES;

// Initial capacity of read buffer.
const INITIAL_CAPACITY: usize = 8 * 1024;

struct State {
    eof: bool,
    is_readable: bool,
    buffer: BytesMut,
}

pin_project! {
    #[derive(Debug)]
    pub(crate) struct ThrottleFramedRead<T> {
        #[pin]
        inner: T,
        state: State,
        throttle_controller: ThrottleController,
    }
}

impl<T:AsyncRead> ThrottleFramedRead<T> {
    /// Creates a new `ThrottleFramedRead`.
    pub fn new(
        inner: T,
        throttle_controller: ThrottleController,
    ) -> ThrottleFramedRead<T> {
        ThrottleFramedRead {
            inner,
            state: Default::default(),
            throttle_controller,
        }
    }
}

impl Default for State {
    fn default() -> Self {
        Self { eof: false, is_readable: false, buffer: BytesMut::with_capacity(INITIAL_CAPACITY) }
    }
}

impl<T:AsyncRead> ThrottleFramedRead<T> {
    fn decode(&mut self) -> Result<Option<Vec<u8>>, io::Error> {
        let buf = &mut self.state.buffer;
        let len_buf = match buf.get(..4).and_then(|s| <[u8; 4]>::try_from(s).ok()) {
            // not enough bytes to start decoding
            None => return Ok(None),
            Some(res) => res,
        };

        let len = u32::from_le_bytes(len_buf) as usize;
        if len > NETWORK_MESSAGE_MAX_SIZE_BYTES {
            // If this point is reached, abusive peer is banned.
            return Ok(Some(Err(ReasonForBan::Abusive)));
        }

        if let Some(data_buf) = buf.get(4..4 + len) {
            let res = Some(Ok(data_buf.to_vec()));
            buf.advance(4 + len);
            if buf.is_empty() && buf.capacity() > 0 {
                *buf = BytesMut::new();
            }
            Ok(res)
        } else {
            // not enough bytes, keep waiting
            Ok(None)
        }
    }
}

impl<T:AsyncRead> Stream for ThrottleFramedRead<T> {
    type Item = Result<Vec<u8>, ReasonForBan>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut pinned = self.project();
        let state: &mut State = pinned.state.borrow_mut();
        loop {
            while !pinned.throttle_controller.is_ready() {
                // This will cause us to subscribe to notifier when something gets pushed to
                // `pinned.receiver`. If there is an element in the queue, we will check again.
                ready!(pinned.throttle_controller.semaphore.poll_acquire(cx));
            }

            // Repeatedly call `decode` or `decode_eof` as long as it is
            // "readable". Readable is defined as not having returned `None`. If
            // the upstream has returned EOF, and the decoder is no longer
            // readable, it can be assumed that the decoder will never become
            // readable again, at which point the stream is terminated.
            if state.is_readable {
                if state.eof {
                    return Poll::Ready(pinned.decode(&mut state.buffer)?.map(Ok));
                }
                trace!("attempting to decode a frame");
                if let Some(frame) = pinned.decode(&mut state.buffer)? {
                    trace!("frame decoded from buffer");
                    pinned.throttle_controller.report_msg_seen();
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
            pinned.throttle_controller.report_bandwidth_used(bytect);
            if bytect == 0 {
                state.eof = true;
            }
            state.is_readable = true;
        }
    }
}

/// Each RateLimiterHelper is associated with exactly one `Peer`. It keeps track total size of all
/// messages / their count that were received from given peer. We are going to keep tracking them
/// as long as their exist in transit in inside `Actix` queues. For example from `Peer` to
/// `PeerManager` and back to `Peer`.
///
/// TODO (#5155) Add throttling by bandwidth.
#[derive(Clone, Debug)]
pub(crate) struct ThrottleController {
    /// Total count of all messages that are tracked by `RateLimiterHelper`
    num_messages_in_progress: Arc<AtomicUsize>,
    /// Maximum value of `num_messages_in_progress` recorded since last `consume` call.
    max_messages_in_progress: Arc<AtomicUsize>,
    /// Total size of all messages that are tracked by `RateLimiterHelper`
    total_sizeof_messages_in_progress: Arc<AtomicUsize>,
    /// Keeps track of read bandwidth.
    bandwidth_read: Arc<AtomicUsize>,
    /// Stores count of messages seen.
    msg_seen: Arc<AtomicUsize>,
    /// We have an unbounded queue of messages, that we use to wake `ThrottleRateLimiter`.
    /// This is the sender part, which is used to notify `ThrottleRateLimiter` to try to
    /// read again from queue.
    /// max size of num_messages_in_progress
    max_num_messages_in_progress: usize,
    /// max size of max_total_sizeof_messages_in_progress
    max_total_sizeof_messages_in_progress: usize,
    semaphore: PollSemaphore,
}

/// Stores metadata about size of message, which is currently tracked by `ThrottleController`.
/// Only one instance of this struct should exist, that's why there is no close.
pub(crate) struct ThrottleToken {
    /// Represents limits for `PeerActorManager`
    throttle_controller: Option<ThrottleController>,
    /// Size of message tracked.
    msg_len: usize,
}

impl ThrottleToken {
    /// Creates Token with specified `msg_len`.
    pub fn new(mut throttle_controller: Option<ThrottleController>, msg_len: usize) -> Self {
        if let Some(th) = throttle_controller.as_mut() {
            th.add_msg(msg_len);
        };
        Self { throttle_controller, msg_len }
    }

    /// Creates Token without specifying `msg_len`.
    pub fn new_without_size(mut throttle_controller: Option<ThrottleController>) -> Self {
        if let Some(th) = throttle_controller.as_mut() {
            th.add_msg(0);
        };
        Self { throttle_controller, msg_len: 0 }
    }

    /// Gets ThrottleController associated with `ThrottleToken`.
    pub fn throttle_controller(&self) -> Option<&ThrottleController> {
        self.throttle_controller.as_ref()
    }
}

impl Drop for ThrottleToken {
    fn drop(&mut self) {
        if let Some(th) = self.throttle_controller.as_mut() {
            th.remove_msg(self.msg_len);
        };
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
        max_num_messages_in_progress: usize,
        max_total_sizeof_messages_in_progress: usize,
    ) -> Self {
        Self {
            num_messages_in_progress: Default::default(),
            max_messages_in_progress: Default::default(),
            total_sizeof_messages_in_progress: Default::default(),
            bandwidth_read: Default::default(),
            msg_seen: Default::default(),
            max_num_messages_in_progress,
            max_total_sizeof_messages_in_progress,
            semaphore: PollSemaphore::new(Arc::new(Semaphore::new(0))),
        }
    }

    /// Check whenever `ThrottleFramedRead` is allowed to read from socket.
    /// That is, we didn't exceed limits yet.
    fn is_ready(&self) -> bool {
        (self.num_messages_in_progress.load(Ordering::Relaxed) < self.max_num_messages_in_progress)
            && (self.total_sizeof_messages_in_progress.load(Ordering::Relaxed)
                < self.max_total_sizeof_messages_in_progress)
    }

    /// Tracks the message and increase limits by size of the message.
    pub fn add_msg(&self, msg_size: usize) {
        let new_cnt = self.num_messages_in_progress.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_messages_in_progress.fetch_max(new_cnt, Ordering::Relaxed);
        if msg_size != 0 {
            self.total_sizeof_messages_in_progress.fetch_add(msg_size, Ordering::Relaxed);
        }
    }

    /// Un-tracks the message and decreases limits by size of the message and notifies
    /// `ThrottleFramedRead` to try to read again
    pub fn remove_msg(&mut self, msg_size: usize) {
        self.num_messages_in_progress.fetch_sub(1, Ordering::Relaxed);
        if msg_size != 0 {
            self.total_sizeof_messages_in_progress.fetch_sub(msg_size, Ordering::Relaxed);
        }

        // Notify throttled framed reader to start reading
        self.semaphore.add_permits(1);
    }

    pub fn consume_bandwidth_used(&mut self) -> usize {
        self.bandwidth_read.swap(0, Ordering::Relaxed)
    }

    pub fn report_bandwidth_used(&mut self, size: usize) -> usize {
        self.bandwidth_read.fetch_add(size, Ordering::Relaxed)
    }

    pub fn consume_msg_seen(&mut self) -> usize {
        self.msg_seen.swap(0, Ordering::Relaxed)
    }

    pub fn report_msg_seen(&mut self) -> usize {
        self.msg_seen.fetch_add(1, Ordering::Relaxed)
    }

    pub fn consume_max_messages_in_progress(&mut self) -> usize {
        self.max_messages_in_progress.swap(0, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{ThrottleController, ThrottleFramedRead};
    use bytes::{Buf, BytesMut};
    use futures_core::Stream;
    use std::error::Error;
    use std::pin::Pin;
    use std::ptr::null;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::Instant;
    use tokio_stream::StreamExt;
    use tokio_util::codec::Decoder;

    #[tokio::test]
    async fn test_rate_limiter_helper_by_count() {
        let max_messages_count = 20;
        let mut throttle_controller = ThrottleController::new(max_messages_count, 2000000);
        for _ in 0..max_messages_count {
            assert!(throttle_controller.is_ready());
            throttle_controller.add_msg(100);
        }
        assert!(!throttle_controller.is_ready());

        for _ in 0..max_messages_count {
            throttle_controller.add_msg(100);
        }

        assert_eq!(
            throttle_controller.num_messages_in_progress.load(Ordering::Relaxed),
            2 * max_messages_count
        );
        assert_eq!(
            throttle_controller.total_sizeof_messages_in_progress.load(Ordering::Relaxed),
            2 * max_messages_count * 100
        );

        for _ in 0..max_messages_count {
            assert!(!throttle_controller.is_ready());
            throttle_controller.remove_msg(100);
        }

        assert!(!throttle_controller.is_ready());

        for _ in 0..max_messages_count {
            throttle_controller.remove_msg(100);
            assert!(throttle_controller.is_ready());
        }

        assert_eq!(throttle_controller.num_messages_in_progress.load(Ordering::Relaxed), 0);
        assert_eq!(
            throttle_controller.total_sizeof_messages_in_progress.load(Ordering::Relaxed),
            0
        );

        assert_eq!(throttle_controller.semaphore.available_permits(), 40);
    }

    #[tokio::test]
    async fn test_rate_limiter_helper_by_size() {
        let max_messages_total_size = 500_000_000;
        let mut throttle_controller = ThrottleController::new(1000, max_messages_total_size);

        for _ in 0..8 {
            assert!(throttle_controller.is_ready());
            throttle_controller.add_msg(max_messages_total_size / 8);
        }
        assert!(!throttle_controller.is_ready());

        for _ in 0..8 {
            throttle_controller.add_msg(max_messages_total_size / 8);
        }

        assert_eq!(throttle_controller.num_messages_in_progress.load(Ordering::Relaxed), 2 * 8);
        assert_eq!(
            throttle_controller.total_sizeof_messages_in_progress.load(Ordering::Relaxed),
            2 * max_messages_total_size
        );

        for _ in 0..8 {
            assert!(!throttle_controller.is_ready());
            throttle_controller.remove_msg(max_messages_total_size / 8);
        }

        assert!(!throttle_controller.is_ready());

        for _ in 0..8 {
            throttle_controller.remove_msg(max_messages_total_size / 8);
            assert!(throttle_controller.is_ready());
        }

        assert_eq!(throttle_controller.num_messages_in_progress.load(Ordering::Relaxed), 0);
        assert_eq!(
            throttle_controller.total_sizeof_messages_in_progress.load(Ordering::Relaxed),
            0
        );

        assert_eq!(throttle_controller.semaphore.available_permits(), 16);
    }

    #[test]
    fn test_throttle_controller() {
        let max_messages_total_size = 500_000_000;
        let mut throttle_controller = ThrottleController::new(1000, max_messages_total_size);

        assert_eq!(throttle_controller.consume_msg_seen(), 0);
        throttle_controller.report_msg_seen();
        throttle_controller.report_msg_seen();
        throttle_controller.report_msg_seen();
        assert_eq!(throttle_controller.consume_msg_seen(), 3);
        assert_eq!(throttle_controller.consume_msg_seen(), 0);

        assert_eq!(throttle_controller.consume_bandwidth_used(), 0);
        throttle_controller.report_bandwidth_used(10);
        throttle_controller.report_bandwidth_used(1000);
        throttle_controller.report_bandwidth_used(5);
        assert_eq!(throttle_controller.consume_bandwidth_used(), 1015);
        assert_eq!(throttle_controller.consume_bandwidth_used(), 0);
    }

    #[tokio::test]
    async fn test_max_messages_in_progress() {
        let mut throttle_controller = ThrottleController::new(1000, usize::MAX);

        assert_eq!(throttle_controller.consume_max_messages_in_progress(), 0);
        throttle_controller.add_msg(0);
        throttle_controller.add_msg(0);
        throttle_controller.remove_msg(0);
        throttle_controller.remove_msg(0);

        assert_eq!(throttle_controller.consume_max_messages_in_progress(), 2);
        assert_eq!(throttle_controller.consume_max_messages_in_progress(), 0);
    }

    #[derive(Default)]
    pub struct Codec {}

    impl Decoder for Codec {
        type Item = u8;
        type Error = std::io::Error;

        fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, std::io::Error> {
            if buf.is_empty() {
                // not enough bytes to start decoding
                return Ok(None);
            }
            let res = buf[0];
            buf.advance(1);
            Ok(Some(res))
        }
    }

    unsafe fn noop_clone(_data: *const ()) -> RawWaker {
        noop_raw_waker()
    }

    unsafe fn noop(_data: *const ()) {}

    const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);

    const fn noop_raw_waker() -> RawWaker {
        RawWaker::new(null(), &NOOP_WAKER_VTABLE)
    }
    pub fn noop_waker() -> Waker {
        // FIXME: Since 1.46.0 we can use transmute in consts, allowing this function to be const.
        unsafe { Waker::from_raw(noop_raw_waker()) }
    }

    #[tokio::test]
    async fn test_throttle_framed_read() -> Result<(), Box<dyn Error>> {
        let rate_limiter = ThrottleController::new(1, usize::MAX);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("TcpListener::bind");
        let addr = listener.local_addr().expect("addr");

        let client_stream = TcpStream::connect(addr).await?;

        let (mut server_tcp_stream, _socket) = listener.accept().await.unwrap();

        // Write some data.
        server_tcp_stream.write_all(b"hello world!").await?;

        let (read, _write) = tokio::io::split(client_stream);
        let mut read = ThrottleFramedRead::new(read, Codec::default(), rate_limiter.clone());

        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        let start = Instant::now();
        // Unfortunately, the test times out for some reason.
        loop {
            let msg = read.next().await;
            if let Some(msg) = msg {
                tracing::info!(message = "GOT", ?msg);
            }

            assert!(start.elapsed() < Duration::from_secs(10), "timed out {:?}", start.elapsed());

            match Pin::new(&mut read).poll_next(&mut context) {
                Poll::Pending => {
                    // this expected
                }
                Poll::Ready(Some(Ok(101))) => {
                    // got 'h' as expected
                    break;
                }
                Poll::Ready(val) => {
                    panic!("unexpected got {:?}", val);
                }
            }
        }

        rate_limiter.add_msg(1);
        rate_limiter.add_msg(1);

        match Pin::new(&mut read).poll_next(&mut context) {
            Poll::Pending => {
                // this expected
            }
            Poll::Ready(x) => {
                panic!("got {:?}", x);
            }
        }

        Ok(())
    }
}
