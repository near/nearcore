use crate::concurrency::rate;
use crate::peer_manager::connection;
use crate::stats::metrics;
use crate::time;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

/// Maximum capacity of write buffer in bytes.
const MAX_WRITE_BUFFER_CAPACITY_BYTES: usize = bytesize::GIB as usize;

type ReadHalf = tokio::io::ReadHalf<tokio::net::TcpStream>;
type WriteHalf = tokio::io::WriteHalf<tokio::net::TcpStream>;

#[derive(thiserror::Error, Debug)]
pub(crate) enum SendError {
    #[error("IO error")]
    IO(#[source] io::Error),
    #[error("queue is full, got {got_bytes}B, max capacity is {want_max_bytes}")]
    QueueOverflow { got_bytes: usize, want_max_bytes: usize },
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RecvError {
    #[error("IO error")]
    IO(#[source] io::Error),
    #[error("message too large: got {got_bytes}B, want <={want_max_bytes}B")]
    MessageTooLarge { got_bytes: u64, want_max_bytes: u64 },
    #[error("stream closed")]
    Closed,
}

#[derive(actix::Message, PartialEq, Eq, Clone, Debug)]
#[rtype(result = "()")]
pub(crate) struct Frame(pub Vec<u8>);

/// Stream critical error.
/// Actor is responsible for calling ctx.stop() after receiving stream::Error.
/// Actor might receive more than 1 stream::Error, but should call ctx.stop() just after the
/// first one.
/// WARNING: send/recv loops might not get closed if Actor won't call ctx.stop()!.
// TODO(gprusak): once we implement cancellation support for structured concurrency,
// send/recv loops should be cancelled and return before the error is reported to Actor.
#[derive(thiserror::Error, Debug, actix::Message)]
#[rtype(result = "()")]
pub(crate) enum Error {
    #[error("send")]
    Send(#[source] SendError),
    #[error("recv")]
    Recv(#[source] RecvError),
}

pub(crate) struct FramedWriter<Actor: actix::Actor> {
    scope: Scope<Actor>,
    queue_send: tokio::sync::mpsc::UnboundedSender<Frame>,
    stats: Arc<connection::Stats>,
    buf_size_metric: Arc<metrics::IntGaugeGuard>,
}

pub(crate) struct FramedReader {
    closed: bool,
    read: tokio::io::BufReader<ReadHalf>,
    stats: Arc<connection::Stats>,
    msg_size_metric: metrics::HistogramGuard,
    buf_size_metric: metrics::IntGaugeGuard,
}

struct CloseOnDrop<'a>(&'a mut FramedReader);

impl<'a> Drop for CloseOnDrop<'a> {
    fn drop(&mut self) {
        self.0.closed = true;
    }
}

impl FramedReader {
    /// Receives a message from the stream.
    /// Stream uses a fixed small buffer allocated by BufReader.
    /// It allocates a Vec with exact size of the message.
    /// If cancelled (or on error) FramedReader closes.
    pub async fn recv(
        &mut self,
        clock: &time::Clock,
        bytes_limiter: &rate::Limiter,
    ) -> Result<Frame, RecvError> {
        if self.closed {
            return Err(RecvError::Closed);
        }
        let this = CloseOnDrop(self);
        let frame = this.0.recv_inner(clock, bytes_limiter).await?;
        let _ = std::mem::ManuallyDrop::new(this);
        Ok(frame)
    }

    // TODO(gprusak): once borsh support is dropped, we can parse a proto
    // directly from the stream.
    async fn recv_inner(
        &mut self,
        clock: &time::Clock,
        bytes_limiter: &rate::Limiter,
    ) -> Result<Frame, RecvError> {
        let n = self.read.read_u32_le().await.map_err(RecvError::IO)? as u64;
        // TODO(gprusak): separate limit for TIER1 message size.
        bytes_limiter.acquire(clock, n).await.map_err(|err| RecvError::MessageTooLarge {
            got_bytes: err.requested,
            want_max_bytes: err.max,
        })?;
        self.msg_size_metric.observe(n as f64);
        self.buf_size_metric.set(n as i64);
        let mut buf = vec![0; n as usize];
        let t = metrics::PEER_MSG_READ_LATENCY.start_timer();
        self.read.read_exact(&mut buf[..]).await.map_err(RecvError::IO)?;
        t.observe_duration();
        self.buf_size_metric.set(0);
        self.stats.received_messages.fetch_add(1, Ordering::Relaxed);
        self.stats.received_bytes.fetch_add(n as u64, Ordering::Relaxed);
        Ok(Frame(buf))
    }
}

pub(crate) struct Scope<A: actix::Actor> {
    pub arbiter: actix::ArbiterHandle,
    pub addr: actix::Addr<A>,
}

impl<A: actix::Actor> Clone for Scope<A> {
    fn clone(&self) -> Self {
        Self { arbiter: self.arbiter.clone(), addr: self.addr.clone() }
    }
}

impl<Actor: actix::Actor + actix::Handler<Error>> FramedWriter<Actor>
where
    Actor::Context: actix::dev::ToEnvelope<Actor, Error>,
{
    pub fn spawn(
        scope: &Scope<Actor>,
        peer_addr: SocketAddr,
        stream: tokio::net::TcpStream,
        stats: Arc<connection::Stats>,
    ) -> (Self, FramedReader) {
        let (tcp_recv, tcp_send) = tokio::io::split(stream);
        let (queue_send, queue_recv) = tokio::sync::mpsc::unbounded_channel();
        let send_buf_size_metric = Arc::new(metrics::MetricGuard::new(
            &*metrics::PEER_DATA_WRITE_BUFFER_SIZE,
            vec![peer_addr.to_string()],
        ));
        scope.arbiter.spawn({
            let scope = scope.clone();
            let stats = stats.clone();
            let m = send_buf_size_metric.clone();
            async move {
                if let Err(err) = Self::run_send_loop(tcp_send, queue_recv, stats, m).await {
                    scope.addr.do_send(Error::Send(SendError::IO(err)));
                }
            }
        });

        const READ_BUFFER_CAPACITY: usize = 8 * 1024;
        (
            FramedWriter {
                scope: scope.clone(),
                queue_send,
                stats: stats.clone(),
                buf_size_metric: send_buf_size_metric,
            },
            FramedReader {
                closed: false,
                stats,
                read: tokio::io::BufReader::with_capacity(READ_BUFFER_CAPACITY, tcp_recv),
                msg_size_metric: metrics::MetricGuard::new(
                    &metrics::PEER_MSG_SIZE_BYTES,
                    vec![peer_addr.to_string()],
                ),
                buf_size_metric: metrics::MetricGuard::new(
                    &metrics::PEER_DATA_READ_BUFFER_SIZE,
                    vec![peer_addr.to_string()],
                ),
            },
        )
    }

    /// Pushes `msg` to the send queue.
    /// Silently drops message if the connection has been closed.
    /// If the message is too large, it will be silently dropped inside run_send_loop.
    /// Emits a critical error to Actor if send queue is full.
    pub fn send(&self, frame: Frame) {
        let msg = &frame.0;
        let mut buf_size =
            self.stats.bytes_to_send.fetch_add(msg.len() as u64, Ordering::Acquire) as usize;
        buf_size += msg.len();
        self.stats.messages_to_send.fetch_add(1, Ordering::Acquire);
        self.buf_size_metric.add(msg.len() as i64);
        // Exceeding buffer capacity is a critical error and Actor should call ctx.stop()
        // when receiving one. It is not like we do any extra allocations, so we can affort
        // pushing the message to the queue anyway.
        if buf_size > MAX_WRITE_BUFFER_CAPACITY_BYTES {
            metrics::MessageDropped::MaxCapacityExceeded.inc_unknown_msg();
            self.scope.addr.do_send(Error::Send(SendError::QueueOverflow {
                got_bytes: buf_size,
                want_max_bytes: MAX_WRITE_BUFFER_CAPACITY_BYTES,
            }));
        }
        let _ = self.queue_send.send(frame);
    }

    async fn run_send_loop(
        tcp_send: WriteHalf,
        mut queue_recv: tokio::sync::mpsc::UnboundedReceiver<Frame>,
        stats: Arc<connection::Stats>,
        buf_size_metric: Arc<metrics::IntGaugeGuard>,
    ) -> io::Result<()> {
        const WRITE_BUFFER_CAPACITY: usize = 8 * 1024;
        let mut writer = tokio::io::BufWriter::with_capacity(WRITE_BUFFER_CAPACITY, tcp_send);
        while let Some(Frame(mut msg)) = queue_recv.recv().await {
            // Try writing a batch of messages and flush once at the end.
            loop {
                writer.write_u32_le(msg.len() as u32).await?;
                writer.write_all(&msg[..]).await?;
                stats.messages_to_send.fetch_sub(1, Ordering::Release);
                stats.bytes_to_send.fetch_sub(msg.len() as u64, Ordering::Release);
                buf_size_metric.sub(msg.len() as i64);
                msg = match queue_recv.try_recv() {
                    Ok(Frame(it)) => it,
                    Err(_) => break,
                };
            }
            // This is an unconditional flush, which means that even if new messages
            // will be added to the queue in the meantime, we will wait for the buffer
            // to be flushed before sending them. This is suboptimal in case messages are small
            // and added to the queue at a rate similar to flush latency. To fix that
            // we would need to put writer.flush() and queue_recv.recv() into a tokio::select
            // and make sure that both are cancellation-safe.
            writer.flush().await?;
        }
        Ok(())
    }
}
