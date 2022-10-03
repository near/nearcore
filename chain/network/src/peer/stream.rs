use crate::peer_manager::connection;
use crate::stats::metrics;
use crate::tcp;
use actix::fut::future::wrap_future;
use actix::AsyncContext as _;
use bytesize::{GIB, MIB};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const NETWORK_MESSAGE_MAX_SIZE_BYTES: usize = 512 * MIB as usize;
/// Maximum capacity of write buffer in bytes.
const MAX_WRITE_BUFFER_CAPACITY_BYTES: usize = GIB as usize;

type ReadHalf = tokio::io::ReadHalf<tokio::net::TcpStream>;
type WriteHalf = tokio::io::WriteHalf<tokio::net::TcpStream>;

#[derive(thiserror::Error, Debug)]
pub(crate) enum SendError {
    #[error("IO error: {0}")]
    IO(#[source] io::Error),
    #[error("queue is full, got {got_bytes}B, max capacity is {want_max_bytes}")]
    QueueOverflow { got_bytes: usize, want_max_bytes: usize },
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RecvError {
    #[error("IO error")]
    IO(#[source] io::Error),
    #[error("message too large: got {got_bytes}B, want <={want_max_bytes}B")]
    MessageTooLarge { got_bytes: usize, want_max_bytes: usize },
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
    #[error("send: {0}")]
    Send(#[source] SendError),
    #[error("recv: {0}")]
    Recv(#[source] RecvError),
}

pub(crate) struct FramedStream<Actor: actix::Actor> {
    queue_send: tokio::sync::mpsc::UnboundedSender<Frame>,
    stats: Arc<connection::Stats>,
    send_buf_size_metric: Arc<metrics::IntGaugeGuard>,
    addr: actix::Addr<Actor>,
}

impl<Actor> FramedStream<Actor>
where
    Actor: actix::Actor<Context = actix::Context<Actor>>
        + actix::Handler<Error>
        + actix::Handler<Frame>,
{
    pub fn spawn(
        ctx: &mut actix::Context<Actor>,
        stream: tcp::Stream,
        stats: Arc<connection::Stats>,
    ) -> Self {
        let (tcp_recv, tcp_send) = tokio::io::split(stream.stream);
        let (queue_send, queue_recv) = tokio::sync::mpsc::unbounded_channel();
        let send_buf_size_metric = Arc::new(metrics::MetricGuard::new(
            &*metrics::PEER_DATA_WRITE_BUFFER_SIZE,
            vec![stream.peer_addr.to_string()],
        ));
        ctx.spawn(wrap_future({
            let addr = ctx.address();
            let stats = stats.clone();
            let m = send_buf_size_metric.clone();
            async move {
                if let Err(err) = Self::run_send_loop(tcp_send, queue_recv, stats, m).await {
                    addr.do_send(Error::Send(SendError::IO(err)));
                }
            }
        }));
        ctx.spawn(wrap_future({
            let addr = ctx.address();
            let stats = stats.clone();
            async move {
                if let Err(err) =
                    Self::run_recv_loop(stream.peer_addr, tcp_recv, addr.clone(), stats).await
                {
                    addr.do_send(Error::Recv(err));
                }
            }
        }));
        Self { queue_send, stats, send_buf_size_metric, addr: ctx.address() }
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
        self.send_buf_size_metric.add(msg.len() as i64);
        // Exceeding buffer capacity is a critical error and Actor should call ctx.stop()
        // when receiving one. It is not like we do any extra allocations, so we can affort
        // pushing the message to the queue anyway.
        if buf_size > MAX_WRITE_BUFFER_CAPACITY_BYTES {
            metrics::MessageDropped::MaxCapacityExceeded.inc_unknown_msg();
            self.addr.do_send(Error::Send(SendError::QueueOverflow {
                got_bytes: buf_size,
                want_max_bytes: MAX_WRITE_BUFFER_CAPACITY_BYTES,
            }));
        }
        let _ = self.queue_send.send(frame);
    }

    /// Event loop receiving and processing messages.
    /// Loop waits for the message to be processed before reading the next message.
    /// Note that if the message handler spawns an asynchronous subhandler and returns,
    /// then the loop will start reading the next message before the subhandler returns.
    /// Loop uses a fixed small buffer allocated by BufReader.
    /// For each message it allocates a Vec with exact size of the message.
    // TODO(gprusak): once borsh support is dropped, we can parse a proto
    // directly from the stream.
    async fn run_recv_loop(
        peer_addr: SocketAddr,
        read: ReadHalf,
        addr: actix::Addr<Actor>,
        stats: Arc<connection::Stats>,
    ) -> Result<(), RecvError> {
        const READ_BUFFER_CAPACITY: usize = 8 * 1024;
        let mut read = tokio::io::BufReader::with_capacity(READ_BUFFER_CAPACITY, read);

        let msg_size_metric =
            metrics::MetricGuard::new(&metrics::PEER_MSG_SIZE_BYTES, vec![peer_addr.to_string()]);
        let buf_size_metric = metrics::MetricGuard::new(
            &metrics::PEER_DATA_READ_BUFFER_SIZE,
            vec![peer_addr.to_string()],
        );
        loop {
            let n = read.read_u32_le().await.map_err(RecvError::IO)? as usize;
            if n > NETWORK_MESSAGE_MAX_SIZE_BYTES {
                return Err(RecvError::MessageTooLarge {
                    got_bytes: n,
                    want_max_bytes: NETWORK_MESSAGE_MAX_SIZE_BYTES,
                });
            }
            msg_size_metric.observe(n as f64);
            buf_size_metric.set(n as i64);
            let mut buf = vec![0; n];
            let t = metrics::PEER_MSG_READ_LATENCY.start_timer();
            read.read_exact(&mut buf[..]).await.map_err(RecvError::IO)?;
            t.observe_duration();
            buf_size_metric.set(0);
            stats.received_messages.fetch_add(1, Ordering::Relaxed);
            stats.received_bytes.fetch_add(n as u64, Ordering::Relaxed);
            if let Err(_) = addr.send(Frame(buf)).await {
                // We got mailbox error, which means that Actor has stopped,
                // so we should just close the stream.
                return Ok(());
            }
        }
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
                // TODO(gprusak): sending a too large message should probably be treated as a bug,
                // since dropping messages may lead to hard-to-debug high-level issues.
                if msg.len() > NETWORK_MESSAGE_MAX_SIZE_BYTES {
                    metrics::MessageDropped::InputTooLong.inc_unknown_msg();
                } else {
                    writer.write_u32_le(msg.len() as u32).await?;
                    writer.write_all(&msg[..]).await?;
                }
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
