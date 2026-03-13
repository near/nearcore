use crate::peer_manager::connection;
use crate::stats::metrics;
use crate::tcp;
use bytesize::{GIB, MIB};
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{AsyncSender, Sender};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const NETWORK_MESSAGE_MAX_SIZE_BYTES: usize = 512 * MIB as usize;
/// Maximum capacity of write buffer in bytes.
const MAX_WRITE_BUFFER_CAPACITY_BYTES: usize = GIB as usize;

/// Timeout for individual write operations (write + flush) to detect if the connection is
/// stuck due to a half-open TCP connection where the peer stopped ACKing writes.
const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

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

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct Frame(pub Vec<u8>);

/// Stream critical error.
/// Actor is responsible for calling ctx.stop() after receiving stream::Error.
/// Actor might receive more than 1 stream::Error, but should call ctx.stop() just after the
/// first one.
/// WARNING: send/recv loops might not get closed if Actor won't call ctx.stop()!.
// TODO(gprusak): once we implement cancellation support for structured concurrency,
// send/recv loops should be cancelled and return before the error is reported to Actor.
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("send: {0}")]
    Send(#[source] SendError),
    #[error("recv: {0}")]
    Recv(#[source] RecvError),
}

pub(crate) struct FramedStream {
    queue_send: tokio::sync::mpsc::UnboundedSender<Frame>,
    stats: Arc<connection::Stats>,
    send_buf_size_metric: Arc<metrics::IntGaugeGuard>,
    /// Sender to send the error to the PeerActor.
    error_sender: Sender<Error>,
}

impl FramedStream {
    pub fn spawn(
        error_sender: Sender<Error>,
        frame_sender: AsyncSender<Frame, ()>,
        future_spawner: &dyn FutureSpawner,
        stream: tcp::Stream,
        stats: Arc<connection::Stats>,
    ) -> Self {
        let (tcp_recv, tcp_send) = tokio::io::split(stream.stream);
        let (queue_send, queue_recv) = tokio::sync::mpsc::unbounded_channel();
        let send_buf_size_metric = Arc::new(metrics::MetricGuard::new(
            &*metrics::PEER_DATA_WRITE_BUFFER_SIZE,
            vec![stream.peer_addr.to_string()],
        ));
        future_spawner.spawn("run_send_loop", {
            let stats = stats.clone();
            let error_sender = error_sender.clone();
            let m = send_buf_size_metric.clone();
            async move {
                if let Err(err) =
                    Self::run_send_loop(tcp_send, queue_recv, stats, m, WRITE_TIMEOUT).await
                {
                    error_sender.send(Error::Send(SendError::IO(err)));
                }
            }
        });
        future_spawner.spawn("run_recv_loop", {
            let error_sender = error_sender.clone();
            let stats = stats.clone();
            async move {
                if let Err(err) =
                    Self::run_recv_loop(stream.peer_addr, tcp_recv, frame_sender, stats).await
                {
                    error_sender.send(Error::Recv(err));
                }
            }
        });
        Self { queue_send, stats, send_buf_size_metric, error_sender }
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
        // when receiving one. It is not like we do any extra allocations, so we can afford
        // pushing the message to the queue anyway.
        if buf_size > MAX_WRITE_BUFFER_CAPACITY_BYTES {
            metrics::MessageDropped::MaxCapacityExceeded.inc_unknown_msg();
            self.error_sender.send(Error::Send(SendError::QueueOverflow {
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
        frame_sender: AsyncSender<Frame, ()>,
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
            if let Err(_) = frame_sender.send_async(Frame(buf)).await {
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
        write_timeout: std::time::Duration,
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
                    tokio::time::timeout(write_timeout, async {
                        writer.write_u32_le(msg.len() as u32).await?;
                        writer.write_all(&msg[..]).await?;
                        io::Result::Ok(())
                    })
                    .await
                    .map_err(|_| {
                        tracing::debug!(target: "network", timeout_secs = write_timeout.as_secs(), "write timed out, closing half-open connection");
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "write timed out, connection may be half-open",
                        )
                    })??;
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
            tokio::time::timeout(write_timeout, writer.flush()).await.map_err(|_| {
                tracing::warn!(target: "network", timeout_secs = write_timeout.as_secs(), "flush timed out, closing half-open connection");
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "flush timed out, connection may be half-open",
                )
            })??;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::metrics;

    /// Simulates a half-open TCP connection by holding the receiver socket open without reading.
    /// The sender's TCP buffer fills up, writes block, and the write timeout fires.
    /// Uses run_send_loop directly with a short timeout (5s) to keep the test fast.
    #[tokio::test]
    async fn write_timeout_on_half_open_connection() {
        near_o11y::testonly::init_test_logger();
        let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, server) =
            tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept(),);
        let client = client.unwrap();
        // Hold the server socket open without reading — simulates a process that crashed
        // without sending FIN (e.g. killed with SIGKILL, or network partition).
        let _server = server.unwrap().0;

        let (_, tcp_send) = tokio::io::split(client);
        let (queue_send, queue_recv) = tokio::sync::mpsc::unbounded_channel();
        let stats: Arc<connection::Stats> = Arc::default();
        let buf_size_metric = Arc::new(metrics::MetricGuard::new(
            &*metrics::PEER_DATA_WRITE_BUFFER_SIZE,
            vec!["test_half_open".to_string()],
        ));

        // Enqueue enough large messages to fill the TCP send buffer.
        for _ in 0..64 {
            let _ = queue_send.send(Frame(vec![0u8; 1024 * 1024]));
        }
        // Close the sender so run_send_loop will drain the queue and exit.
        drop(queue_send);

        // Use a short write timeout (5s) to keep the test fast.
        let write_timeout = std::time::Duration::from_secs(5);
        let result = FramedStream::run_send_loop(
            tcp_send,
            queue_recv,
            stats,
            buf_size_metric,
            write_timeout,
        )
        .await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }
}
