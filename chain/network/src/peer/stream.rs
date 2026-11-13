use crate::concurrency::outgoing_queue_limiter::OutgoingPermit;
use crate::network_protocol::{Encoding, PeerMessageFrames, PeerMessageHeader, PeerMessageKind};
use crate::peer_manager::connection;
use crate::recv_permit::RecvMessagePermit;
use crate::stats::metrics;
use crate::tcp;
use borsh::BorshDeserialize;
use bytesize::{KIB, MIB};
use itertools::Itertools;
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{AsyncSender, Sender};
use near_o11y::log_assert_fail;
use near_o11y::metrics::Histogram;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Semaphore;

/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const NETWORK_MESSAGE_MAX_SIZE_BYTES: usize = 512 * MIB as usize;

/// Maximum size of a small frame: the first frame received on a connection and the header of any
/// borsh-encoded message.
pub(crate) const MAX_SMALL_FRAME_SIZE_BYTES: usize = 20 * KIB as usize;

/// Timeout for individual write operations (write + flush) to detect if the connection is
/// stuck due to a half-open TCP connection where the peer stopped ACKing writes.
const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(2);

/// Timeout for receiving a network message. Same as the write timeout.
const READ_TIMEOUT: std::time::Duration = WRITE_TIMEOUT;

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
    #[error("incoming_semaphore has been closed; cannot acquire recv_permit")]
    IncomingSemaphoreClosed,
    #[error("network protocol violation: {0}")]
    NetworkProtocolViolation(#[from] NetworkProtocolViolation),
}

/// Violations of the wire format by the peer, see `FramedStream::run_recv_loop`.
#[derive(thiserror::Error, Debug)]
pub(crate) enum NetworkProtocolViolation {
    #[error("message too large: got {got_bytes}B, want <={want_max_bytes}B")]
    MessageTooLarge { got_bytes: usize, want_max_bytes: usize },
    #[error("frame too large: got {got_bytes}B, want <={want_max_bytes}B")]
    FrameTooLarge { got_bytes: usize, want_max_bytes: usize },
    #[error("invalid header: {0}")]
    InvalidHeader(#[source] std::io::Error),
    #[error("missing header for a {0:?} message")]
    MissingHeader(PeerMessageKind),
    #[error("header kind {header:?} doesn't match the body kind {body:?}")]
    KindMismatch { header: PeerMessageKind, body: PeerMessageKind },
    #[error("body of a {kind:?} message too large: got {got_bytes}B, want <={want_max_bytes}B")]
    BodyTooLarge { kind: PeerMessageKind, got_bytes: usize, want_max_bytes: usize },
}

pub(crate) struct Frame {
    pub message: PeerMessageFrames,
    _permit: Option<OutgoingPermit>,
}

impl Frame {
    pub fn with_permit(message: PeerMessageFrames, permit: OutgoingPermit) -> Self {
        Self { message, _permit: Some(permit) }
    }
}

impl std::fmt::Debug for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Frame").field("bytes_len", &self.message.len()).finish()
    }
}

impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}

impl Eq for Frame {}

pub(crate) struct IncomingFrame {
    pub message: PeerMessageFrames,
    pub recv_permit: RecvMessagePermit,
}

impl std::fmt::Debug for IncomingFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingFrame")
            .field("header", &self.message.header)
            .field("body_prefix", &&self.message.body.iter().take(32).collect_vec())
            .field("body_len", &self.message.body.len())
            .finish()
    }
}

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
    /// Maximum capacity of the write buffer in bytes.
    max_write_buffer_capacity_bytes: usize,
}

impl FramedStream {
    pub fn spawn(
        error_sender: Sender<Error>,
        frame_sender: AsyncSender<IncomingFrame, ()>,
        future_spawner: &dyn FutureSpawner,
        stream: tcp::Stream,
        stats: Arc<connection::Stats>,
        incoming_message_semaphore: Arc<Semaphore>,
        accepted_encodings: &'static [Encoding],
        max_write_buffer_capacity_bytes: usize,
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
                if let Err(err) = Self::run_recv_loop(
                    stream.peer_addr,
                    tcp_recv,
                    frame_sender,
                    stats,
                    incoming_message_semaphore,
                    accepted_encodings,
                    READ_TIMEOUT,
                )
                .await
                {
                    error_sender.send(Error::Recv(err));
                }
            }
        });
        Self {
            queue_send,
            stats,
            send_buf_size_metric,
            error_sender,
            max_write_buffer_capacity_bytes,
        }
    }

    /// Pushes `msg` to the send queue.
    /// Silently drops message if the connection has been closed.
    /// If the message is too large, it will be silently dropped inside run_send_loop.
    /// Emits a critical error to Actor if send queue is full.
    pub fn send(&self, frame: Frame) {
        let msg_len = frame.message.len();
        let mut buf_size =
            self.stats.bytes_to_send.fetch_add(msg_len as u64, Ordering::Acquire) as usize;
        buf_size += msg_len;
        self.stats.messages_to_send.fetch_add(1, Ordering::Acquire);
        self.send_buf_size_metric.add(msg_len as i64);
        // Exceeding buffer capacity is a critical error and Actor should call ctx.stop()
        // when receiving one. It is not like we do any extra allocations, so we can afford
        // pushing the message to the queue anyway.
        if buf_size > self.max_write_buffer_capacity_bytes {
            metrics::MessageDropped::MaxCapacityExceeded.inc_unknown_msg();
            self.error_sender.send(Error::Send(SendError::QueueOverflow {
                got_bytes: buf_size,
                want_max_bytes: self.max_write_buffer_capacity_bytes,
            }));
        }
        let _ = self.queue_send.send(frame);
    }

    /// Event loop receiving and processing messages.
    ///
    /// The first frame received from the peer determines the encoding of the connection: if it
    /// parses as a borsh header, all the messages are borsh-encoded (see `RecvLoop::run_borsh`),
    /// otherwise it is treated as a protobuf-encoded message (see `RecvLoop::run_proto`).
    ///
    /// A node which doesn't accept borsh (e.g. 2.14, or a node with ConnectionEncoding::ProtobufOnly)
    /// interprets all messages as protobuf. Such nodes handle borsh messages as invalid unparsable
    /// ones and ignore them.
    async fn run_recv_loop(
        peer_addr: SocketAddr,
        read: ReadHalf,
        frame_sender: AsyncSender<IncomingFrame, ()>,
        stats: Arc<connection::Stats>,
        incoming_message_semaphore: Arc<Semaphore>,
        accepted_encodings: &'static [Encoding],
        read_timeout: std::time::Duration,
    ) -> Result<(), RecvError> {
        const READ_BUFFER_CAPACITY: usize = 8 * 1024;
        let mut recv = RecvLoop {
            read: tokio::io::BufReader::with_capacity(READ_BUFFER_CAPACITY, read),
            frame_sender,
            stats,
            incoming_message_semaphore,
            read_timeout,
            msg_size_metric: metrics::MetricGuard::new(
                &metrics::PEER_MSG_SIZE_BYTES,
                vec![peer_addr.to_string()],
            ),
            buf_size_metric: metrics::MetricGuard::new(
                &metrics::PEER_DATA_READ_BUFFER_SIZE,
                vec![peer_addr.to_string()],
            ),
        };
        if !accepted_encodings.contains(&Encoding::Borsh) {
            return recv.run_proto(None).await;
        }
        let frame = recv.read_small_frame().await?;
        match PeerMessageHeader::try_from_slice(&frame) {
            Ok(header) => recv.run_borsh(frame, header.kind()).await,
            // Anything which is not a borsh header is treated as a protobuf-encoded message.
            Err(err) => {
                if accepted_encodings.contains(&Encoding::Proto) {
                    recv.run_proto(Some(frame)).await
                } else {
                    Err(NetworkProtocolViolation::InvalidHeader(err).into())
                }
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
        while let Some(mut frame) = queue_recv.recv().await {
            // Try writing a batch of messages and flush once at the end.
            loop {
                let msg_len = frame.message.len();
                if frame.message.body.len() > NETWORK_MESSAGE_MAX_SIZE_BYTES {
                    metrics::MessageDropped::InputTooLong.inc_unknown_msg();
                    log_assert_fail!(
                        "dropping outgoing message of {} bytes exceeding the maximum network message size of {} bytes",
                        frame.message.body.len(),
                        NETWORK_MESSAGE_MAX_SIZE_BYTES,
                    );
                } else {
                    tokio::time::timeout(write_timeout, async {
                        // The header and the body are written together, so that a message is
                        // never split by another one.
                        if let Some(header) = &frame.message.header {
                            writer.write_u32_le(header.len() as u32).await?;
                            writer.write_all(header).await?;
                        }
                        writer.write_u32_le(frame.message.body.len() as u32).await?;
                        writer.write_all(&frame.message.body[..]).await?;
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
                stats.bytes_to_send.fetch_sub(msg_len as u64, Ordering::Release);
                buf_size_metric.sub(msg_len as i64);
                // Drop the previous frame (and its permit) before pulling the
                // next, so the limiter sees capacity return immediately.
                drop(frame);
                frame = match queue_recv.try_recv() {
                    Ok(f) => f,
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
                tracing::debug!(target: "network", timeout_secs = write_timeout.as_secs(), "flush timed out, closing half-open connection");
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "flush timed out, connection may be half-open",
                )
            })??;
        }
        Ok(())
    }
}

/// Receives messages from the peer, see `FramedStream::run_recv_loop`.
struct RecvLoop {
    read: tokio::io::BufReader<ReadHalf>,
    frame_sender: AsyncSender<IncomingFrame, ()>,
    stats: Arc<connection::Stats>,
    incoming_message_semaphore: Arc<Semaphore>,
    read_timeout: std::time::Duration,
    msg_size_metric: metrics::MetricGuard<Histogram>,
    buf_size_metric: metrics::IntGaugeGuard,
}

impl RecvLoop {
    /// Receives protobuf-encoded messages, each sent as a single frame. `first_frame` is the
    /// first message, if it has already been read.
    async fn run_proto(mut self, first_frame: Option<Vec<u8>>) -> Result<(), RecvError> {
        if let Some(body) = first_frame {
            let recv_permit =
                with_read_timeout(self.read_timeout, self.acquire_recv_permit(body.len())).await?;
            if !self.deliver(PeerMessageFrames::proto(body), recv_permit).await {
                return Ok(());
            }
        }
        loop {
            let n = self.read_frame_len().await?;
            let (body, recv_permit) = self.read_body_frame(n).await?;
            if !self.deliver(PeerMessageFrames::proto(body), recv_permit).await {
                return Ok(());
            }
        }
    }

    /// Receives borsh-encoded messages. Every message is sent as a header frame followed by a
    /// body frame. The header is read alone, and the size limit of the message kind is checked
    /// before the body is read. `first_header` is the header of the first message, which has
    /// already been read.
    ///
    /// Anything else, including a body which exceeds the size limit, is a network protocol
    /// violation.
    async fn run_borsh(
        mut self,
        first_header: Vec<u8>,
        first_kind: PeerMessageKind,
    ) -> Result<(), RecvError> {
        let mut pending_header = Some((first_header, first_kind));
        // The peer may follow its first handshake with the copies of it in the other encodings it
        // accepts, which are ignored.
        let mut skip_handshake_copy = true;
        loop {
            let (header, kind) = match pending_header.take() {
                Some(header) => header,
                None => self.read_header(&mut skip_handshake_copy).await?,
            };
            let body_len = self.read_frame_len().await?;
            let max_size = kind.max_size();
            if body_len > max_size {
                return Err(NetworkProtocolViolation::BodyTooLarge {
                    kind,
                    got_bytes: body_len,
                    want_max_bytes: max_size,
                }
                .into());
            }
            let (body, recv_permit) = self.read_body_frame(body_len).await?;
            let frames =
                PeerMessageFrames { encoding: Encoding::Borsh, header: Some(header), body };
            if !self.deliver(frames, recv_permit).await {
                return Ok(());
            }
        }
    }

    /// Reads the header of the next message. Any frame which is not a header is a network
    /// protocol violation, except for the single copy of the first handshake in another encoding,
    /// which `skip_handshake_copy` allows right after the first handshake.
    async fn read_header(
        &mut self,
        skip_handshake_copy: &mut bool,
    ) -> Result<(Vec<u8>, PeerMessageKind), RecvError> {
        loop {
            let frame = self.read_small_frame().await?;
            match PeerMessageHeader::try_from_slice(&frame) {
                Ok(header) => {
                    *skip_handshake_copy = false;
                    return Ok((frame, header.kind()));
                }
                Err(err) => {
                    // Right after the first handshake, the frame may be its copy in another
                    // encoding, which is ignored.
                    if *skip_handshake_copy {
                        *skip_handshake_copy = false;
                        continue;
                    }
                    return Err(NetworkProtocolViolation::InvalidHeader(err).into());
                }
            }
        }
    }

    /// Reads the length of the next frame.
    async fn read_frame_len(&mut self) -> Result<usize, RecvError> {
        let n_u32: u32 = self.read.read_u32_le().await.map_err(RecvError::IO)?;
        let n: usize = n_u32.try_into().expect("u32 must fit into usize");
        if n > NETWORK_MESSAGE_MAX_SIZE_BYTES {
            return Err(NetworkProtocolViolation::MessageTooLarge {
                got_bytes: n,
                want_max_bytes: NETWORK_MESSAGE_MAX_SIZE_BYTES,
            }
            .into());
        }
        Ok(n)
    }

    /// Reads a frame of at most `MAX_SMALL_FRAME_SIZE_BYTES` bytes, without recording it in the metrics.
    async fn read_small_frame(&mut self) -> Result<Vec<u8>, RecvError> {
        let n = self.read_frame_len().await?;
        if n > MAX_SMALL_FRAME_SIZE_BYTES {
            return Err(NetworkProtocolViolation::FrameTooLarge {
                got_bytes: n,
                want_max_bytes: MAX_SMALL_FRAME_SIZE_BYTES,
            }
            .into());
        }
        let mut frame = vec![0; n];
        with_read_timeout(self.read_timeout, async {
            self.read.read_exact(&mut frame).await.map_err(RecvError::IO)?;
            Ok(())
        })
        .await?;
        Ok(frame)
    }

    /// Acquires a permit for an incoming message of `n` bytes, before it is read into memory.
    async fn acquire_recv_permit(&self, n: usize) -> Result<RecvMessagePermit, RecvError> {
        let incoming_semaphore_permit = self
            .incoming_message_semaphore
            .clone()
            .acquire_many_owned(n as u32)
            .await
            .map_err(|_| RecvError::IncomingSemaphoreClosed)?;
        Ok(RecvMessagePermit::new(incoming_semaphore_permit))
    }

    /// Reads a frame of `n` bytes, which contains a message or its body.
    async fn read_body_frame(
        &mut self,
        n: usize,
    ) -> Result<(Vec<u8>, RecvMessagePermit), RecvError> {
        with_read_timeout(self.read_timeout, async {
            let recv_permit = self.acquire_recv_permit(n).await?;

            // Read the message from the socket
            self.msg_size_metric.observe(n as f64);
            self.buf_size_metric.set(n as i64);
            let mut buf = vec![0; n];
            let t = metrics::PEER_MSG_READ_LATENCY.start_timer();
            self.read.read_exact(&mut buf[..]).await.map_err(RecvError::IO)?;
            t.observe_duration();
            self.buf_size_metric.set(0);
            Ok((buf, recv_permit))
        })
        .await
    }

    /// Passes a message to the actor. Returns false if the actor has stopped, which means that
    /// the stream should be closed.
    async fn deliver(&self, message: PeerMessageFrames, recv_permit: RecvMessagePermit) -> bool {
        self.stats.received_messages.fetch_add(1, Ordering::Relaxed);
        self.stats.received_bytes.fetch_add(message.len() as u64, Ordering::Relaxed);
        self.frame_sender.send_async(IncomingFrame { message, recv_permit }).await.is_ok()
    }
}

/// Runs `fut`, failing with a `TimedOut` error if it takes longer than `read_timeout`.
async fn with_read_timeout<T>(
    read_timeout: std::time::Duration,
    fut: impl Future<Output = Result<T, RecvError>>,
) -> Result<T, RecvError> {
    tokio::time::timeout(read_timeout, fut).await.map_err(|_| {
        tracing::debug!(target: "network", timeout_secs = read_timeout.as_secs(), "read timed out");
        RecvError::IO(io::Error::new(io::ErrorKind::TimedOut, "read timed out"))
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network_protocol::{PeerMessage, PeersRequest};
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

        // Enqueue enough large borsh messages to fill the TCP send buffer. The send loop treats
        // the frames as opaque, so only the header needs to be a valid borsh header.
        let header =
            PeerMessage::PeersRequest(PeersRequest { max_peers: None, max_direct_peers: None })
                .serialize_with_encoding(Encoding::Borsh)
                .header;
        for _ in 0..64 {
            let message = PeerMessageFrames {
                encoding: Encoding::Borsh,
                header: header.clone(),
                body: vec![0u8; 1024 * 1024],
            };
            let _ = queue_send.send(Frame::with_permit(message, OutgoingPermit::fake_for_test()));
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
