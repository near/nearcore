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

type ReadHalf = tokio::io::BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>;
type WriteHalf = tokio::io::BufWriter<tokio::io::WriteHalf<tokio::net::TcpStream>>;

#[derive(thiserror::Error, Debug)]
#[error("queue is full, got {got_bytes}B, max capacity is {want_max_bytes}")]
pub(crate) struct ErrSendQueueFull { got_bytes: usize, want_max_bytes: usize }

#[derive(thiserror::Error, Debug)]
#[error("message too large: got {got_bytes}B, want <={want_max_bytes}B")]
pub(crate) struct ErrMessageTooLarge { got_bytes: usize, want_max_bytes: usize }

#[derive(thiserror::Error, Debug)]
#[error("ErrStreamClosed")]
pub(crate) struct ErrStreamClosed { got_bytes: usize, want_max_bytes: usize }

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct Frame(pub Vec<u8>);

pub struct FrameStream{
    service: scope::Service,
    stats: Stats,
    flusher: tokio::Notify,
    send: tokio::sync::Mutex<Option<WriteHalf>>,
    recv: tokio::sync::Mutex<Option<ReadHalf>>,
}

impl FrameStream {
    fn new(service: scope::Service, stream: tcp::Stream) -> anyhow::Result<FrameStream> {
        let (tcp_recv, tcp_send) = tokio::io::split(stream.stream);

        const READ_BUFFER_CAPACITY: usize = 8 * 1024;
        const WRITE_BUFFER_CAPACITY: usize = 8 * 1024;
        let s = Arc::new(Self {
            service,
            stats: Stats::new(stream.peer_addr),
            flusher: tokio::Notify::new(),
            send: tokio::sync::Mutex::new(tokio::io::BufWriter::with_capacity(WRITE_BUFFER_CAPACITY, tcp_send)),
            recv: tokio::sync::Mutex::new(tokio::io::BufReader::with_capacity(READ_BUFFER_CAPACITY, tcp_recv)),
        });

        /// Subtask flushing the sending buffer.
        let self_ = s.clone();
        s.service.spawn(async move {
            loop {
                ctx::wait(self_.flusher.notified()).await?;
                let mut send = ctx::wait(self_.send.lock()).await?;
                let inner = send.take().ok_or(ErrStreamClosed)?;
                ctx::wait(inner.flush()).await??;
                *send = inner;
            }
        })?;
        s
    }

    /// Sends a frame over the TCP connection.
    /// If error is returned, the message may or may not have been sent.
    async fn send(self: &Arc<Self>, frame: Frame) -> anyhow::Result<()> {
        let guard = SendGuard::new(self.clone(),frame.0.len())?; 
        let mut send = ctx::wait(self.send.lock()).await?;
        let inner = send.take().ok_or(ErrStreamClosed)?;
        let self_ = self.clone();
        self.service.spawn(ctx::run_with_timeout(time::Duration::minutes(1), async move {
            let guard = guard;
            ctx::wait(inner.write_u32_le(frame.0.len() as u32)).await??;
            ctx::wait(inner.write_all(&frame.0[..])).await??;
            self_.flusher.notify_one();
            *send = Some(inner);
            Ok(())
        }))?.await?
    }
    
    /// Receives a frame from the TCP connection.
    /// If error is returned, a message may or may not been received.
    // TODO(gprusak): partially received message will be still received in the background and
    // then dropped. If that semantics is not OK, we can use tokio::sync::mpsc channel instead.
    // reserve_owned() function will allow us to avoid receiving messages that are not awaited yet.
    async fn recv(self: &Arc<Self>) -> anyhow::Result<Frame> {
        let recv = ctx::wait(self.recv.lock()).await?;
        let inner = recv.take().ok_or(ErrStreamClosed)?;
        let self_ = self.clone();
        self.service.spawn(async move {
            let n = ctx::wait(inner.read_u32_le()).await?? as usize;
            let guard = RecvGuard::new(self_,n)?;
            let mut buf = vec![0; n];
            // Timeout on message transmission.
            ctx::with_timeout(
                time::Duration::minutes(1),
                ctx::wait(inner.read_exact(&mut buf[..])),
            ).await??;
            *recv = inner;
            Ok(Frame(buf))
        })?.await?
    } 
}

#[derive(Default)]
pub(crate) struct Stats {
    /// Number of messages received since the last reset of the counter.
    pub received_messages: AtomicU64,
    /// Number of bytes received since the last reset of the counter.
    pub received_bytes: AtomicU64,
    /// Avg received bytes/s, based on the last few minutes of traffic.
    pub received_bytes_per_sec: AtomicU64,
    /// Avg sent bytes/s, based on the last few minutes of traffic.
    pub sent_bytes_per_sec: AtomicU64,

    /// Number of messages in the buffer to send.
    pub messages_to_send: AtomicU64,
    /// Number of bytes (sum of message sizes) in the buffer to send.
    pub bytes_to_send: AtomicU64,

    /// metrics
    send_buf_size_metric: metrics::IntGaugeGuard,
    recv_msg_size_metric: metrics::MetricGuard,
    recv_buf_size_metric: metrics::MetricGuard,

    /// Bytes we've sent.
    sent_bytes: TransferStats,
    /// Bytes we've received.
    received_bytes: TransferStats,
}

impl Stats {
    fn new(peer_addr: &PeerAddr) -> Self {
        let labels = vec![peer_addr.to_string()];
        Self {
            send_buf_size_metric: metrics::MetricGuard::new(&*metrics::PEER_DATA_WRITE_BUFFER_SIZE, labels.clone()),
            recv_msg_size_metric: metrics::MetricGuard::new(&metrics::PEER_MSG_SIZE_BYTES, labels.clone()),
            recv_buf_size_metric: metrics::MetricGuard::new(&metrics::PEER_DATA_READ_BUFFER_SIZE, labels.clone()),
        }
    }
}

struct SendGuard {
    stream: Arc<FrameStream>,
    frame_size: usize,
}

impl SendGuard {
    fn new(stream: &Arc<FrameStream>, frame_size: usize) -> anyhow::Result<SendGuard> {
        if frame_size > NETWORK_MESSAGE_MAX_SIZE_BYTES {
            metrics::MessageDropped::InputTooLong.inc_unknown_msg();
            return Err(ErrMessageTooLarge);
        }
        let buf_size = self.stats.bytes_to_send.fetch_add(frame_size, Ordering::Relaxed) + frame_size;
        if buf_size.total() > MAX_WRITE_BUFFER_CAPACITY_BYTES {
            metrics::MessageDropped::MaxCapacityExceeded.inc_unknown_msg();
            self.stats.bytes_to_send.fetch_sub(frame_size, Ordering::Relaxed);
            return Err(ErrSendQueueFull);
        }
        self.stats.messages_to_send.fetch_add(1, Ordering::Relaxed);
        self.buf_size_metric.add(frame_size); 
    }
}

impl Drop for SendGuard {
    fn drop(&mut self) {
        self.buf_size_metric.sub(self.frame_size);
        self.stats.messages_to_send.fetch_sub(1, Ordering::Relaxed);
        self.stats.bytes_to_send.fetch_sub(self.frame_size, Ordering::Relaxed);
    }
}

struct RecvGuard {
    stream: Arc<FrameStream>,
    start_time: time::Instant,
    frame_size: usize,
}

impl RecvGuard {
    fn new(stream: &Arc<FrameStream>, frame_size: usize) -> anyhow::Result<RecvGuard> {
        if n > NETWORK_MESSAGE_MAX_SIZE_BYTES {
            return Err(RecvError::MessageTooLarge {
                got_bytes: frame_size,
                want_max_bytes: NETWORK_MESSAGE_MAX_SIZE_BYTES,
            });
        } 
        self.stats.recv_buf_size_metric.set(frame_size as i64);
        Self {
            stream: stream.clone(),
            start_time: ctx::time::now(),
            frame_size,
        }
    }
}

impl Drop for RecvGuard {
    fn drop(&mut self) {
        metrics::PEER_MSG_READ_LATENCY.observe((ctx::time::now()-self.start_timer).as_secs_f64());
        self.stats.recv_buf_size_metric.set(0);
        self.stats.recv_msg_size_metric.observe(self.frame_size as u64);
        self.stats.received_messages.fetch_add(1, Ordering::Relaxed);
        self.stats.received_bytes.fetch_add(self.frame_size as u64, Ordering::Relaxed);
    }
}
