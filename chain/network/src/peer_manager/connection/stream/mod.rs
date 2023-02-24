#![allow(dead_code)]
use crate::concurrency::ctx;
use crate::concurrency::scope;
use crate::tcp;
use std::sync::Arc;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

pub(crate) mod stats;
type ReadHalf = tokio::io::BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>;
type WriteHalf = tokio::io::BufWriter<tokio::io::WriteHalf<tokio::net::TcpStream>>;

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct Frame(pub Vec<u8>);

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Canceled(#[from] ctx::ErrCanceled),
    #[error(transparent)]
    Stats(#[from] stats::Error),
    #[error("Stream has been closed")]
    StreamClosed,
}

pub struct FrameStream {
    service: scope::Service<Error>,
    stats: Arc<stats::Stats>,
    flusher: tokio::sync::Notify,
    send: Arc<tokio::sync::Mutex<Option<WriteHalf>>>,
    recv: Arc<tokio::sync::Mutex<Option<ReadHalf>>>,
}

impl FrameStream {
    fn new(
        service: scope::Service<Error>,
        stream: tcp::Stream,
    ) -> anyhow::Result<Arc<FrameStream>> {
        let (tcp_recv, tcp_send) = tokio::io::split(stream.stream);

        const READ_BUFFER_CAPACITY: usize = 8 * 1024;
        const WRITE_BUFFER_CAPACITY: usize = 8 * 1024;
        let s = Arc::new(Self {
            service,
            stats: Arc::new(stats::Stats::new(&stream.peer_addr)),
            flusher: tokio::sync::Notify::new(),
            send: Arc::new(tokio::sync::Mutex::new(Some(WriteHalf::with_capacity(
                WRITE_BUFFER_CAPACITY,
                tcp_send,
            )))),
            recv: Arc::new(tokio::sync::Mutex::new(Some(ReadHalf::with_capacity(
                READ_BUFFER_CAPACITY,
                tcp_recv,
            )))),
        });

        // Subtask flushing the sending buffer.
        let self_ = s.clone();
        s.service.spawn::<()>(async move {
            loop {
                ctx::wait(self_.flusher.notified()).await?;
                let mut send = ctx::wait(self_.send.lock()).await?;
                let mut inner = send.take().ok_or(Error::StreamClosed)?;
                ctx::wait(inner.flush()).await??;
                // If loop iteration exits early, the stream becomes broken.
                *send = Some(inner);
            }
        })?;
        Ok(s)
    }

    /// Sends a frame over the TCP connection.
    /// If error is returned, the message may or may not have been sent.
    async fn send(self: &Arc<Self>, frame: Frame) -> anyhow::Result<()> {
        let guard = stats::SendGuard::new(self.stats.clone(), frame.0.len())?;
        let mut send = ctx::wait(self.send.clone().lock_owned()).await?;
        let mut inner = send.take().ok_or(Error::StreamClosed)?;
        let self_ = self.clone();
        Ok(self
            .service
            .spawn(ctx::run_with_timeout(time::Duration::minutes(1), async move {
                let _guard = guard;
                ctx::wait(inner.write_u32_le(frame.0.len() as u32)).await??;
                ctx::wait(inner.write_all(&frame.0[..])).await??;
                self_.flusher.notify_one();
                // If this task exits early, the stream becomes broken.
                *send = Some(inner);
                Ok(())
            }))?
            .join()
            .await??)
    }

    /// Receives a frame from the TCP connection.
    /// If error is returned, a message may or may not been received.
    // TODO(gprusak): partially received message will be still received in the background and
    // then dropped. If that semantics is not OK, we can use tokio::sync::mpsc channel instead.
    // reserve_owned() function will allow us to avoid receiving messages that are not awaited yet.
    async fn recv(self: &Arc<Self>) -> anyhow::Result<Frame> {
        let mut recv = ctx::wait(self.recv.clone().lock_owned()).await?;
        let mut inner = recv.take().ok_or(Error::StreamClosed)?;
        let stats = self.stats.clone();
        Ok(self
            .service
            .spawn(async move {
                let n = ctx::wait(inner.read_u32_le()).await?? as usize;
                let _guard = stats::RecvGuard::new(stats, n)?;
                let mut buf = vec![0; n];
                // Timeout on message transmission.
                ctx::run_with_timeout(
                    time::Duration::minutes(1),
                    ctx::wait(inner.read_exact(&mut buf[..])),
                )
                .await??;
                // If loop iteration exits early, the stream becomes broken.
                *recv = Some(inner);
                Ok(Frame(buf))
            })?
            .join()
            .await??)
    }
}
