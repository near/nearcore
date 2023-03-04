//! All the low level stream operations in this module invalidate the stream if they fail.
//! or_close allows to prevent further use after such a failure.
//! An example of important cases for preventing further use:
//! * we read/write a part of a frame, then encounter an error
//! * reading/writing the next frame may lead to unexpected errors which might be hard to
//!  diagnose.
#![allow(dead_code)]
use crate::concurrency::ctx;
use crate::concurrency::scope;
use crate::tcp;
use std::sync::Arc;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

pub(crate) mod stats;

#[cfg(test)]
mod tests;

/// If sending/receiving/flushing takes more than `RESPONSIVENESS_TIMEOUT`, we consider the stream broken.
const RESPONSIVENESS_TIMEOUT: time::Duration = time::Duration::minutes(1);

type SendHalf = tokio::io::BufWriter<tokio::io::WriteHalf<tokio::net::TcpStream>>;
type RecvHalf = tokio::io::BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>;

#[derive(thiserror::Error, Clone, Debug)]
pub(crate) enum Error {
    #[error(transparent)]
    IO(Arc<std::io::Error>),
    #[error(transparent)]
    Canceled(#[from] ctx::ErrCanceled),
    #[error(transparent)]
    Terminated(#[from] scope::ErrTerminated),
    #[error(transparent)]
    Stats(#[from] stats::Error),
    #[error("Stream has been closed")]
    StreamClosed,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct Frame(pub Vec<u8>);

pub(crate) struct FrameSender {
    inner: Option<SendHalf>,
    stats: Arc<stats::Stats>,
}

impl FrameSender {
    /// Sends the frame synchronously. Closes the stream on failure.
    pub async fn send(&mut self, frame: &Frame) -> Result<(), Error> {
        let inner = self.inner.as_mut().ok_or(Error::StreamClosed)?;
        let res = ctx::run_with_timeout(RESPONSIVENESS_TIMEOUT, async {
            let _guard = stats::SendGuard::new(self.stats.clone(), frame.0.len())?;
            ctx::wait(inner.write_u32_le(frame.0.len() as u32)).await??;
            ctx::wait(inner.write_all(&frame.0[..])).await??;
            ctx::wait(inner.flush()).await??;
            Ok(())
        })
        .await;
        if res.is_err() {
            self.inner.take();
        }
        res
    }
}

pub(crate) struct FrameReceiver {
    inner: Option<RecvHalf>,
    stats: Arc<stats::Stats>,
}

impl FrameReceiver {
    /// Receives a frame from the TCP connection. Closes the stream on error.
    pub async fn recv(&mut self) -> Result<Frame, Error> {
        let inner = self.inner.as_mut().ok_or(Error::StreamClosed)?;
        let res = async {
            // First we wait for the initial size bytes.
            let n = ctx::wait(inner.read_u32_le()).await?? as usize;
            // Then we expect the message to be received in a reasonable time.
            ctx::run_with_timeout(RESPONSIVENESS_TIMEOUT, async {
                let _guard = stats::RecvGuard::new(self.stats.clone(), n)?;
                let mut buf = vec![0; n];
                ctx::wait(inner.read_exact(&mut buf[..])).await??;
                Ok(Frame(buf))
            })
            .await
        }
        .await;
        if res.is_err() {
            self.inner.take();
        }
        res
    }
}

pub(crate) fn split(stream: tcp::Stream) -> (FrameSender, FrameReceiver) {
    let (tcp_recv, tcp_send) = tokio::io::split(stream.stream);
    const RECV_BUFFER_CAPACITY: usize = 8 * 1024;
    const SEND_BUFFER_CAPACITY: usize = 8 * 1024;
    let stats = Arc::new(stats::Stats::new(stream.info));
    let sender = FrameSender {
        inner: Some(SendHalf::with_capacity(SEND_BUFFER_CAPACITY, tcp_send)),
        stats: stats.clone(),
    };
    let receiver = FrameReceiver {
        inner: Some(RecvHalf::with_capacity(RECV_BUFFER_CAPACITY, tcp_recv)),
        stats: stats.clone(),
    };
    (sender, receiver)
}

// Manual implementation of the error casting, to wrap io::Error into a
// cloneable Arc.
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO(Arc::new(err))
    }
}

pub(crate) struct SharedFrameSender {
    /// To allow independent callers to send messages, we need a mutex.
    /// This mutex also acts as a FIFO queue for the messages to send.
    inner: Arc<tokio::sync::Mutex<Option<SendHalf>>>,
    /// Signal used to trigger background flushing of the send stream,
    /// as soon as the current batch of send calls is completed.
    flusher: tokio::sync::Notify,
    /// Statistics about the resource usage.
    stats: Arc<stats::Stats>,
}

/// SharedFrameSender
/// * sends the frame asynchronously (to avoid invalidating the stream),
///   in case the caller of SharedFrameSender::send cancels in the middle.
/// * flushes the constant size buffer asynchronously, which allows batching
///   small messages before the flush.
impl scope::ServiceTrait for SharedFrameSender {
    type E = Error;

    fn start(this: &scope::ServiceScope<Self>) {
        // Subtask flushing the stream buffer.
        scope::spawn!(this, |this| async move {
            loop {
                // Flushing is triggered after each send().
                // Since there is only one task doing the flushing,
                // the flushing requests get deduplicated here
                // (see `tokio::sync::Notify`).
                ctx::wait(this.flusher.notified()).await?;
                // Flusher needs to acquire the stream.
                // Since tokio mutex acts as a FIFO queue, all the
                // awaiting messages will be sent before the flushing.
                let mut stream = ctx::wait(this.inner.clone().lock_owned()).await?;
                let inner = stream.as_mut().ok_or(Error::StreamClosed)?;
                let res = ctx::run_with_timeout(RESPONSIVENESS_TIMEOUT, async {
                    Ok(ctx::wait(inner.flush()).await??)
                })
                .await;
                // Failure to flush within timeout invalidates the stream.
                if res.is_err() {
                    stream.take();
                    return res;
                }
            }
        });
    }
}

impl SharedFrameSender {
    fn new(sender: FrameSender) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(sender.inner).into(),
            stats: sender.stats,
            flusher: tokio::sync::Notify::new(),
        }
    }

    /// Sends a frame over the TCP connection.
    /// If an error is returned, the message may or may not have been sent.
    async fn send(this: &scope::ServiceScope<Self>, frame: Frame) -> Result<(), Error> {
        // Guard which increments the usage of resources in the stats object.
        // The usage will be decremented back as soon as the guard is dropped.
        // If creating the guard fails, it means that resource quota has been exceeded.
        let guard = stats::SendGuard::new(this.stats.clone(), frame.0.len())?;
        // Take ownership of the send half of the TCP stream.
        // If the context is canceled before getting the stream, nothing gets send as intended.
        let mut stream = ctx::wait(this.inner.clone().lock_owned()).await?;
        // We execute the actual sending of the message in the background.
        // This way, even if the context gets canceled, we send the full message anyway
        // (since this is a TCP stream, sending part of the message and then giving up would
        // invalidate the stream).
        Ok(scope::spawn!(this, |this| async move {
            let inner = stream.as_mut().ok_or(Error::StreamClosed)?;
            let res = ctx::run_with_timeout(RESPONSIVENESS_TIMEOUT, async {
                // Stats guard is moved into the sending task, so that the background resource
                // usage is tracked appropriately.
                let _guard = guard;
                ctx::wait(inner.write_u32_le(frame.0.len() as u32)).await??;
                ctx::wait(inner.write_all(&frame.0[..])).await??;
                // After sending we trigger flushing the stream.
                // Flushing is implemented in a way that if multiple messages are sent at once
                // flushing occurs only once. See `SharedFrameSender::start`.
                this.flusher.notify_one();
                Ok(())
            })
            .await;
            if res.is_err() {
                stream.take();
            }
            res
        })
        .join()
        .await??)
    }
}
