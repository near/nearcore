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

/// If sending/receiving a message takes more than `RESPONSIVENESS_TIMEOUT`, we consider the stream broken.
const RESPONSIVENESS_TIMEOUT: time::Duration = time::Duration::minutes(1);

type ReadHalf = tokio::io::BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>;
type WriteHalf = tokio::io::BufWriter<tokio::io::WriteHalf<tokio::net::TcpStream>>;

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct Frame(pub Vec<u8>);

#[derive(thiserror::Error, Clone, Debug)]
pub(crate) enum Error {
    #[error(transparent)]
    IO(Arc<std::io::Error>),
    #[error(transparent)]
    Canceled(#[from] ctx::ErrCanceled),
    #[error(transparent)]
    Stats(#[from] stats::Error),
}

#[derive(thiserror::Error, Debug)]
#[error("Stream has been closed")]
pub(crate) struct ErrStreamClosed;

// Manual implementation of the error casting, to wrap io::Error into a
// cloneable Arc.
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO(Arc::new(err))
    }
}

pub struct FrameStream {
    info: tcp::StreamInfo,
    /// FrameStream acts as a microservice - it is running some tasks in the background.
    /// These tasks are executed within the "service" scope (therefore they will be canceled
    /// and terminated as soon as the outer scope terminates).
    service: scope::Service<Error>,
    /// Statistics about the resource usage.
    stats: Arc<stats::Stats>,
    /// Signal used to trigger background flushing of the send stream,
    /// as soon as the current batch of send calls is completed.
    flusher: tokio::sync::Notify,
    /// send and recv halfs of the stream can be accessed independently,
    /// but each half supports only synchronous sending/receiving.
    /// To allow independent callers to send/recv messages, we need a mutex.
    /// As soon as the stream becomes broken (i.e. an unrecoverable error occurs)
    /// we want to prevent further use of the TCP stream halfs - the call which
    /// encounters the error replaces the respective stream half with None
    /// (that's why we use Mutex<Option<StreamHalf>>).
    ///
    /// An example of important case for preventing further use:
    /// * we read/write a part of a frame, then encounter an error
    /// * reading/writing the next frame may lead to unexpected errors which might be hard to
    ///  diagnose.
    send: Arc<tokio::sync::Mutex<Option<WriteHalf>>>,
    recv: Arc<tokio::sync::Mutex<Option<ReadHalf>>>,
}

impl std::ops::Deref for FrameStream {
    type Target = tcp::StreamInfo;
    fn deref(&self) -> &Self::Target { &self.info }
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
            info: stream.info,
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
                // Flushing is triggered after each send().
                ctx::wait(self_.flusher.notified()).await?;
                // Once triggered, we take a lock on the send half of the TCP stream.
                // Since tokio::sync::Mutex implements FIFO semantics, all send() calls
                // already waiting in queue have priority, hence we flush only after a batch
                // of send() calls.
                let mut send = ctx::wait(self_.send.lock()).await?;
                let mut inner = match send.take() {
                    Some(it) => it,
                    // If stream has been closed, then task is done.
                    None => return Ok(()),
                };
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
        // Guard which increments the usage of resources in the stats object.
        // The usage will be decremented back as soon as the guard is dropped.
        // If creating the guard fails, it means that resource quota has been exceeded.
        let guard = stats::SendGuard::new(self.stats.clone(), frame.0.len())?;
        // Take ownership of the send half of the TCP stream.
        // If the context is canceled before getting the stream, nothing gets send as intended.
        let mut send = ctx::wait(self.send.clone().lock_owned()).await?;
        // We extract the stream from the mutex guard to utilize RAII for
        // handling unrecoverable errors: if an error occurs while sending the frame,
        // we won't put the stream back into the guard, therefore we prevent further use of the
        // stream.
        let mut inner = send.take().ok_or(ErrStreamClosed)?;
        let self_ = self.clone();
        // We execute the actual sending of the message in the background.
        // This way, even if the context gets canceled, we send the full message anyway
        // (since this is a TCP stream, sending part of the message and then giving up would
        // invalidate the stream). Still we put a constant timeout on sending the message,
        // after which we consider the stream broken.
        Ok(self
            .service
            .spawn(ctx::run_with_timeout(RESPONSIVENESS_TIMEOUT, async move {
                // Stats guard is moved into the sending task, so that the background resource
                // usage is tracked appropriately.
                let _guard = guard;
                // NOTE: write_* are not cancel-safe technically (they might write
                // only some of the bytes, if they return an error). Therefore
                // any error (including ErrCanceled) invalidates the stream at this point.
                // This WAI, just in this specific case calling `ctx::wait` on the whole task would
                // be equally good.
                ctx::wait(inner.write_u32_le(frame.0.len() as u32)).await??;
                ctx::wait(inner.write_all(&frame.0[..])).await??;
                // After sending we trigger flushing the stream.
                // Flushing is implemented in a way that if multiple messages are sent at once
                // flushing occurs only once. See implementation of `FramedStream::new`.
                self_.flusher.notify_one();
                // If sending the frame fails and exits early, the stream becomes broken.
                *send = Some(inner);
                Ok(())
            }))?
            .join()
            .await??)
    }

    /// Receives a frame from the TCP connection.
    /// If error is returned, a message may or may not been received.
    async fn recv(self: &Arc<Self>) -> anyhow::Result<Frame> {
        // Take ownership of the recv half of the TCP stream.
        // If the context is canceled before getting the stream, nothing gets received, as intended.
        let mut recv = ctx::wait(self.recv.clone().lock_owned()).await?;
        // We extract the stream from the mutex guard to utilize RAII for
        // handling unrecoverable errors: if an error occurs while receiving the frame,
        // we won't put the stream back into the guard, therefore we prevent further use of the
        // stream.
        let mut inner = recv.take().ok_or(ErrStreamClosed)?;
        let stats = self.stats.clone();
        // We execute the actual receiving of the message in the background.
        // This way, even if the context gets canceled, we receive the full message anyway
        // (since this is a TCP stream, receiving part of the message and then giving up would
        // invalidate the stream).
        Ok(self
            .service
            .spawn(async move {
                // First we wait for some initial bytes.
                // Note that we will read and drop the whole frame,
                // if the caller context cancels even before the first byte arrives.
                // That's good enough - optimizing the implementation to avoid dropping
                // before receiving the first byte doesn't give us any useful guarantees.
                // NOTE: we intentionally do not cache a Frame in FrameStream if noone is
                // awaiting recv() - this prevents memory leaks caused by caching large unwanted
                // messages. Still, some improvement could be made, as currently we keep the buffer
                // until the frame is received (or until RESPONSIVENESS_TIMEOUT).
                // Alternatively we could drop the large buffer immediately as soon as
                // the recv() caller cancels the context and just drop the remaining incoming bytes
                // in the background.
                // NOTE: read_* are not cancel safe, hence any error (including ErrCanceled)
                // invalidates the stream (WAI). However, wrapping the whole task in ctx::wait
                // would be equivalent in this specific case.
                let n = ctx::wait(inner.read_u32_le()).await?? as usize;
                // Once we know how large the frame will be, we create a guard which increments
                // the usage of resources in the stats object.
                // The usage will be decremented back as soon as the guard is dropped.
                // If creating the guard fails, it means that resource quota has been exceeded.
                let _guard = stats::RecvGuard::new(stats, n)?;
                let mut buf = vec![0; n];
                // Once we received the first bytes, we apply a constant timeout for
                // receiving the frame, after which we consider the stream broken.
                ctx::run_with_timeout(
                    RESPONSIVENESS_TIMEOUT,
                    ctx::wait(inner.read_exact(&mut buf[..])),
                )
                .await??;
                // If receiving the frame fails and exits early, the stream becomes broken.
                *recv = Some(inner);
                Ok(Frame(buf))
            })?
            .join()
            .await??)
    }
}
