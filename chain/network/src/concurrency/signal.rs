use std::sync::Arc;

#[derive(Clone)]
pub(super) struct Once(Arc<tokio::sync::Semaphore>);

impl Once {
    pub fn new() -> Self {
        Self(Arc::new(tokio::sync::Semaphore::new(0)))
    }

    /// sends the signal, waking all tasks awaiting for recv().
    /// After this call recv().await will always return immediately.
    /// After this call any subsequent call to send() is a noop.
    pub fn send(&self) {
        self.0.close();
    }

    /// recv() waits for the first call to send().
    /// Cancellable.
    pub async fn recv(&self) {
        let _ = self.0.acquire().await;
    }

    /// checks if send() was already called.
    pub fn try_recv(&self) -> bool {
        self.0.is_closed()
    }
}
