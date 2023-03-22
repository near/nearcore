use std::sync::Arc;

#[derive(Clone)]
pub(super) struct Once(Arc<tokio::sync::Semaphore>);

impl Once {
    pub fn new() -> Self {
        Self(Arc::new(tokio::sync::Semaphore::new(0)))
    }

    /// Sends the signal, waking all tasks awaiting for recv().
    ///
    /// After this call recv().await will always return immediately.
    /// After this call any subsequent call to send() is a noop.
    pub fn send(&self) {
        self.0.close();
    }

    /// recv() waits for the first call to send().
    ///
    /// Cancellable.
    pub async fn recv(&self) {
        // We await for the underlying semaphore to get closed.
        // This is the only possible outcome, because we never add
        // any permits to the semaphore.
        let res = self.0.acquire().await;
        debug_assert!(res.is_err());
    }

    /// Checks if send() was already called.
    pub fn try_recv(&self) -> bool {
        self.0.is_closed()
    }
}
