use parking_lot::Mutex;

/// A one-shot shutdown signal. When fired, executes its callback exactly once.
pub struct ShutdownSignal {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl ShutdownSignal {
    pub fn new(f: impl FnOnce() + Send + 'static) -> Self {
        Self { callback: Mutex::new(Some(Box::new(f))) }
    }

    /// Creates a no-op signal that does nothing when fired.
    pub fn noop() -> Self {
        Self::new(|| {})
    }

    /// Fires the signal. Only the first call executes the callback; subsequent calls are no-ops.
    pub fn fire(&self) {
        if let Some(f) = self.callback.lock().take() {
            f();
        }
    }
}
