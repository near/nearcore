use std::sync::Arc;

/// Single-threaded runtime which cancels all the tasks as soon as it is dropped.
/// A potential in-place replacement for actix::Actor.
pub(crate) struct Runtime {
    pub handle: tokio::runtime::Handle,
    stop: Arc<tokio::sync::Notify>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Runtime {
    pub fn new() -> Self {
        let stop = Arc::new(tokio::sync::Notify::new());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let handle = runtime.handle().clone();
        let thread = std::thread::spawn({
            let stop = stop.clone();
            move || runtime.block_on(stop.notified())
        });
        Self { handle, stop, thread: Some(thread) }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop.notify_one();
        let thread = self.thread.take().unwrap();
        // Await for the thread to stop, unless it is the current thread
        // (i.e. nobody waits for it).
        if std::thread::current().id() != thread.thread().id() {
            thread.join().unwrap();
        }
    }
}
