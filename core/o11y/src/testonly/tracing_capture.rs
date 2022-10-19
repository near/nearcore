use std::fmt::Write;
use std::mem;
use std::sync::{Arc, Mutex};

/// Intercepts `tracing` logs.
///
/// The intended use-case is for tests which want to probe inner workings of the
/// system which are not observable through public APIs only.
pub struct TracingCapture {
    captured: Arc<Mutex<Captured>>,
    _guard: tracing::subscriber::DefaultGuard,
}

struct Captured {
    on_log: Arc<dyn Fn(&str) + Send + Sync>,
    logs: Vec<String>,
}

struct Subscriber(Arc<Mutex<Captured>>);

impl TracingCapture {
    /// Sets `TracingCapture` as the "default subscriber".
    ///
    /// "default subscriber" is essentially a thread-local, so some care must be
    /// taken to properly propagate this across threads for multi-threaded
    /// tests.
    pub fn enable() -> TracingCapture {
        let captured =
            Arc::new(Mutex::new(Captured { on_log: Arc::new(|_| ()), logs: Vec::new() }));
        let subscriber = Subscriber(Arc::clone(&captured));
        let _guard = tracing::subscriber::set_default(subscriber);
        TracingCapture { captured, _guard }
    }
    /// Get all the logs so-far.
    ///
    /// Useful to verify that some particular code-path was hit by a test.
    pub fn drain(&mut self) -> Vec<String> {
        let mut guard = self.captured.lock().unwrap();
        mem::take(&mut guard.logs)
    }
    /// Sets the callback to execute on every log line emitted.
    ///
    /// The intended use-case is for testing multithreaded code: by *blocking*
    /// in the `on_log` for specific log-lines, the test can maneuver the
    /// threads into particularly interesting interleavings.
    pub fn set_callback(&mut self, on_log: impl Fn(&str) + Send + Sync + 'static) {
        self.captured.lock().unwrap().on_log = Arc::new(on_log)
    }
}

impl tracing::Subscriber for Subscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let buf = span.metadata().name().to_string();

        let buf = {
            let mut visitor = AppendToString(buf);
            span.record(&mut visitor);
            visitor.0
        };

        // Tricky: as `on_log` is expected to block, we take care to call it
        // *without* holding any mutexes.
        let on_log = Arc::clone(&self.0.lock().unwrap().on_log);
        on_log(&buf);

        let mut guard = self.0.lock().unwrap();
        guard.logs.push(buf);

        tracing::span::Id::from_u64(guard.logs.len() as u64)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, _event: &tracing::Event<'_>) {}
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

struct AppendToString(String);
impl tracing::field::Visit for AppendToString {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let _ = write!(self.0, " {}={:?}", field.name(), value);
    }
}
