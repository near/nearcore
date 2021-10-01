use std::fmt::Write;
use std::mem;
use std::sync::{Arc, Mutex};

/// Captures logs into a `Vec<String>`.
///
/// You can use this in tests to verify that the code hits a particular code
/// path.
pub struct TracingCapture {
    captured: Arc<Mutex<Captured>>,
    _guard: tracing::subscriber::DefaultGuard,
}

#[derive(Default)]
struct Captured {
    logs: Vec<String>,
}

#[derive(Default)]
struct Subscriber(Arc<Mutex<Captured>>);

impl TracingCapture {
    pub fn enable() -> TracingCapture {
        let subscriber = Subscriber::default();
        let captured = subscriber.0.clone();
        let _guard = tracing::subscriber::set_default(subscriber);
        TracingCapture { captured, _guard }
    }
    pub fn drain(&mut self) -> Vec<String> {
        let mut guard = self.captured.lock().unwrap();
        mem::take(&mut guard.logs)
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
