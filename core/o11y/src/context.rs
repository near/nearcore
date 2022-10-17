use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Wraps an actix message with the current Span's context.
/// This lets us trace execution across several actix Actors.
#[derive(actix::Message, Debug)]
#[rtype(result = "<T as actix::Message>::Result")]
pub struct WithSpanContext<T: actix::Message> {
    pub msg: T,
    pub context: opentelemetry::Context,
}

impl<T: actix::Message> WithSpanContext<T> {
    pub fn new(msg: T) -> Self {
        Self { msg, context: Span::current().context() }
    }
}

/// Allows easily attaching the current span's context to a Message.
pub trait WithSpanContextExt: actix::Message {
    fn with_span_context(self) -> WithSpanContext<Self>
    where
        Self: Sized,
    {
        WithSpanContext::<Self>::new(self)
    }
}
impl<T: actix::Message> WithSpanContextExt for T {}
