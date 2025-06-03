use crate::metrics::try_create_histogram_vec;
use prometheus::HistogramVec;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tracing::Id;
use tracing::span::Attributes;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

#[derive(Default)]
pub(crate) struct SpanDurationLogger {}

pub(crate) static SPAN_BUSY_DURATIONS: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_span_busy_duration",
        "Busy duration of spans",
        &["name", "level", "target", "measure"],
        // Cover the range from 0.01s to 10s.
        // Keep the number of buckets small to limit the memory usage.
        Some(vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
    )
    .unwrap()
});

// Keeps track of the time a span existed and was entered.
// The time since creation of a span is `idle + busy`.
struct Timings {
    /// The time a span existed but wasn't entered.
    idle: Duration,
    /// Measures the time spent in the span, i.e. between enter() and exit().
    /// Note that a span may be entered and exited multiple times.
    busy: Duration,
    /// Instant of a last event: creation, enter, exit.
    last: Instant,
    /// Label to use for the "measure" attribute.
    measure: String,
}

impl Timings {
    fn new(measure: String) -> Self {
        Self { idle: Duration::ZERO, busy: Duration::ZERO, last: Instant::now(), measure }
    }

    // Unlikely to overflow. Even if overflows, the impact is negligible.
    #[allow(clippy::arithmetic_side_effects)]
    fn observe_idle(&mut self) {
        let previous = std::mem::replace(&mut self.last, Instant::now());
        self.idle += self.last.duration_since(previous);
    }

    // Unlikely to overflow. Even if overflows, the impact is negligible.
    #[allow(clippy::arithmetic_side_effects)]
    fn observe_busy(&mut self) {
        let previous = std::mem::replace(&mut self.last, Instant::now());
        self.busy += self.last.duration_since(previous);
    }
}

struct CheckForAttribute<'a> {
    name: &'a str,
    value: Option<String>,
}

impl<'a> CheckForAttribute<'a> {
    fn new(name: &'a str) -> Self {
        Self { name, value: None }
    }
}

impl<'a> tracing::field::Visit for CheckForAttribute<'a> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == self.name {
            self.value = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == self.name {
            self.value = Some(format!("{:?}", value));
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.value.is_some() {
            return; // Already found a measure attribute, no need to overwrite.
        }
        if field.name() == "tag_block_production" && value {
            self.value = "tag_block_production".to_string().into();
        }
    }
}

impl<S> Layer<S> for SpanDurationLogger
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            tracing::error!(target: "span_duration_logger", ?id, "on_new_span: no span available");
            return;
        };

        // TODO: implement with filter
        let level = span.metadata().level();
        if matches!(level, &tracing::Level::TRACE) {
            return;
        }

        // Check if the attribute "measure" is set and capture its value.
        let mut check = CheckForAttribute::new("measure");
        attrs.record(&mut check);

        let Some(measure) = check.value else {
            return;
        };

        let mut extensions = span.extensions_mut();
        extensions.insert(Timings::new(measure));
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if let Some(timings) = extensions.get_mut::<Timings>() {
                timings.observe_idle();
            }
        } else {
            tracing::error!(target: "span_duration_logger", ?id, "on_enter: no span available");
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if let Some(timings) = extensions.get_mut::<Timings>() {
                timings.observe_busy();
            }
        } else {
            tracing::error!(target: "span_duration_logger", ?id, "on_exit: no span available");
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            let mut extensions = span.extensions_mut();
            if let Some(timings) = extensions.get_mut::<Timings>() {
                timings.observe_idle();

                let name = span.name();
                let level = span.metadata().level();
                let target = span.metadata().target();
                SPAN_BUSY_DURATIONS
                    .with_label_values(&[name, level.as_str(), target, timings.measure.as_str()])
                    .observe(timings.busy.as_secs_f64());

                const MAX_SPAN_BUSY_DURATION_SEC: u64 = 1;
                if timings.busy > Duration::from_secs(MAX_SPAN_BUSY_DURATION_SEC) {
                    tracing::debug!(
                        target: "span_duration_logger",
                        busy = ?timings.busy,
                        idle = ?timings.idle,
                        ?level,
                        ?target,
                        ?name,
                        file = ?span.metadata().file(),
                        line = ?span.metadata().line(),
                        "Span duration too long");
                }
            }
        } else {
            tracing::error!(target: "span_duration_logger", ?id, "on_close: no span available");
        }
    }
}
