use crate::metrics::try_create_histogram_vec;
use once_cell::sync::Lazy;
use prometheus::{exponential_buckets, HistogramVec};
use std::time::Instant;
use tracing::span::Attributes;
use tracing::Id;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

#[derive(Default)]
pub(crate) struct SpanDurationLogger {}

const MAX_BUSY_DURATION_NS: u64 = 500000000; // 0.5 sec

pub(crate) static LONG_SPAN_HISTOGRAM: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
<<<<<<<< HEAD:core/o11y/src/delay_detector.rs
        "near_long_span_elapsed",
        "Distribution of the duration of spans wth long duration",
        &["name", "level", "target", "file", "line"],
        Some(exponential_buckets(1.0, 1.2, 30).unwrap()),
========
        "near_span_duration",
        "Distribution of the duration of spans with long duration",
        &["name", "level", "target"],
        // Cover the range from 0.01s to 10s.
        // Keep the number of buckets small to limit the memory usage.
        Some(exponential_buckets(0.01, 10f64.powf(1. / 3.), 10).unwrap()),
>>>>>>>> 3aa61992e (Rename to SpanDurationLogger):core/o11y/src/span_duration_logger.rs
    )
    .unwrap()
});

// Keeps track of the time a span existed and was entered.
// The time since creation of a span is `idle + busy`.
struct Timings {
    /// The time a span existed but wasn't entered.
    idle: u64,
    /// Measures the time spent in the span, i.e. between enter() and exit().
    /// Note that a span may be entered and exited multiple times.
    busy: u64,
    /// Instant of a last event: creation, enter, exit.
    last: Instant,
}

impl Timings {
    fn new() -> Self {
        Self { idle: 0, busy: 0, last: Instant::now() }
    }
}

<<<<<<<< HEAD:core/o11y/src/delay_detector.rs
impl<S> Layer<S> for DelayDetectorLayer
========
// Allow arithmetic side effects, because this is not a critical code. It
// computes the durations of spans, and the worst that can happen is a span
// duration getting reported in a wrong bucket.
#[allow(clippy::arithmetic_side_effects)]
impl<S> Layer<S> for SpanDurationLogger
>>>>>>>> 3aa61992e (Rename to SpanDurationLogger):core/o11y/src/span_duration_logger.rs
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();
        extensions.insert(Timings::new());
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();
        if let Some(timings) = extensions.get_mut::<Timings>() {
            let now = Instant::now();
            timings.idle += (now - timings.last).as_nanos() as u64;
            timings.last = now;
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();
        if let Some(timings) = extensions.get_mut::<Timings>() {
            let now = Instant::now();
            timings.busy += (now - timings.last).as_nanos() as u64;
            timings.last = now;
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("Span not found, this is a bug");
        let extensions = span.extensions();
        if let Some(Timings { busy, mut idle, last }) = extensions.get::<Timings>() {
            idle += (Instant::now() - *last).as_nanos() as u64;

            if busy > &MAX_BUSY_DURATION_NS {
                let level = span.metadata().level();
                let target = span.metadata().target();
                let file = span.metadata().file().unwrap_or("");
                let line = span.metadata().line().map_or("".to_string(), |x| x.to_string());
                let name = span.name();

                let busy_sec = *busy as f64 * 1e-9;
                let idle_sec = idle as f64 * 1e-9;
                tracing::warn!(target: "delay_detector",
                    "Span duration too long: {busy_sec:.2}s. Idle time: {idle_sec:.2}s. {level}: {target}: {name}. {file}:{line}",
                );
                LONG_SPAN_HISTOGRAM
                    .with_label_values(&[name, level.as_str(), target, file, &line])
                    .observe(busy_sec);
            }
        }
    }
}
