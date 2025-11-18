use crate::metrics::try_create_histogram_vec;
use std::sync::LazyLock;
use prometheus::{exponential_buckets, HistogramVec};
use std::time::Instant;
use tracing::span::Attributes;
use tracing::Id;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

#[derive(Default)]
pub(crate) struct DelayDetectorLayer {}

const MAX_BUSY_DURATION_NS: u64 = 500000000; // 0.5 sec

pub(crate) static LONG_SPAN_HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_long_span_elapsed",
        "Distribution of the duration of spans wth long duration",
        &["name", "level", "target", "file", "line"],
        Some(exponential_buckets(1.0, 1.2, 30).unwrap()),
    )
    .unwrap()
});

struct Timings {
    idle: u64,
    busy: u64,
    last: Instant,
}

impl Timings {
    fn new() -> Self {
        Self { idle: 0, busy: 0, last: Instant::now() }
    }
}

impl<S> Layer<S> for DelayDetectorLayer
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
