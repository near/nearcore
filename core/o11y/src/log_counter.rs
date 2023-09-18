use crate::metrics::try_create_int_counter_vec;
use crate::OpenTelemetryLevel;
use once_cell::sync::Lazy;
use prometheus::{IntCounter, IntCounterVec};
use tracing_subscriber::layer::{Context, Layered, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{reload, Layer};

pub(crate) static LOG_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_log_msg_total",
        "Number of messages logged at various log levels",
        &["level"],
    )
    .unwrap()
});

pub(crate) static LOG_WITH_LOCATION_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_log_msg_with_loc_total",
        "Number of messages logged at various log levels wth target and location",
        &["level", "target", "location"],
    )
    .unwrap()
});

type LogCountingLayer<Inner> = Layered<reload::Layer<LogCounter, Inner>, Inner>;

/// Constructs a LogCounter layer which keeps tracks of the number of messages
/// emitted per severity level, and their line-of-code location.
pub(crate) async fn add_log_counting_layer<S>(
    subscriber: S,
) -> (LogCountingLayer<S>, reload::Handle<LogCounter, S>)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let (layer, handle) = reload::Layer::<LogCounter, S>::new(LogCounter::default());
    (subscriber.with(layer), handle)
}

/// A tracing Layer that updates prometheus metrics based on the metadata of log events.
pub(crate) struct LogCounter {
    // Initializes individual counters as a performance optimization.
    error_metric: IntCounter,
    warn_metric: IntCounter,
    info_metric: IntCounter,
    debug_metric: IntCounter,
    trace_metric: IntCounter,
}

impl LogCounter {
    /// Increments a counter for every log message.
    fn count_log(&self, level: &tracing::Level) {
        match level {
            &tracing::Level::ERROR => self.error_metric.inc(),
            &tracing::Level::WARN => self.warn_metric.inc(),
            &tracing::Level::INFO => self.info_metric.inc(),
            &tracing::Level::DEBUG => self.debug_metric.inc(),
            &tracing::Level::TRACE => self.trace_metric.inc(),
        };
    }

    /// Increments a counter with target and LoC for high severity messages.
    fn count_log_with_loc(&self, level: &tracing::Level, target: &str, location: &str) {
        match level {
            &tracing::Level::ERROR | &tracing::Level::WARN => LOG_WITH_LOCATION_COUNTER
                .with_label_values(&[&level.as_str(), target, location])
                .inc(),
            // Retaining LoC for low-severity messages can lead to excessive memory usage.
            // Therefore, only record LoC for high severity log messages.
            _ => {}
        };
    }

    // Event names are usually "event path/to/file.rs:123". Strip the prefix.
    fn parse_loc<'a>(&'a self, name: &'a str) -> &'a str {
        if let Some((_, loc)) = name.split_once("event ") {
            loc
        } else {
            name
        }
    }
}

impl Default for LogCounter {
    fn default() -> Self {
        Self {
            error_metric: LOG_COUNTER.with_label_values(&[&OpenTelemetryLevel::ERROR.as_ref()]),
            warn_metric: LOG_COUNTER.with_label_values(&[&OpenTelemetryLevel::WARN.as_ref()]),
            info_metric: LOG_COUNTER.with_label_values(&[&OpenTelemetryLevel::INFO.as_ref()]),
            debug_metric: LOG_COUNTER.with_label_values(&[&OpenTelemetryLevel::DEBUG.as_ref()]),
            trace_metric: LOG_COUNTER.with_label_values(&[&OpenTelemetryLevel::TRACE.as_ref()]),
        }
    }
}

impl<S> Layer<S> for LogCounter
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    fn on_event(&self, event: &tracing::Event, _ctx: Context<S>) {
        let level = event.metadata().level();
        self.count_log(level);

        let target = event.metadata().target();
        let loc = self.parse_loc(event.metadata().name());
        self.count_log_with_loc(level, target, loc);
    }
}
