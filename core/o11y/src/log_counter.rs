use crate::metrics::try_create_int_counter_vec;
use prometheus::{IntCounter, IntCounterVec};
use std::sync::LazyLock;
use tracing_subscriber::layer::{Context, Layered};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

pub(crate) static LOG_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_log_msg_total",
        "Number of messages logged at various log levels",
        &["level"],
    )
    .unwrap()
});

pub(crate) static LOG_WITH_LOCATION_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_log_msg_with_loc_total",
        "Number of messages logged at various log levels wth target and location",
        &["level", "target", "file", "line"],
    )
    .unwrap()
});

pub(crate) type LogCountingLayer<Inner> = Layered<LogCounter, Inner>;

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
    fn count_log_with_loc(
        &self,
        level: &tracing::Level,
        target: &str,
        file: Option<&str>,
        line: Option<u32>,
    ) {
        match level {
            &tracing::Level::ERROR | &tracing::Level::WARN | &tracing::Level::INFO => {
                LOG_WITH_LOCATION_COUNTER
                    .with_label_values(&[
                        &level.as_str(),
                        target,
                        file.unwrap_or(""),
                        &line.map_or("".to_string(), |x| x.to_string()),
                    ])
                    .inc()
            }
            // Retaining LoC for low-severity messages can lead to excessive memory usage.
            // Therefore, only record LoC for high severity log messages.
            _ => {}
        };
    }
}

impl Default for LogCounter {
    fn default() -> Self {
        Self {
            error_metric: LOG_COUNTER.with_label_values(&["error"]),
            warn_metric: LOG_COUNTER.with_label_values(&["warn"]),
            info_metric: LOG_COUNTER.with_label_values(&["info"]),
            debug_metric: LOG_COUNTER.with_label_values(&["debug"]),
            trace_metric: LOG_COUNTER.with_label_values(&["trace"]),
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
        let file = event.metadata().file();
        let line = event.metadata().line();
        self.count_log_with_loc(level, target, file, line);
    }
}
