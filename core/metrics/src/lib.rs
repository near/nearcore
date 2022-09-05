//! A fork of the lighthouse_metrics crate used to implement prometheus
//!
//! A wrapper around the `prometheus` crate that provides a global, `lazy_static` metrics registry
//! and functions to add and use the following components (more info at
//! [Prometheus docs](https://prometheus.io/docs/concepts/metric_types/)):
//!
//! - `Histogram`: used with `start_timer()` and `observe_duration()` or
//!     `observe()` method to record durations (e.g., block processing time).
//! - `IncCounter`: used to represent an ideally ever-growing, never-shrinking
//!     integer (e.g., number of block processing requests).
//! - `IntGauge`: used to represent an varying integer (e.g., number of
//!     attestations per block).
//!
//! ## Important
//!
//! Metrics will fail if two items have the same `name`. All metrics must have a unique `name`.
//! Because we use a global registry there is no namespace per crate, it's one big global space.
//!
//! See the [Prometheus naming best practices](https://prometheus.io/docs/practices/naming/) when
//! choosing metric names.
//!
//! ## Example
//!
//! ```rust
//! use once_cell::sync::Lazy;
//!
//! use near_metrics::*;
//!
//! // These metrics are "magically" linked to the global registry defined in `lighthouse_metrics`.
//! pub static RUN_COUNT: Lazy<IntCounter> = Lazy::new(|| {
//!     try_create_int_counter(
//!         "runs_total",
//!         "Total number of runs",
//!     )
//!     .unwrap()
//! });
//! pub static CURRENT_VALUE: Lazy<IntGauge> = Lazy::new(|| {
//!     try_create_int_gauge(
//!         "current_value",
//!         "The current value",
//!     )
//!     .unwrap()
//! });
//! pub static RUN_TIME: Lazy<Histogram> = Lazy::new(|| {
//!     try_create_histogram(
//!         "run_seconds",
//!         "Time taken (measured to high precision)",
//!     )
//!     .unwrap()
//! });
//!
//! fn main() {
//!     for i in 0..100 {
//!         RUN_COUNT.inc();
//!         let timer = RUN_TIME.start_timer();
//!         for j in 0..10 {
//!             CURRENT_VALUE.set(j);
//!             println!("Howdy partner");
//!         }
//!         timer.observe_duration();
//!     }
//!
//!     assert_eq!(100, RUN_COUNT.get());
//!     assert_eq!(9, CURRENT_VALUE.get());
//!     assert_eq!(100, RUN_TIME.get_sample_count());
//!     assert!(0.0 < RUN_TIME.get_sample_sum());
//! }
//! ```

pub use prometheus::{
    self, exponential_buckets, linear_buckets, Counter, Encoder, Gauge, GaugeVec, Histogram,
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Result,
    TextEncoder,
};

/// Collect all the metrics for reporting.
pub fn gather() -> Vec<prometheus::proto::MetricFamily> {
    prometheus::gather()
}

/// Attempts to crate an `IntCounter`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_int_counter(name: &str, help: &str) -> Result<IntCounter> {
    let opts = Opts::new(name, help);
    let counter = IntCounter::with_opts(opts)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

/// Attempts to crate an `IntCounterVec`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_int_counter_vec(
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<IntCounterVec> {
    let opts = Opts::new(name, help);
    let counter = IntCounterVec::new(opts, labels)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

/// Attempts to crate an `Counter`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_counter(name: &str, help: &str) -> Result<Counter> {
    let opts = Opts::new(name, help);
    let counter = Counter::with_opts(opts)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

/// Creates 'IntCounterVec' - if it has trouble registering to Prometheus
/// it will keep appending a number until the name is unique.
pub fn do_create_int_counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    if let Ok(value) = try_create_int_counter_vec(name, help, labels) {
        return value;
    }
    let mut suffix = 0;

    loop {
        if let Ok(value) =
            try_create_int_counter_vec(format!("{}_{}", name, suffix).as_str(), help, labels)
        {
            return value;
        }
        suffix += 1;
    }
}

/// Attempts to crate an `IntGauge`, returning `Err` if the registry does not accept the gauge
/// (potentially due to naming conflict).
pub fn try_create_int_gauge(name: &str, help: &str) -> Result<IntGauge> {
    let opts = Opts::new(name, help);
    let gauge = IntGauge::with_opts(opts)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Attempts to crate an `IntGaugeVec`, returning `Err` if the registry does not accept the gauge
/// (potentially due to naming conflict).
pub fn try_create_int_gauge_vec(name: &str, help: &str, labels: &[&str]) -> Result<IntGaugeVec> {
    let opts = Opts::new(name, help);
    let gauge = IntGaugeVec::new(opts, labels)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Attempts to crate an `Gauge`, returning `Err` if the registry does not accept the gauge
/// (potentially due to naming conflict).
pub fn try_create_gauge(name: &str, help: &str) -> Result<Gauge> {
    let opts = Opts::new(name, help);
    let gauge = Gauge::with_opts(opts)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Attempts to crate an `GaugeVec`, returning `Err` if the registry does not accept the gauge
/// (potentially due to naming conflict).
pub fn try_create_gauge_vec(name: &str, help: &str, labels: &[&str]) -> Result<GaugeVec> {
    let opts = Opts::new(name, help);
    let gauge = GaugeVec::new(opts, labels)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Attempts to crate a `Histogram`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_histogram(name: &str, help: &str) -> Result<Histogram> {
    let opts = HistogramOpts::new(name, help);
    let histogram = Histogram::with_opts(opts)?;
    prometheus::register(Box::new(histogram.clone()))?;
    Ok(histogram)
}

/// Attempts to crate a `Histogram`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_histogram_with_buckets(name: &str, help: &str, buckets: Vec<f64>) -> Result<Histogram> {
    let opts = HistogramOpts::new(name, help).buckets(buckets);
    let histogram = Histogram::with_opts(opts)?;
    prometheus::register(Box::new(histogram.clone()))?;
    Ok(histogram)
}

/// Attempts to create a `HistogramVector`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_histogram_vec(
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Option<Vec<f64>>,
) -> Result<HistogramVec> {
    let mut opts = HistogramOpts::new(name, help);
    if let Some(buckets) = buckets {
        opts = opts.buckets(buckets);
    }
    let histogram = HistogramVec::new(opts, labels)?;
    prometheus::register(Box::new(histogram.clone()))?;
    Ok(histogram)
}
