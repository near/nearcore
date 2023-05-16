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
//! use near_o11y::metrics::*;
//!
//! // These metrics are "magically" linked to the global registry defined in `lighthouse_metrics`.
//! pub static RUN_COUNT: Lazy<IntCounter> = Lazy::new(|| {
//!     try_create_int_counter(
//!         "near_runs_total",
//!         "Total number of runs",
//!     )
//!     .unwrap()
//! });
//! pub static CURRENT_VALUE: Lazy<IntGauge> = Lazy::new(|| {
//!     try_create_int_gauge(
//!         "near_current_value",
//!         "The current value",
//!     )
//!     .unwrap()
//! });
//! pub static RUN_TIME: Lazy<Histogram> = Lazy::new(|| {
//!     try_create_histogram(
//!         "near_run_seconds",
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

use once_cell::sync::Lazy;
pub use prometheus::{
    self, core::MetricVec, core::MetricVecBuilder, exponential_buckets, linear_buckets, Counter,
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Opts, Result, TextEncoder,
};
use std::collections::HashSet;

/// Collect all the metrics for reporting.
pub fn gather() -> Vec<prometheus::proto::MetricFamily> {
    prometheus::gather()
}

/// Attempts to crate an `IntCounter`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_int_counter(name: &str, help: &str) -> Result<IntCounter> {
    check_metric_near_prefix(name)?;
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
    check_metric_near_prefix(name)?;
    let opts = Opts::new(name, help);
    let counter = IntCounterVec::new(opts, labels)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

/// Attempts to crate an `Counter`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_counter(name: &str, help: &str) -> Result<Counter> {
    check_metric_near_prefix(name)?;
    let opts = Opts::new(name, help);
    let counter = Counter::with_opts(opts)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

/// Attempts to crate an `IntGauge`, returning `Err` if the registry does not accept the gauge
/// (potentially due to naming conflict).
pub fn try_create_int_gauge(name: &str, help: &str) -> Result<IntGauge> {
    check_metric_near_prefix(name)?;
    let opts = Opts::new(name, help);
    let gauge = IntGauge::with_opts(opts)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Attempts to crate an `IntGaugeVec`, returning `Err` if the registry does not accept the gauge
/// (potentially due to naming conflict).
pub fn try_create_int_gauge_vec(name: &str, help: &str, labels: &[&str]) -> Result<IntGaugeVec> {
    check_metric_near_prefix(name)?;
    let opts = Opts::new(name, help);
    let gauge = IntGaugeVec::new(opts, labels)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Attempts to crate an `Gauge`, returning `Err` if the registry does not accept the gauge
/// (potentially due to naming conflict).
pub fn try_create_gauge(name: &str, help: &str) -> Result<Gauge> {
    check_metric_near_prefix(name)?;
    let opts = Opts::new(name, help);
    let gauge = Gauge::with_opts(opts)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Attempts to crate an `GaugeVec`, returning `Err` if the registry does not accept the gauge
/// (potentially due to naming conflict).
pub fn try_create_gauge_vec(name: &str, help: &str, labels: &[&str]) -> Result<GaugeVec> {
    check_metric_near_prefix(name)?;
    let opts = Opts::new(name, help);
    let gauge = GaugeVec::new(opts, labels)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Attempts to crate a `Histogram`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_histogram(name: &str, help: &str) -> Result<Histogram> {
    check_metric_near_prefix(name)?;
    let opts = HistogramOpts::new(name, help);
    let histogram = Histogram::with_opts(opts)?;
    prometheus::register(Box::new(histogram.clone()))?;
    Ok(histogram)
}

/// Attempts to crate a `Histogram`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_histogram_with_buckets(
    name: &str,
    help: &str,
    buckets: Vec<f64>,
) -> Result<Histogram> {
    check_metric_near_prefix(name)?;
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
    check_metric_near_prefix(name)?;
    let mut opts = HistogramOpts::new(name, help);
    if let Some(buckets) = buckets {
        opts = opts.buckets(buckets);
    }
    let histogram = HistogramVec::new(opts, labels)?;
    prometheus::register(Box::new(histogram.clone()))?;
    Ok(histogram)
}

static EXCEPTIONS: Lazy<HashSet<&str>> = Lazy::new(|| {
    HashSet::from([
        "flat_storage_cached_changes_num_items",
        "flat_storage_cached_changes_size",
        "flat_storage_cached_deltas",
        "flat_storage_creation_fetched_state_items",
        "flat_storage_creation_fetched_state_parts",
        "flat_storage_creation_remaining_state_parts",
        "flat_storage_creation_status",
        "flat_storage_creation_threads_used",
        "flat_storage_distance_to_head",
        "flat_storage_head_height",
    ])
});

/// Expect metrics exported by nearcore to have a common prefix. This helps in the following cases:
/// * Avoids name conflicts with metrics from other systems.
/// * Helps filter and query metrics.
/// * Makes it easy to understand which binary export a certain metric.
fn check_metric_near_prefix(name: &str) -> Result<()> {
    // Some metrics were already introduced without the desired prefix.
    // TODO(#9065): Consistent metric naming.
    if name.starts_with("near_") || EXCEPTIONS.contains(name) {
        Ok(())
    } else {
        Err(prometheus::Error::Msg(format!(
            "Metrics are expected to start with 'near_', got {}",
            name
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics::check_metric_near_prefix;

    #[test]
    fn test_near_prefix() {
        assert!(check_metric_near_prefix("near_abc").is_ok());
        assert!(check_metric_near_prefix("flat_storage_head_height").is_ok());
        assert!(check_metric_near_prefix("near").is_err());
        assert!(check_metric_near_prefix("abc").is_err());
    }
}
