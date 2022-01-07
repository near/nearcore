//! A fork of the lighthouse_metrics crate used to implement prometheus
//!
//! A wrapper around the `prometheus` crate that provides a global, `lazy_static` metrics registry
//! and functions to add and use the following components (more info at
//! [Prometheus docs](https://prometheus.io/docs/concepts/metric_types/)):
//!
//! - `Histogram`: used with `start_timer(..)` and `stop_timer(..)` to record durations (e.g.,
//! block processing time).
//! - `IncCounter`: used to represent an ideally ever-growing, never-shrinking integer (e.g.,
//! number of block processing requests).
//! - `IntGauge`: used to represent an varying integer (e.g., number of attestations per block).
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
//! #[macro_use]
//! extern crate lazy_static;
//! use near_metrics::*;
//!
//! // These metrics are "magically" linked to the global registry defined in `lighthouse_metrics`.
//! lazy_static! {
//!     pub static ref RUN_COUNT: Result<IntCounter> = try_create_int_counter(
//!         "runs_total",
//!         "Total number of runs"
//!     );
//!     pub static ref CURRENT_VALUE: Result<IntGauge> = try_create_int_gauge(
//!         "current_value",
//!         "The current value"
//!     );
//!     pub static ref RUN_TIME: Result<Histogram> =
//!         try_create_histogram("run_seconds", "Time taken (measured to high precision)");
//! }
//!
//!
//! fn main() {
//!     let run_count: Result<IntCounter> = try_create_int_counter(&"RUN_COUNT", &"RUN_COUNT");
//!     let run_time: Result<Histogram>  = try_create_histogram("RUN_TIME", &"RUN_TIME");
//!     let current_value: Result<IntGauge>  = try_create_int_gauge("CURRENT_VALUE", "RUN_COUNT");
//!     for i in 0..100 {
//!         inc_counter(&run_count);
//!         let timer = start_timer(&run_time);
//!
//!         for j in 0..10 {
//!             set_gauge(&current_value, j);
//!             println!("Howdy partner");
//!         }
//!
//!         stop_timer(timer);
//!     }
//! }
//! ```

pub use prometheus::{
    Encoder, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Result,
    TextEncoder,
};
use prometheus::{HistogramOpts, HistogramTimer, Opts};

use log::error;

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

/// Attempts to crate a `Histogram`, returning `Err` if the registry does not accept the counter
/// (potentially due to naming conflict).
pub fn try_create_histogram(name: &str, help: &str) -> Result<Histogram> {
    let opts = HistogramOpts::new(name, help);
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

/// Starts a timer for the given `Histogram`, stopping when it gets dropped or given to `stop_timer(..)`.
pub fn start_timer(histogram: &Result<Histogram>) -> Option<HistogramTimer> {
    if let Ok(histogram) = histogram {
        Some(histogram.start_timer())
    } else {
        error!(target: "metrics", "Failed to fetch histogram");
        None
    }
}

/// Starts a timer for the given `HistogramVec` and labels, stopping when it gets dropped or given to `stop_timer(..)`.
pub fn start_timer_vec(
    histogram: &Result<HistogramVec>,
    label_values: &[&str],
) -> Option<HistogramTimer> {
    if let Ok(histogram) = histogram {
        Some(histogram.with_label_values(label_values).start_timer())
    } else {
        error!(target: "metrics", "Failed to fetch histogram");
        None
    }
}

/// Sets the value of a `Histogram` manually.
pub fn observe(histogram: &Result<Histogram>, value: f64) {
    if let Ok(histogram) = histogram {
        histogram.observe(value);
    } else {
        error!(target: "metrics", "Failed to fetch histogram");
    }
}

/// Stops a timer created with `start_timer(..)`.
pub fn stop_timer(timer: Option<HistogramTimer>) {
    if let Some(t) = timer {
        t.observe_duration();
    }
}

pub fn inc_counter(counter: &Result<IntCounter>) {
    if let Ok(counter) = counter {
        counter.inc();
    } else {
        error!(target: "metrics", "Failed to fetch counter");
    }
}

pub fn inc_counter_vec(counter: &Result<IntCounterVec>, label_values: &[&str]) {
    if let Ok(counter) = counter {
        counter.with_label_values(label_values).inc();
    } else {
        error!(target: "metrics", "Failed to fetch counter");
    }
}

pub fn inc_counter_opt(counter: Option<&IntCounter>) {
    if let Some(counter) = counter {
        counter.inc();
    }
}

pub fn get_counter(counter: &Result<IntCounter>) -> std::result::Result<u64, String> {
    if let Ok(counter) = counter {
        Ok(counter.get())
    } else {
        Err("Failed to fetch counter".to_string())
    }
}

pub fn inc_counter_by(counter: &Result<IntCounter>, value: u64) {
    if let Ok(counter) = counter {
        counter.inc_by(value);
    } else {
        error!(target: "metrics", "Failed to fetch histogram");
    }
}

pub fn inc_counter_by_opt(counter: Option<&IntCounter>, value: u64) {
    if let Some(counter) = counter {
        counter.inc_by(value);
    }
}

pub fn set_gauge(gauge: &Result<IntGauge>, value: i64) {
    if let Ok(gauge) = gauge {
        gauge.set(value);
    } else {
        error!(target: "metrics", "Failed to fetch gauge");
    }
}

pub fn inc_gauge(gauge: &Result<IntGauge>) {
    if let Ok(gauge) = gauge {
        gauge.inc();
    } else {
        error!(target: "metrics", "Failed to fetch gauge");
    }
}

pub fn dec_gauge(gauge: &Result<IntGauge>) {
    if let Ok(gauge) = gauge {
        gauge.dec();
    } else {
        error!(target: "metrics", "Failed to fetch gauge");
    }
}
pub fn get_gauge(gauge: &Result<IntGauge>) -> std::result::Result<i64, String> {
    if let Ok(gauge) = gauge {
        Ok(gauge.get())
    } else {
        Err("Failed to fetch gauge".to_string())
    }
}
