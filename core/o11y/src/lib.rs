#![doc = include_str!("../README.md")]
#![deny(clippy::arithmetic_side_effects)]

pub use context::*;
pub use env_filter::{BuildEnvFilterError, EnvFilterBuilder};
pub use opentelemetry::OpenTelemetryLevel;
pub use reload::{reload, reload_log_config};
#[cfg(feature = "io_trace")]
pub use subscriber::make_io_tracing_layer;
pub use subscriber::{Options, default_subscriber, default_subscriber_with_opentelemetry};
pub use tracing_opentelemetry::OpenTelemetrySpanExt;
pub use {tracing, tracing_appender, tracing_subscriber};

/// Custom tracing subscriber implementation that produces IO traces.
pub mod context;
pub mod env_filter;
mod io_tracer;
pub mod log_config;
mod log_counter;
pub mod macros;
pub mod metrics;
mod opentelemetry;
mod reload;
mod subscriber;
pub mod testonly;

/// Produce a tracing-event for target "io_tracer" that will be consumed by the
/// IO-tracer, if the feature has been enabled.
#[macro_export]
#[cfg(feature = "io_trace")]
macro_rules! io_trace {
    (count: $name:expr) => { tracing::trace!( target: "io_tracer_count", counter = $name) };
    ($($fields:tt)*) => { tracing::trace!( target: "io_tracer", $($fields)*) };
}

#[macro_export]
#[cfg(not(feature = "io_trace"))]
macro_rules! io_trace {
    (count: $name:expr) => {};
    ($($fields:tt)*) => {};
}

/// Asserts that the condition is true, logging an error otherwise.
///
/// This macro complements `assert!` and `debug_assert`. All three macros should
/// only be used for conditions, whose violation signifies a programming error.
/// All three macros are no-ops if the condition is true.
///
/// The behavior when the condition is false (i.e. when the assert fails) is
/// different, and informs different usage patterns.
///
/// `assert!` panics. Use it for sanity-checking invariants, whose violation can
/// compromise correctness of the protocol. For example, it's better to shut a
/// node down via a panic than to admit potentially non-deterministic behavior.
///
/// `debug_assert!` panics if `cfg(debug_assertions)` is true, that is, only
/// during development. In production, `debug_assert!` is compiled away (that
/// is, the condition is not evaluated at all). Use `debug_assert!` if
/// evaluating the condition is too slow. In other words, `debug_assert!` is a
/// performance optimization.
///
/// Finally, `log_assert!` panics in debug mode, while in release mode it emits
/// a `tracing::error!` log line. Use it for sanity-checking non-essential
/// invariants, whose violation signals a bug in the code, where we'd rather
/// avoid shutting the whole node down.
///
/// For example, `log_assert` is a great choice to use in some auxiliary code
/// paths -- would be a shame if a bug in, eg, metrics collection code brought
/// the whole network down.
///
/// Another use case is adding new asserts to the old code -- if you are only
/// 99% sure that the assert is correct, and there's evidence that the old code
/// is working fine in practice, `log_assert!` is the right choice!
///
/// References:
///   * <https://www.sqlite.org/assert.html>
#[macro_export]
macro_rules! log_assert {
    ($cond:expr) => {
        $crate::log_assert!($cond, "assertion failed: {}", stringify!($cond))
    };

    ($cond:expr, $fmt:literal $($arg:tt)*) => {
        if cfg!(debug_assertions) {
            assert!($cond, $fmt $($arg)*);
        } else {
            if !$cond {
                $crate::tracing::error!($fmt $($arg)*);
            }
        }
    };
}

/// The same as 'log_assert' but always fails.
///
/// `log_assert_fail!` panics in debug mode, while in release mode it emits
/// a `tracing::error!` log line. Use it for sanity-checking non-essential
/// invariants, whose violation signals a bug in the code, where we'd rather
/// avoid shutting the whole node down.
#[macro_export]
macro_rules! log_assert_fail {
    ($fmt:literal $($arg:tt)*) => {
        $crate::log_assert!(false, $fmt $($arg)*);
    };
}
