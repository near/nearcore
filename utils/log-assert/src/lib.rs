pub use tracing;

/// Asserts that the condition is true, logging an error otherwise.
///
/// This macro complements `assert!` and `debug_assert`. All three macros should
/// only be used for conditions, whose violation signifise a programming error.
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
/// For example, `log_assert` is a great choice to use in some auxilary code
/// paths -- would be a shame if a bug in, eg, metrics collection code brought
/// the whole network down.
///
/// Another use case is adding new asserts to the old code -- if you are only
/// 99% sure that the assert is correct, and there's evidance that the old code
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