/// Internal use only.
#[macro_export]
macro_rules! handler_span {
    (target: $target:expr, level: $lvl:expr, $msg:expr, $($extra_fields:tt)*) => {{
        let WithSpanContext { msg, context, .. } = $msg;
        let span = tracing::span!(
            target: $target,
            $lvl,
            "handle",
            handler = near_o11y::macros::type_name_of(&msg),
            actor = near_o11y::macros::last_component_of_name(std::any::type_name::<Self>()),
            $($extra_fields)*)
        .entered();
        span.set_parent(context);
        (span, msg)
    }};
}

/// A macro that lets attach `handle()` functions to the tracing context that
/// generated the actix message being processed.
/// Creates a DEBUG-level span with the handler type name and the message type name as attributes.
#[macro_export]
macro_rules! handler_debug_span {
    (target: $target:expr, $msg:expr) => {
        $crate::handler_span!(target: $target, level: tracing::Level::DEBUG, $msg, )
    };
    (target: $target:expr, $msg:expr, $($extra_fields:tt)*) => {
        $crate::handler_span!(target: $target, level: tracing::Level::DEBUG, $msg, $($extra_fields)*)
    };
}

/// A macro that lets attach `handle()` functions to the tracing context that
/// generated the actix message being processed.
/// Creates a TRACE-level span with the handler type name and the message type name as attributes.
#[macro_export]
macro_rules! handler_trace_span {
    (target: $target:expr, $msg:expr) => {
        $crate::handler_span!(target: $target, level: tracing::Level::TRACE, $msg, )
    };
    (target: $target:expr, $msg:expr, $($extra_fields:tt)*) => {
        $crate::handler_span!(target: $target, level: tracing::Level::TRACE, $msg, $($extra_fields)*)
    };
}

/// For internal use by `handler_span!`.
/// Given 'abc::bcd::cde' returns 'cde'.
/// Given 'abc' returns 'abc'.
pub fn last_component_of_name(name: &str) -> &str {
    name.rsplit_once("::").map_or(name, |(_, name)| name)
}

/// For internal use by `handler_span!`.
/// Returns the last component of the name of type `T`.
pub fn type_name_of<T>(_: &T) -> &str {
    last_component_of_name(std::any::type_name::<T>())
}
