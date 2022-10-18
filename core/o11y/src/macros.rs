/// Use this at the beginning of `handle` methods to reduce the boilerplate.
#[macro_export]
macro_rules! handler_span {
    ($target:expr, $msg:expr $(, $extra_fields:tt)*) => {{
        let WithSpanContext { msg, context, .. } = $msg;

        let actor = near_o11y::macros::last_component_of_name(std::any::type_name::<Self>());
        let handler = near_o11y::macros::type_name_of(&msg);

        let span = tracing::debug_span!(
            target: $target,
            "handle",
            handler,
            actor,
            $($extra_fields)*)
        .entered();

        span.set_parent(context);
        (span, msg)
    }};
}

/// Creates a TRACE level span.
#[macro_export]
macro_rules! handler_trace_span {
    ($target:expr, $msg:expr $(, $extra_fields:tt)*) => {{
        let WithSpanContext { msg, context, .. } = $msg;

        let actor = near_o11y::macros::last_component_of_name(std::any::type_name::<Self>());
        let handler = near_o11y::macros::type_name_of(&msg);

        let span = tracing::trace_span!(
            target: $target,
            "handle",
            handler,
            actor,
            $($extra_fields)*)
        .entered();

        span.set_parent(context);
        (span, msg)
    }};
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
