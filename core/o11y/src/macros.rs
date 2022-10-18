#[macro_export]
macro_rules! handler_span_use_context {
    ($span:expr, $msg:expr) => {{}};
}

/// Use this at the beginning of `handle` methods to reduce the boilerplate.
#[macro_export]
macro_rules! handler_span {
    ($target:expr, $msg:expr $(, $extra_fields:tt)*) => {{
        let WithSpanContext { msg, context, .. } = $msg;

        fn last_component_of_name(name: &str) -> &str {
            name.rsplit_once("::").map_or(name, |(_, name)| name)
        }
        fn type_name_of<T>(_: &T) -> &str {
            last_component_of_name(std::any::type_name::<T>())
        }

        let actor = last_component_of_name(std::any::type_name::<Self>());
        let handler = type_name_of(&msg);

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
