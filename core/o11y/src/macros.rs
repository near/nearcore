#[macro_export]
macro_rules! handler_span_use_context {
    ($span:expr, $msg:expr) => {{}};
}

/// Use this at the beginning of `handle` methods to reduce the boilerplate.
#[macro_export]
macro_rules! handler_span {
    ($target:expr, $msg:expr) => {{
        let WithSpanContext { msg, context, .. } = $msg;

        fn fix_name(name: &str) -> &str {
            name.rsplit_once("::").map_or("_", |(_, name)| name)
        }
        fn type_name_of<T>(_: &T) -> &str {
            fix_name(std::any::type_name::<T>())
        }

        let actor = fix_name(std::any::type_name::<Self>());
        let handler = type_name_of(&msg);

        let span = tracing::debug_span!(target: $target, "handle", handler, actor).entered();

        span.set_parent(context);
        (span, msg)
    }};

    // Also specifies type of the received message.
    ($target:expr, $msg:expr, $msg_type_fn:expr) => {{
        let WithSpanContext { msg, context, .. } = $msg;

        fn fix_name(name: &str) -> &str {
            name.rsplit_once("::").map_or("_", |(_, name)| name)
        }
        fn type_name_of<T>(_: &T) -> &str {
            fix_name(std::any::type_name::<T>())
        }

        let msg_type: &'static str = $msg_type_fn(&msg);
        let actor = fix_name(std::any::type_name::<Self>());
        let handler = type_name_of(&msg);

        let span =
            tracing::debug_span!(target: $target, "handle", handler, actor, msg_type).entered();

        span.set_parent(context);
        (span, msg)
    }};
}
