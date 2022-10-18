#[macro_export]
macro_rules! handler_span_use_context {
    ($span:expr, $msg:expr) => {{}};
}

/// Use this at the beginning of `handle` methods to reduce the boilerplate.
#[macro_export]
macro_rules! handler_span {
    ($target:expr, $actor:expr, $handler:expr, $msg:expr) => {{
        let reflection_actor =
            std::any::type_name::<Self>().rsplit_once("::").map_or("__", |(_, name)| name);
        let span = tracing::debug_span!(
            target: $target,
            "handle",
            handler = $handler,
            actor = $actor,
            reflection_actor = reflection_actor
        )
        .entered();

        let WithSpanContext { msg, context, .. } = $msg;
        span.set_parent(context);
        (span, msg)
    }};

    // Also specifies type of the received message.
    ($target:expr, $actor:expr, $handler:expr, $msg:expr, $msg_type_fn:expr) => {{
        let WithSpanContext { msg, context, .. } = $msg;
        let msg_type: &'static str = $msg_type_fn(&msg);
        let reflection_actor =
            std::any::type_name::<Self>().rsplit_once("::").map_or("__", |(_, name)| name);
        let span = tracing::debug_span!(
            target: $target,
            "handle",
            handler = $handler,
            actor = $actor,
            reflection_actor = reflection_actor,
            msg_type
        )
        .entered();

        span.set_parent(context);
        (span, msg)
    }};
}
