/// Use this at the beginning of `handle` methods to reduce the boilerplate.
#[macro_export]
macro_rules! handler_span {
    ($target:expr, $actor:expr, $handler:expr, $msg:expr) => {{
        let span =
            tracing::debug_span!(target: $target, "handle", handler = $handler, actor = $actor)
                .entered();
        let WithSpanContext { msg, context, .. } = $msg;
        span.set_parent(context);
        (span, msg)
    }};
}
