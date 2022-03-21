use tracing_subscriber::EnvFilter;

/// The default value for the `RUST_LOG` environment variable if one isn't specified otherwise.
pub const DEFAULT_RUST_LOG: &'static str = "tokio_reactor=info,\
     near=info,\
     stats=info,\
     telemetry=info,\
     delay_detector=info,\
     near-performance-metrics=info,\
     near-rust-allocator-proxy=info,\
     warn";

/// Run the code with a default subscriber set to the option appropriate for the NEAR code.
pub fn with_default_subscriber<R, F>(log_filter: EnvFilter, run: F) -> R
where
    F: FnOnce() -> R,
{
    // Do not lock the `stderr` here to allow for things like `dbg!()` work during development.
    let stderr = std::io::stderr();
    let lined_stderr = std::io::LineWriter::new(stderr);
    let (writer, _writer_guard) = tracing_appender::non_blocking(lined_stderr);
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::ENTER
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_env_filter(log_filter)
        .with_writer(writer)
        .finish();
    tracing::subscriber::with_default(subscriber, run)
}
