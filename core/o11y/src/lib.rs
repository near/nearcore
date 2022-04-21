#![doc = include_str!("../README.md")]

pub use {tracing, tracing_appender, tracing_subscriber};

use std::borrow::Cow;

use tracing_subscriber::EnvFilter;

/// The default value for the `RUST_LOG` environment variable if one isn't specified otherwise.
pub const DEFAULT_RUST_LOG: &'static str = "tokio_reactor=info,\
     near=info,\
     recompress=info,\
     stats=info,\
     telemetry=info,\
     db=info,\
     delay_detector=info,\
     near-performance-metrics=info,\
     near-rust-allocator-proxy=info,\
     warn";

/// The resource representing a registered subscriber.
///
/// Once dropped, the subscriber is unregistered, and the output is flushed. Any messages output
/// after this value is dropped will be delivered to a previously active subscriber, if any.
pub struct DefaultSubcriberGuard<S> {
    // NB: the field order matters here. I would've used `ManuallyDrop` to indicate this
    // particularity, but somebody decided at some point that doing so is unconventional Rust and
    // that implicit is better than explicit.
    //
    // We must first drop the `local_subscriber_guard` so that no new messages are delivered to
    // this subscriber while we take care of flushing the messages already in queue. If dropped the
    // other way around, the events/spans generated while the subscriber drop guard runs would be
    // lost.
    subscriber: Option<S>,
    local_subscriber_guard: Option<tracing::subscriber::DefaultGuard>,
    #[allow(dead_code)] // This field is never read, but has semantic purpose as a drop guard.
    writer_guard: tracing_appender::non_blocking::WorkerGuard,
}

impl<S: tracing::Subscriber + Send + Sync> DefaultSubcriberGuard<S> {
    /// Register this default subscriber globally , for all threads.
    ///
    /// Must not be called more than once. Mutually exclusive with `Self::local`.
    pub fn global(mut self) -> Self {
        if let Some(subscriber) = self.subscriber.take() {
            tracing::subscriber::set_global_default(subscriber)
                .expect("could not set a global subscriber");
        } else {
            panic!("trying to set a default subscriber that has been already taken")
        }
        self
    }

    /// Register this default subscriber for the current thread.
    ///
    /// Must not be called more than once. Mutually exclusive with `Self::global`.
    pub fn local(mut self) -> Self {
        if let Some(subscriber) = self.subscriber.take() {
            self.local_subscriber_guard = Some(tracing::subscriber::set_default(subscriber));
        } else {
            panic!("trying to set a default subscriber that has been already taken")
        }
        self
    }
}

/// Run the code with a default subscriber set to the option appropriate for the NEAR code.
///
/// This will override any subscribers set until now, and will be in effect until the value
/// returned by this function goes out of scope.
///
/// # Example
///
/// ```rust
/// let filter = near_o11y::EnvFilterBuilder::from_env().finish();
/// let _subscriber = near_o11y::default_subscriber(filter);
/// near_o11y::tracing::info!(message = "Still a lot of work remains to make it proper o11y");
/// ```
pub fn default_subscriber(
    log_filter: EnvFilter,
) -> DefaultSubcriberGuard<impl tracing::Subscriber + Send + Sync> {
    // Do not lock the `stderr` here to allow for things like `dbg!()` work during development.
    let stderr = std::io::stderr();
    let lined_stderr = std::io::LineWriter::new(stderr);
    let (writer, writer_guard) = tracing_appender::non_blocking(lined_stderr);
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::ENTER
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_env_filter(log_filter)
        .with_writer(writer)
        .finish();
    DefaultSubcriberGuard {
        subscriber: Some(subscriber),
        local_subscriber_guard: None,
        writer_guard,
    }
}

pub struct EnvFilterBuilder<'a> {
    rust_log: Cow<'a, str>,
    verbose: Option<Cow<'a, str>>,
}

impl<'a> EnvFilterBuilder<'a> {
    /// Create the `EnvFilter` from the environment variable or the [`DEFAULT_RUST_LOG`] value if
    /// the environment is not set.
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("RUST_LOG").map(Cow::Owned).unwrap_or(Cow::Borrowed(DEFAULT_RUST_LOG)),
        )
    }

    /// Specify an exact `RUST_LOG` value to use.
    ///
    /// This method will not inspect the environment variable.
    pub fn new<S: Into<Cow<'a, str>>>(rust_log: S) -> Self {
        Self { rust_log: rust_log.into(), verbose: None }
    }

    /// Make the produced [`EnvFilter`] verbose.
    ///
    /// If the `module` string is empty, all targets will log debug output. Otherwise only the
    /// specified target will log the debug output.
    pub fn verbose<S: Into<Cow<'a, str>>>(mut self, target: Option<S>) -> Self {
        self.verbose = target.map(Into::into);
        self
    }

    /// Construct an [`EnvFilter`] as configured.
    pub fn finish(self) -> EnvFilter {
        let mut env_filter = EnvFilter::new(self.rust_log);
        if let Some(module) = self.verbose {
            env_filter = env_filter
                .add_directive("cranelift_codegen=warn".parse().expect("parse directive"))
                .add_directive("h2=warn".parse().expect("parse directive"))
                .add_directive("trust_dns_resolver=warn".parse().expect("parse_directive"))
                .add_directive("trust_dns_proto=warn".parse().expect("parse_directive"));
            env_filter = if module.is_empty() {
                env_filter.add_directive(tracing::Level::DEBUG.into())
            } else {
                let directive = format!("{}=debug", module).parse().expect("parse directive");
                env_filter.add_directive(directive)
            };
        }
        env_filter
    }
}
