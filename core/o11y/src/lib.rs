#![doc = include_str!("../README.md")]

pub use {tracing, tracing_appender, tracing_subscriber};

use once_cell::sync::OnceCell;
use std::borrow::Cow;
use tracing_appender::non_blocking::NonBlocking;

use tracing_subscriber::filter::ParseError;
use tracing_subscriber::fmt::format::{DefaultFields, Format};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::Layered;
use tracing_subscriber::reload::{Error, Handle};
use tracing_subscriber::{EnvFilter, Registry};

static ENV_FILTER_RELOAD_HANDLE: OnceCell<
    Handle<EnvFilter, Layered<Layer<Registry, DefaultFields, Format, NonBlocking>, Registry>>,
> = OnceCell::new();

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
/// let filter = near_o11y::EnvFilterBuilder::from_env().finish().unwrap();
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

    let subscriber_builder = tracing_subscriber::FmtSubscriber::builder()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::ENTER
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_writer(writer)
        .with_env_filter(log_filter)
        .with_filter_reloading();
    let reload_handle = subscriber_builder.reload_handle();
    ENV_FILTER_RELOAD_HANDLE.set(reload_handle).unwrap();

    let subscriber = subscriber_builder.finish();
    DefaultSubcriberGuard {
        subscriber: Some(subscriber),
        local_subscriber_guard: None,
        writer_guard,
    }
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ReloadError {
    #[error("could not set the new log filter")]
    Reload(#[source] Error),
    #[error("could not create the log filter")]
    Parse(#[source] BuildEnvFilterError),
    #[error("env_filter reload handle is not available")]
    NoReloadHandle,
}

/// Constructs an `EnvFilter` and sets it as the active filter in the default tracing subscriber.
///
/// The newly constructed `EnvFilter` provides behavior equivalent to what can be obtained via
/// setting `RUST_LOG` environment variable and the `--verbose` command-line flag.
/// `rust_log` is equivalent to setting `RUST_LOG` environment variable.
/// `verbose` indicates whether `--verbose` command-line flag is present.
/// `verbose_module` is equivalent to the value of the `--verbose` command-line flag.
pub fn reload_env_filter(
    rust_log: Option<&str>,
    verbose_module: Option<&str>,
) -> Result<(), ReloadError> {
    ENV_FILTER_RELOAD_HANDLE.get().map_or(Err(ReloadError::NoReloadHandle), |reload_handle| {
        let mut builder = rust_log.map_or_else(
            || EnvFilterBuilder::from_env(),
            |rust_log| EnvFilterBuilder::new(rust_log),
        );
        if let Some(module) = verbose_module {
            builder = builder.verbose(Some(module));
        }
        reload_handle
            .reload(builder.finish().map_err(ReloadError::Parse)?)
            .map_err(ReloadError::Reload)?;
        Ok(())
    })
}

#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum BuildEnvFilterError {
    #[error("could not create a log filter for {1}")]
    CreateEnvFilter(#[source] ParseError, String),
}

#[derive(Debug)]
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
    pub fn finish(self) -> Result<EnvFilter, BuildEnvFilterError> {
        let mut env_filter = EnvFilter::try_new(self.rust_log.clone())
            .map_err(|err| BuildEnvFilterError::CreateEnvFilter(err, self.rust_log.to_string()))?;
        if let Some(module) = self.verbose {
            env_filter = env_filter
                .add_directive("cranelift_codegen=warn".parse().expect("parse directive"))
                .add_directive("h2=warn".parse().expect("parse directive"))
                .add_directive("trust_dns_resolver=warn".parse().expect("parse directive"))
                .add_directive("trust_dns_proto=warn".parse().expect("parse directive"));
            env_filter = if module.is_empty() {
                env_filter.add_directive(tracing::Level::DEBUG.into())
            } else {
                let directive = format!("{}=debug", module).parse().map_err(|err| {
                    BuildEnvFilterError::CreateEnvFilter(
                        err,
                        format!("{}=debug", module).to_string(),
                    )
                })?;
                env_filter.add_directive(directive)
            };
        }
        Ok(env_filter)
    }
}
