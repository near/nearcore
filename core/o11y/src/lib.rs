#![doc = include_str!("../README.md")]

pub use {backtrace, tracing, tracing_appender, tracing_subscriber};

use clap::Parser;
use near_crypto::PublicKey;
use near_primitives::types::AccountId;
use once_cell::sync::OnceCell;
use opentelemetry::sdk::trace::{self, RandomIdGenerator, Sampler, Tracer};
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::path::PathBuf;
use tracing::level_filters::LevelFilter;
use tracing::subscriber::DefaultGuard;
use tracing_appender::non_blocking::NonBlocking;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::{Filtered, ParseError};
use tracing_subscriber::fmt::format::{DefaultFields, Format};
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::reload::{Error, Handle};
use tracing_subscriber::{reload, EnvFilter, Layer, Registry};

/// Custom tracing subscriber implementation that produces IO traces.
mod io_tracer;
pub mod metrics;
pub mod testonly;

/// Produce a tracing-event for target "io_tracer" that will be consumed by the
/// IO-tracer, if the feature has been enabled.
#[macro_export]
#[cfg(feature = "io_trace")]
macro_rules! io_trace {
    (count: $name:expr) => { tracing::trace!( target: "io_tracer_count", counter = $name) };
    ($($fields:tt)*) => { tracing::trace!( target: "io_tracer", $($fields)*) };
}

#[macro_export]
#[cfg(not(feature = "io_trace"))]
macro_rules! io_trace {
    (count: $name:expr) => {};
    ($($fields:tt)*) => {};
}

static LOG_LAYER_RELOAD_HANDLE: OnceCell<Handle<EnvFilter, Registry>> = OnceCell::new();

static OTLP_LAYER_RELOAD_HANDLE: OnceCell<
    Handle<
        LevelFilter,
        Layered<
            Filtered<
                tracing_subscriber::fmt::Layer<Registry, DefaultFields, Format, NonBlocking>,
                reload::Layer<EnvFilter, Registry>,
                Registry,
            >,
            Registry,
        >,
    >,
> = OnceCell::new();

// Records the level of opentelemetry tracing verbosity configured via command-line flags at the startup.
static DEFAULT_OTLP_LEVEL: OnceCell<OpenTelemetryLevel> = OnceCell::new();

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
pub struct DefaultSubscriberGuard<S> {
    // NB: the field order matters here. I would've used `ManuallyDrop` to indicate this
    // particularity, but somebody decided at some point that doing so is unconventional Rust and
    // that implicit is better than explicit.
    //
    // We must first drop the `local_subscriber_guard` so that no new messages are delivered to
    // this subscriber while we take care of flushing the messages already in queue. If dropped the
    // other way around, the events/spans generated while the subscriber drop guard runs would be
    // lost.
    pub subscriber: Option<S>,
    pub local_subscriber_guard: Option<DefaultGuard>,
    #[allow(dead_code)] // This field is never read, but has semantic purpose as a drop guard.
    pub writer_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    #[allow(dead_code)] // This field is never read, but has semantic purpose as a drop guard.
    pub io_trace_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

// Doesn't define WARN and ERROR, because the highest verbosity of spans is INFO.
#[derive(Copy, Clone, Debug, clap::ArgEnum, Serialize, Deserialize)]
pub enum OpenTelemetryLevel {
    OFF,
    INFO,
    DEBUG,
    TRACE,
}

impl Default for OpenTelemetryLevel {
    fn default() -> Self {
        OpenTelemetryLevel::OFF
    }
}

/// Configures exporter of span and trace data.
// Currently empty, but more fields will be added in the future.
#[derive(Debug, Default, Parser)]
pub struct Options {
    /// Enables export of span data using opentelemetry exporters.
    #[clap(long, arg_enum, default_value = "off")]
    opentelemetry: OpenTelemetryLevel,

    /// Whether the log needs to be colored.
    #[clap(long, arg_enum, default_value = "auto")]
    color: ColorOutput,

    /// Enable JSON output of IO events, written to a file.
    #[clap(long)]
    record_io_trace: Option<PathBuf>,
}

impl<S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync>
    DefaultSubscriberGuard<S>
{
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

/// Whether to use colored log format.
/// Option `Auto` enables color output only if the logging is done to a terminal and
/// `NO_COLOR` environment variable is not set.
#[derive(clap::ArgEnum, Debug, Clone)]
pub enum ColorOutput {
    Always,
    Never,
    Auto,
}

impl Default for ColorOutput {
    fn default() -> Self {
        ColorOutput::Auto
    }
}

fn is_terminal() -> bool {
    // Crate `atty` provides a platform-independent way of checking whether the output is a tty.
    atty::is(atty::Stream::Stderr)
}

fn add_simple_log_layer<S>(
    filter: EnvFilter,
    ansi: bool,
    subscriber: S,
) -> Layered<Filtered<tracing_subscriber::fmt::Layer<S>, EnvFilter, S>, S>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(ansi)
        // Synthesizing ENTER and CLOSE events lets us log durations of spans to the log.
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::ENTER
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_filter(filter);

    let subscriber = subscriber.with(layer);
    subscriber
}

fn add_log_layer<S>(
    filter: EnvFilter,
    writer: NonBlocking,
    ansi: bool,
    subscriber: S,
) -> (
    Layered<
        Filtered<
            tracing_subscriber::fmt::Layer<S, DefaultFields, Format, NonBlocking>,
            reload::Layer<EnvFilter, S>,
            S,
        >,
        S,
    >,
    Handle<EnvFilter, S>,
)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let (filter, handle) = reload::Layer::<EnvFilter, S>::new(filter);

    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(ansi)
        // Synthesizing ENTER and CLOSE events lets us log durations of spans to the log.
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::ENTER
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_writer(writer)
        .with_filter(filter);

    let subscriber = subscriber.with(layer);
    (subscriber, handle)
}

/// Constructs an OpenTelemetryConfig which sends span data to an external collector.
//
// NB: this function is `async` because `install_batch(Tokio)` requires a tokio context to
// register timers and channels and whatnot.
async fn add_opentelemetry_layer<S>(
    opentelemetry_level: OpenTelemetryLevel,
    chain_id: String,
    node_public_key: PublicKey,
    account_id: Option<AccountId>,
    subscriber: S,
) -> (
    Layered<Filtered<OpenTelemetryLayer<S, Tracer>, reload::Layer<LevelFilter, S>, S>, S>,
    Handle<LevelFilter, S>,
)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let filter = get_opentelemetry_filter(opentelemetry_level);
    let (filter, handle) = reload::Layer::<LevelFilter, S>::new(filter);

    let mut resource = vec![
        KeyValue::new(SERVICE_NAME, "neard"),
        KeyValue::new("chain_id", chain_id),
        KeyValue::new("node_id", node_public_key.to_string()),
    ];
    if let Some(account_id) = account_id {
        resource.push(KeyValue::new("account_id", account_id.to_string()));
    }

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(Resource::new(resource)),
        )
        .install_batch(opentelemetry::runtime::Tokio)
        .unwrap();
    let layer = tracing_opentelemetry::layer().with_tracer(tracer).with_filter(filter);
    let subscriber = subscriber.with(layer);
    (subscriber, handle)
}

pub fn get_opentelemetry_filter(opentelemetry_level: OpenTelemetryLevel) -> LevelFilter {
    match opentelemetry_level {
        OpenTelemetryLevel::OFF => LevelFilter::OFF,
        OpenTelemetryLevel::INFO => LevelFilter::INFO,
        OpenTelemetryLevel::DEBUG => LevelFilter::DEBUG,
        OpenTelemetryLevel::TRACE => LevelFilter::TRACE,
    }
}

/// The constructed layer writes storage and DB events in a custom format to a
/// specified file.
///
/// This layer is useful to collect detailed IO access patterns for block
/// production. Typically used for debugging IO and to replay on the estimator.
#[cfg(feature = "io_trace")]
pub fn make_io_tracing_layer<S>(
    file: std::fs::File,
) -> (Filtered<io_tracer::IoTraceLayer, EnvFilter, S>, tracing_appender::non_blocking::WorkerGuard)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    use std::io::BufWriter;
    let (base_io_layer, guard) = io_tracer::IoTraceLayer::new(BufWriter::new(file));
    let io_layer = base_io_layer.with_filter(tracing_subscriber::filter::EnvFilter::new(
        "store=trace,vm_logic=trace,host-function=trace,runtime=debug,io_tracer=trace,io_tracer_count=trace",
    ));
    (io_layer, guard)
}

/// Run the code with a default subscriber set to the option appropriate for the NEAR code.
///
/// This will override any subscribers set until now, and will be in effect until the value
/// returned by this function goes out of scope.
/// Subscriber creation needs an async runtime.
///
/// # Example
///
/// ```rust
/// let runtime = tokio::runtime::Runtime::new().unwrap();
/// let filter = near_o11y::EnvFilterBuilder::from_env().finish().unwrap();
/// let _subscriber = runtime.block_on(async {
///     near_o11y::default_subscriber(filter, &Default::default()).await.global()
/// });
/// ```
pub fn default_subscriber_logging(
    verbose: Option<&str>,
    options: &Options,
) -> DefaultSubscriberGuard<impl tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync> {
    let env_filter = EnvFilterBuilder::from_env().verbose(verbose).finish().unwrap();
    // Sandbox node can log to sandbox logging target via sandbox_debug_log host function.
    // This is hidden by default so we enable it for sandbox node.
    let env_filter = if cfg!(feature = "sandbox") {
        env_filter.add_directive("sandbox=debug".parse().unwrap())
    } else {
        env_filter
    };

    let ansi = match options.color {
        ColorOutput::Always => true,
        ColorOutput::Never => false,
        ColorOutput::Auto => std::env::var_os("NO_COLOR").is_none() && is_terminal(),
    };

    let subscriber = tracing_subscriber::registry();
    let subscriber = add_simple_log_layer(env_filter, ansi, subscriber);

    DefaultSubscriberGuard {
        subscriber: Some(subscriber),
        local_subscriber_guard: None,
        writer_guard: None,
        io_trace_guard: None,
    }
}

pub async fn default_subscriber_everything(
    verbose: Option<&str>,
    options: &Options,
    chain_id: String,
    node_public_key: PublicKey,
    account_id: Option<AccountId>,
) -> DefaultSubscriberGuard<impl tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync> {
    let env_filter = EnvFilterBuilder::from_env().verbose(verbose).finish().unwrap();
    // Sandbox node can log to sandbox logging target via sandbox_debug_log host function.
    // This is hidden by default so we enable it for sandbox node.
    let env_filter = if cfg!(feature = "sandbox") {
        env_filter.add_directive("sandbox=debug".parse().unwrap())
    } else {
        env_filter
    };

    // Do not lock the `stderr` here to allow for things like `dbg!()` work during development.
    let stderr = std::io::stderr();
    let lined_stderr = std::io::LineWriter::new(stderr);
    let (writer, writer_guard) = tracing_appender::non_blocking(lined_stderr);

    let ansi = match options.color {
        ColorOutput::Always => true,
        ColorOutput::Never => false,
        ColorOutput::Auto => std::env::var_os("NO_COLOR").is_none() && is_terminal(),
    };

    let subscriber = tracing_subscriber::registry();
    // Record the initial OTLP level specified as a command-line flag. Use this recorded value to
    // reset opentelemetry filter when the LogConfig file gets deleted.
    DEFAULT_OTLP_LEVEL.set(options.opentelemetry).unwrap();

    let (subscriber, handle) = add_log_layer(env_filter, writer, ansi, subscriber);
    LOG_LAYER_RELOAD_HANDLE
        .set(handle)
        .unwrap_or_else(|_| panic!("Failed to set Log Layer Filter"));

    let (subscriber, handle) = add_opentelemetry_layer(
        options.opentelemetry,
        chain_id,
        node_public_key,
        account_id,
        subscriber,
    )
    .await;
    OTLP_LAYER_RELOAD_HANDLE
        .set(handle)
        .unwrap_or_else(|_| panic!("Failed to set OTLP Layer Filter"));

    #[allow(unused_mut)]
    let mut io_trace_guard = None;
    #[cfg(feature = "io_trace")]
    let subscriber = subscriber.with(options.record_io_trace.as_ref().map(|output_path| {
        let (sub, guard) = make_io_tracing_layer(
            std::fs::File::create(output_path)
                .expect("unable to create or truncate IO trace output file"),
        );
        io_trace_guard = Some(guard);
        sub
    }));

    DefaultSubscriberGuard {
        subscriber: Some(subscriber),
        local_subscriber_guard: None,
        writer_guard: Some(writer_guard),
        io_trace_guard,
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

/// Constructs new filters for the logging and opentelemetry layers.
///
/// The newly constructed `EnvFilter` provides behavior equivalent to what can be obtained via
/// setting `RUST_LOG` environment variable and the `--verbose` command-line flag.
/// `rust_log` is equivalent to setting `RUST_LOG` environment variable.
/// `verbose` indicates whether `--verbose` command-line flag is present.
/// `verbose_module` is equivalent to the value of the `--verbose` command-line flag.
pub fn reload_layers(
    rust_log: Option<&str>,
    verbose_module: Option<&str>,
    opentelemetry_level: Option<OpenTelemetryLevel>,
) -> Result<(), ReloadError> {
    LOG_LAYER_RELOAD_HANDLE.get().map_or(Err(ReloadError::NoReloadHandle), |reload_handle| {
        let mut builder = rust_log.map_or_else(
            || EnvFilterBuilder::from_env(),
            |rust_log| EnvFilterBuilder::new(rust_log),
        );
        if let Some(module) = verbose_module {
            builder = builder.verbose(Some(module));
        }
        let env_filter = builder.finish().map_err(ReloadError::Parse)?;

        reload_handle
            .modify(|log_filter| {
                *log_filter = env_filter;
            })
            .map_err(ReloadError::Reload)?;
        Ok(())
    })?;

    let opentelemetry_level = opentelemetry_level
        .unwrap_or(*DEFAULT_OTLP_LEVEL.get().unwrap_or(&OpenTelemetryLevel::OFF));
    OTLP_LAYER_RELOAD_HANDLE.get().map_or(Err(ReloadError::NoReloadHandle), |reload_handle| {
        reload_handle
            .modify(|otlp_layer| {
                *otlp_layer = get_opentelemetry_filter(opentelemetry_level);
            })
            .map_err(ReloadError::Reload)?;
        Ok(())
    })?;

    Ok(())
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
    verbose: Option<&'a str>,
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
    pub fn verbose(mut self, target: Option<&'a str>) -> Self {
        self.verbose = target;
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
                    BuildEnvFilterError::CreateEnvFilter(err, format!("{}=debug", module))
                })?;
                env_filter.add_directive(directive)
            };
        }
        Ok(env_filter)
    }
}

/// Prints backtrace to stderr.
///
/// This is intended as a printf-debugging aid.
pub fn print_backtrace() {
    let bt = backtrace::Backtrace::new();
    eprintln!("{bt:?}")
}

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
