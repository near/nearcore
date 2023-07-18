#![doc = include_str!("../README.md")]
#![deny(clippy::arithmetic_side_effects)]

pub use context::*;
use near_crypto::PublicKey;
use near_primitives_core::types::AccountId;
use once_cell::sync::OnceCell;
use opentelemetry::sdk::trace::{self, IdGenerator, Sampler, Tracer};
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use std::borrow::Cow;
use std::path::PathBuf;
use tracing::level_filters::LevelFilter;
use tracing::subscriber::DefaultGuard;
use tracing_appender::non_blocking::NonBlocking;
use tracing_opentelemetry::OpenTelemetryLayer;
pub use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{Filtered, ParseError};
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{fmt, reload, EnvFilter, Layer, Registry};
pub use {tracing, tracing_appender, tracing_subscriber};

/// Custom tracing subscriber implementation that produces IO traces.
pub mod context;
mod io_tracer;
pub mod log_config;
pub mod macros;
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

static LOG_LAYER_RELOAD_HANDLE: OnceCell<reload::Handle<EnvFilter, Registry>> = OnceCell::new();
static OTLP_LAYER_RELOAD_HANDLE: OnceCell<reload::Handle<LevelFilter, LogLayer<Registry>>> =
    OnceCell::new();

type LogLayer<Inner> = Layered<
    Filtered<
        fmt::Layer<Inner, fmt::format::DefaultFields, fmt::format::Format, NonBlocking>,
        reload::Layer<EnvFilter, Inner>,
        Inner,
    >,
    Inner,
>;

type SimpleLogLayer<Inner, W> = Layered<
    Filtered<
        fmt::Layer<Inner, fmt::format::DefaultFields, fmt::format::Format, W>,
        EnvFilter,
        Inner,
    >,
    Inner,
>;

type TracingLayer<Inner> = Layered<
    Filtered<OpenTelemetryLayer<Inner, Tracer>, reload::Layer<LevelFilter, Inner>, Inner>,
    Inner,
>;

// Records the level of opentelemetry tracing verbosity configured via command-line flags at the startup.
static DEFAULT_OTLP_LEVEL: OnceCell<OpenTelemetryLevel> = OnceCell::new();

/// The default value for the `RUST_LOG` environment variable if one isn't specified otherwise.
pub const DEFAULT_RUST_LOG: &str = "tokio_reactor=info,\
     config=info,\
     near=info,\
     recompress=info,\
     stats=info,\
     telemetry=info,\
     db=info,\
     delay_detector=info,\
     near-performance-metrics=info,\
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
    subscriber: Option<S>,
    local_subscriber_guard: Option<DefaultGuard>,
    #[allow(dead_code)] // This field is never read, but has semantic purpose as a drop guard.
    writer_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    #[allow(dead_code)] // This field is never read, but has semantic purpose as a drop guard.
    io_trace_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

// Doesn't define WARN and ERROR, because the highest verbosity of spans is INFO.
#[derive(Copy, Clone, Debug, Default, clap::ValueEnum, serde::Serialize, serde::Deserialize)]
pub enum OpenTelemetryLevel {
    #[default]
    OFF,
    INFO,
    DEBUG,
    TRACE,
}

/// Configures exporter of span and trace data.
// Currently empty, but more fields will be added in the future.
#[derive(Debug, Default, clap::Parser)]
pub struct Options {
    /// Enables export of span data using opentelemetry exporters.
    #[clap(long, value_enum, default_value = "off")]
    opentelemetry: OpenTelemetryLevel,

    /// Whether the log needs to be colored.
    #[clap(long, value_enum, default_value = "auto")]
    color: ColorOutput,

    /// Enable logging of spans. For instance, this prints timestamps of entering and exiting a span,
    /// together with the span duration and used/idle CPU time.
    #[clap(long)]
    log_span_events: bool,

    /// Enable JSON output of IO events, written to a file.
    #[clap(long)]
    record_io_trace: Option<PathBuf>,
}

impl<S: tracing::Subscriber + Send + Sync> DefaultSubscriberGuard<S> {
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
#[derive(clap::ValueEnum, Debug, Clone, Default)]
pub enum ColorOutput {
    #[default]
    Always,
    Never,
    Auto,
}

fn is_terminal() -> bool {
    // Crate `atty` provides a platform-independent way of checking whether the output is a tty.
    atty::is(atty::Stream::Stderr)
}

fn add_simple_log_layer<S, W>(
    filter: EnvFilter,
    writer: W,
    ansi: bool,
    subscriber: S,
) -> SimpleLogLayer<S, W>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
    W: for<'writer> fmt::MakeWriter<'writer> + 'static,
{
    let layer = fmt::layer().with_ansi(ansi).with_writer(writer).with_filter(filter);

    subscriber.with(layer)
}

fn get_fmt_span(with_span_events: bool) -> fmt::format::FmtSpan {
    if with_span_events {
        fmt::format::FmtSpan::ENTER | fmt::format::FmtSpan::CLOSE
    } else {
        fmt::format::FmtSpan::NONE
    }
}

fn add_non_blocking_log_layer<S>(
    filter: EnvFilter,
    writer: NonBlocking,
    ansi: bool,
    with_span_events: bool,
    subscriber: S,
) -> (LogLayer<S>, reload::Handle<EnvFilter, S>)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let (filter, handle) = reload::Layer::<EnvFilter, S>::new(filter);

    let layer = fmt::layer()
        .with_ansi(ansi)
        .with_span_events(get_fmt_span(with_span_events))
        .with_writer(writer)
        .with_filter(filter);

    (subscriber.with(layer), handle)
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
) -> (TracingLayer<S>, reload::Handle<LevelFilter, S>)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let filter = get_opentelemetry_filter(opentelemetry_level);
    let (filter, handle) = reload::Layer::<LevelFilter, S>::new(filter);

    let mut resource = vec![
        KeyValue::new("chain_id", chain_id),
        KeyValue::new("node_id", node_public_key.to_string()),
    ];
    // Prefer account name as the node name.
    // Fallback to a node public key if a validator key is unavailable.
    let service_name = if let Some(account_id) = account_id {
        resource.push(KeyValue::new("account_id", account_id.to_string()));
        format!("neard:{}", account_id)
    } else {
        format!("neard:{}", node_public_key)
    };
    resource.push(KeyValue::new(SERVICE_NAME, service_name));

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(IdGenerator::default())
                .with_resource(Resource::new(resource)),
        )
        .install_batch(opentelemetry::runtime::Tokio)
        .unwrap();
    let layer = tracing_opentelemetry::layer().with_tracer(tracer).with_filter(filter);
    (subscriber.with(layer), handle)
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

fn use_color_output(options: &Options) -> bool {
    match options.color {
        ColorOutput::Always => true,
        ColorOutput::Never => false,
        ColorOutput::Auto => use_color_auto(),
    }
}

fn use_color_auto() -> bool {
    std::env::var_os("NO_COLOR").is_none() && is_terminal()
}

/// Constructs a subscriber set to the option appropriate for the NEAR code.
///
/// Subscriber enables only logging.
///
/// # Example
///
/// ```rust
/// let filter = near_o11y::EnvFilterBuilder::from_env().finish().unwrap();
/// let _subscriber = near_o11y::default_subscriber(filter, &Default::default()).global();
/// ```
pub fn default_subscriber(
    env_filter: EnvFilter,
    options: &Options,
) -> DefaultSubscriberGuard<impl tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync> {
    let color_output = use_color_output(options);

    let make_writer = || {
        let stderr = std::io::stderr();
        std::io::LineWriter::new(stderr)
    };

    let subscriber = tracing_subscriber::registry();
    let subscriber = add_simple_log_layer(env_filter, make_writer, color_output, subscriber);

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
        writer_guard: None,
        io_trace_guard,
    }
}

pub fn set_default_otlp_level(options: &Options) {
    // Record the initial tracing level specified as a command-line flag. Use this recorded value to
    // reset opentelemetry filter when the LogConfig file gets deleted.
    DEFAULT_OTLP_LEVEL.set(options.opentelemetry).unwrap();
}

/// Constructs a subscriber set to the option appropriate for the NEAR code.
///
/// The subscriber enables logging, tracing and io tracing.
/// Subscriber creation needs an async runtime.
pub async fn default_subscriber_with_opentelemetry(
    env_filter: EnvFilter,
    options: &Options,
    chain_id: String,
    node_public_key: PublicKey,
    account_id: Option<AccountId>,
) -> DefaultSubscriberGuard<impl tracing::Subscriber + Send + Sync> {
    let color_output = use_color_output(options);

    // Do not lock the `stderr` here to allow for things like `dbg!()` work during development.
    let stderr = std::io::stderr();
    let lined_stderr = std::io::LineWriter::new(stderr);
    let (writer, writer_guard) = tracing_appender::non_blocking(lined_stderr);

    let subscriber = tracing_subscriber::registry();

    set_default_otlp_level(options);

    let (subscriber, handle) = add_non_blocking_log_layer(
        env_filter,
        writer,
        color_output,
        options.log_span_events,
        subscriber,
    );
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
    #[error("env_filter reload handle is not available")]
    NoLogReloadHandle,
    #[error("opentelemetry reload handle is not available")]
    NoOpentelemetryReloadHandle,
    #[error("could not set the new log filter")]
    ReloadLogLayer(#[source] reload::Error),
    #[error("could not set the new opentelemetry filter")]
    ReloadOpentelemetryLayer(#[source] reload::Error),
    #[error("could not create the log filter")]
    Parse(#[source] BuildEnvFilterError),
}

pub fn reload_log_config(config: Option<&log_config::LogConfig>) {
    let result = if let Some(config) = config {
        reload(
            config.rust_log.as_deref(),
            config.verbose_module.as_deref(),
            config.opentelemetry_level,
        )
    } else {
        // When the LOG_CONFIG_FILENAME is not available, reset to the tracing and logging config
        // when the node was started.
        reload(None, None, None)
    };
    match result {
        Ok(_) => {
            tracing::info!("Updated the logging layer according to `log_config.json`");
        }
        Err(err) => {
            eprintln!(
                "Failed to update the logging layer according to the changed `log_config.json`. Errors: {:?}",
                err
            );
        }
    }
}

/// Constructs new filters for the logging and opentelemetry layers.
///
/// Attempts to reload all available errors. Returns errors for each layer that failed to reload.
///
/// The newly constructed `EnvFilter` provides behavior equivalent to what can be obtained via
/// setting `RUST_LOG` environment variable and the `--verbose` command-line flag.
/// `rust_log` is equivalent to setting `RUST_LOG` environment variable.
/// `verbose` indicates whether `--verbose` command-line flag is present.
/// `verbose_module` is equivalent to the value of the `--verbose` command-line flag.
pub fn reload(
    rust_log: Option<&str>,
    verbose_module: Option<&str>,
    opentelemetry_level: Option<OpenTelemetryLevel>,
) -> Result<(), Vec<ReloadError>> {
    let log_reload_result = LOG_LAYER_RELOAD_HANDLE.get().map_or(
        Err(ReloadError::NoLogReloadHandle),
        |reload_handle| {
            let mut builder =
                rust_log.map_or_else(EnvFilterBuilder::from_env, EnvFilterBuilder::new);
            if let Some(module) = verbose_module {
                builder = builder.verbose(Some(module));
            }
            let env_filter = builder.finish().map_err(ReloadError::Parse)?;

            reload_handle
                .modify(|log_filter| {
                    *log_filter = env_filter;
                })
                .map_err(ReloadError::ReloadLogLayer)?;
            Ok(())
        },
    );

    let opentelemetry_level = opentelemetry_level
        .unwrap_or(*DEFAULT_OTLP_LEVEL.get().unwrap_or(&OpenTelemetryLevel::OFF));
    let opentelemetry_reload_result = OTLP_LAYER_RELOAD_HANDLE.get().map_or(
        Err(ReloadError::NoOpentelemetryReloadHandle),
        |reload_handle| {
            reload_handle
                .modify(|otlp_filter| {
                    *otlp_filter = get_opentelemetry_filter(opentelemetry_level);
                })
                .map_err(ReloadError::ReloadOpentelemetryLayer)?;
            Ok(())
        },
    );

    let mut errors: Vec<ReloadError> = vec![];
    if let Err(err) = log_reload_result {
        errors.push(err);
    }
    if let Err(err) = opentelemetry_reload_result {
        errors.push(err);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
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
                .add_directive("tower=warn".parse().expect("parse directive"))
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
    let bt = std::backtrace::Backtrace::force_capture();
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

/// The same as 'log_assert' but always fails.
///
/// `log_assert_fail!` panics in debug mode, while in release mode it emits
/// a `tracing::error!` log line. Use it for sanity-checking non-essential
/// invariants, whose violation signals a bug in the code, where we'd rather
/// avoid shutting the whole node down.
#[macro_export]
macro_rules! log_assert_fail {
    ($fmt:literal $($arg:tt)*) => {
        $crate::log_assert!(false, $fmt $($arg)*);
    };
}
