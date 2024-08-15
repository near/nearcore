use crate::opentelemetry::add_opentelemetry_layer;
use crate::reload::{
    set_default_otlp_level, set_log_layer_handle, set_otlp_layer_handle, LogLayer, SimpleLogLayer,
};
use crate::{log_counter, OpenTelemetryLevel};
use near_crypto::PublicKey;
use near_primitives_core::types::AccountId;
use std::path::PathBuf;
use tracing::subscriber::DefaultGuard;
use tracing_appender::non_blocking::NonBlocking;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{fmt, reload, EnvFilter, Layer};

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
    _writer_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    _io_trace_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

/// Configures exporter of span and trace data.
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
    use std::io::IsTerminal;
    std::io::stderr().is_terminal()
}

fn add_simple_log_layer<S, W>(
    filter: EnvFilter,
    writer: W,
    ansi: bool,
    with_span_events: bool,
    subscriber: S,
) -> SimpleLogLayer<S, W>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
    W: for<'writer> fmt::MakeWriter<'writer> + 'static,
{
    let layer = fmt::layer()
        .with_ansi(ansi)
        .with_span_events(get_fmt_span(with_span_events))
        .with_writer(writer)
        .with_filter(filter);

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

/// The constructed layer writes storage and DB events in a custom format to a
/// specified file.
///
/// This layer is useful to collect detailed IO access patterns for block
/// production. Typically used for debugging IO and to replay on the estimator.
#[cfg(feature = "io_trace")]
pub fn make_io_tracing_layer<S>(
    file: std::fs::File,
) -> (
    tracing_subscriber::filter::Filtered<crate::io_tracer::IoTraceLayer, EnvFilter, S>,
    tracing_appender::non_blocking::WorkerGuard,
)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    use std::io::BufWriter;
    let (base_io_layer, guard) = crate::io_tracer::IoTraceLayer::new(BufWriter::new(file));
    let io_layer = base_io_layer.with_filter(EnvFilter::new(
        "store=trace,vm_logic=trace,host-function=trace,runtime=debug,crate::io_tracer=trace,io_tracer_count=trace",
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

pub(crate) fn use_color_auto() -> bool {
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
    let subscriber = subscriber.with(log_counter::LogCounter::default());
    let subscriber = add_simple_log_layer(
        env_filter,
        make_writer,
        color_output,
        options.log_span_events,
        subscriber,
    );

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
        _writer_guard: None,
        _io_trace_guard: io_trace_guard,
    }
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
    // Installs LogCounter as the innermost layer.
    let subscriber = subscriber.with(log_counter::LogCounter::default());

    set_default_otlp_level(options.opentelemetry);

    let (subscriber, handle) = add_non_blocking_log_layer(
        env_filter,
        writer,
        color_output,
        options.log_span_events,
        subscriber,
    );
    set_log_layer_handle(handle);

    let (subscriber, handle) = add_opentelemetry_layer(
        options.opentelemetry,
        chain_id,
        node_public_key,
        account_id,
        subscriber,
    )
    .await;
    set_otlp_layer_handle(handle);

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
        _writer_guard: Some(writer_guard),
        _io_trace_guard: io_trace_guard,
    }
}
