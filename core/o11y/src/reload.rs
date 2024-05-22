use crate::opentelemetry::get_opentelemetry_filter;
use crate::{log_config, log_counter, BuildEnvFilterError, EnvFilterBuilder, OpenTelemetryLevel};
use once_cell::sync::OnceCell;
use opentelemetry_sdk::trace::Tracer;
use std::str::FromStr as _;
use tracing_appender::non_blocking::NonBlocking;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::{Filtered, Targets};
use tracing_subscriber::layer::Layered;
use tracing_subscriber::reload::Handle;
use tracing_subscriber::{fmt, reload, EnvFilter, Registry};

static LOG_LAYER_RELOAD_HANDLE: OnceCell<
    Handle<EnvFilter, log_counter::LogCountingLayer<Registry>>,
> = OnceCell::new();
static OTLP_LAYER_RELOAD_HANDLE: OnceCell<
    Handle<Targets, LogLayer<log_counter::LogCountingLayer<Registry>>>,
> = OnceCell::new();

// Records the level of opentelemetry tracing verbosity configured via command-line flags at the startup.
static DEFAULT_OTLP_LEVEL: OnceCell<OpenTelemetryLevel> = OnceCell::new();

pub(crate) type LogLayer<Inner> = Layered<
    Filtered<
        fmt::Layer<Inner, fmt::format::DefaultFields, fmt::format::Format, NonBlocking>,
        reload::Layer<EnvFilter, Inner>,
        Inner,
    >,
    Inner,
>;

pub(crate) type SimpleLogLayer<Inner, W> = Layered<
    Filtered<
        fmt::Layer<Inner, fmt::format::DefaultFields, fmt::format::Format, W>,
        EnvFilter,
        Inner,
    >,
    Inner,
>;

pub(crate) type TracingLayer<Inner> = Layered<
    Filtered<OpenTelemetryLayer<Inner, Tracer>, reload::Layer<Targets, Inner>, Inner>,
    Inner,
>;

pub(crate) fn set_log_layer_handle(
    handle: Handle<EnvFilter, log_counter::LogCountingLayer<Registry>>,
) {
    LOG_LAYER_RELOAD_HANDLE
        .set(handle)
        .unwrap_or_else(|_| panic!("Failed to set Log Layer Filter"));
}

pub(crate) fn set_otlp_layer_handle(
    handle: Handle<Targets, LogLayer<log_counter::LogCountingLayer<Registry>>>,
) {
    OTLP_LAYER_RELOAD_HANDLE
        .set(handle)
        .unwrap_or_else(|_| panic!("Failed to set OTLP Layer Filter"));
}

pub(crate) fn set_default_otlp_level(level: OpenTelemetryLevel) {
    // Record the initial tracing level specified as a command-line flag. Use this recorded value to
    // reset opentelemetry filter when the LogConfig file gets deleted.
    DEFAULT_OTLP_LEVEL.set(level).unwrap();
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
    #[error("could not parse the opentelemetry filter")]
    ParseOpentelemetry(#[source] tracing_subscriber::filter::ParseError),
}

pub fn reload_log_config(config: Option<&log_config::LogConfig>) {
    let result = if let Some(config) = config {
        reload(
            config.rust_log.as_deref(),
            config.verbose_module.as_deref(),
            config.opentelemetry.as_deref(),
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
    opentelemetry: Option<&str>,
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

    let opentelemetry_filter = opentelemetry
        .map(|f| Targets::from_str(f).map_err(ReloadError::ParseOpentelemetry))
        .unwrap_or_else(|| {
            Ok(get_opentelemetry_filter(
                *DEFAULT_OTLP_LEVEL.get().unwrap_or(&OpenTelemetryLevel::OFF),
            ))
        });
    let opentelemetry_reload_result = OTLP_LAYER_RELOAD_HANDLE.get().map_or(
        Err(ReloadError::NoOpentelemetryReloadHandle),
        |reload_handle| {
            let opentelemetry_filter = opentelemetry_filter?;
            reload_handle
                .modify(|otlp_filter| {
                    *otlp_filter = opentelemetry_filter;
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
