use crate::watchers::{WatchConfigError, Watcher};
use near_o11y::{reload, OpenTelemetryLevel, ReloadError};
use serde::{Deserialize, Serialize};

/// Configures logging.
#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct LogConfig {
    /// Comma-separated list of EnvFitler directives.
    pub rust_log: Option<String>,
    /// Some("") enables global debug logging.
    /// Some("module") enables debug logging for "module".
    pub verbose_module: Option<String>,
    /// Verbosity level of collected traces.
    pub opentelemetry_level: Option<OpenTelemetryLevel>,
}

impl Watcher for LogConfig {
    fn reload(instance: Option<Self>) -> Result<(), WatchConfigError> {
        if let Some(LogConfig { rust_log, verbose_module, opentelemetry_level }) = instance {
            Ok(reload(rust_log.as_deref(), verbose_module.as_deref(), opentelemetry_level)
                .map_err(|e| into_config_err(e))?)
        } else {
            Ok(reload(None, None, None).map_err(|e| into_config_err(e))?)
        }
    }
}

fn into_config_err(reload_errs: Vec<ReloadError>) -> WatchConfigError {
    let error_msgs: Vec<String> = reload_errs.iter().map(|e| e.to_string()).collect();
    WatchConfigError::Reload(error_msgs.join(""))
}
