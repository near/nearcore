use serde::{Deserialize, Serialize};

/// Configures logging.
#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct LogConfig {
    /// Comma-separated list of EnvFitler directives.
    pub rust_log: Option<String>,
    /// Some("") enables global debug logging.
    /// Some("module") enables debug logging for "module".
    pub verbose_module: Option<String>,
    /// Verbosity level of collected traces.
    pub opentelemetry_level: Option<crate::OpenTelemetryLevel>,
}
