use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{fs::File, io::Write};

/// Configures logging.
#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct LogConfig {
    /// Comma-separated list of EnvFitler directives.
    pub rust_log: Option<String>,
    /// Some("") enables global debug logging.
    /// Some("module") enables debug logging for "module".
    pub verbose_module: Option<String>,
    /// Verbosity level of collected traces.
    ///
    /// This is similar to `rust_log` but rather than
    /// [`EnvFilter`](tracing_subscriber::filter::EnvFilter) it uses a simpler
    /// [`Targets`](tracing_subscriber::filter::targets::Targets) filter.
    ///
    /// You can use the usual `debug` or `info` to set the same level for all spans, or customize
    /// individual spans with something like `debug,store::trie=trace` to have specific targets be
    /// more verbose than the default.
    pub opentelemetry: Option<String>,
}

impl LogConfig {
    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut file = File::create(path)?;
        let str = serde_json::to_string_pretty(self)?;
        file.write_all(str.as_bytes())
    }
}
