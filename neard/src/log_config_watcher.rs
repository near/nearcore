use near_o11y::reload_env_filter;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{error, info};

/// Configures logging.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LogConfig {
    /// Comma-separated list of EnvFitler directives.
    pub rust_log: Option<String>,
    /// Some("") enables global debug logging.
    /// Some("module") enables debug logging for "module".
    pub verbose_module: Option<String>,
}

pub(crate) struct LogConfigWatcher {
    pub watched_path: PathBuf,
}

impl LogConfigWatcher {
    pub fn update(&self) {
        info!(target: "neard", "Received SIGHUP, reloading logging config");
        match std::fs::read_to_string(&self.watched_path) {
            Ok(log_config_str) => {
                match serde_json::from_str::<LogConfig>(&log_config_str) {
                    Ok(log_config) => {
                        info!(target: "neard", "Changing EnvFilter to {:?}", log_config);
                        if let Err(err) = reload_env_filter(
                            log_config.rust_log.as_deref(),
                            log_config.verbose_module.as_deref(),
                        ) {
                            error!(target: "neard", "Failed to reload EnvFilter: {:?}", err);
                        }
                        // If file doesn't exist or isn't parse-able, the tail of this function will
                        // reset the config to `RUST_LOG`.
                        return;
                    }
                    Err(err) => {
                        error!(target: "neard", "Ignoring the logging config change because failed to parse logging config file {}: {:?}", self.watched_path.display(), err);
                        return;
                    }
                }
            }
            Err(err) => {
                info!(target: "neard",
                    "Resetting EnvFilter, because failed to read logging config file {}: {:?}",
                    self.watched_path.display(),
                    err
                );
            }
        }
        // Reset EnvFilter to `RUST_LOG`.
        if let Err(err) = reload_env_filter(None, None) {
            error!(target: "neard", "Failed to reload EnvFilter: {:?}", err);
        }
    }
}
