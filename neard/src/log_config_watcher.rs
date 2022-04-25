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

pub(crate) enum UpdateBehavior {
    UpdateOrReset,
    UpdateOnlyIfExists,
}

impl LogConfigWatcher {
    pub fn update(&self, update_behavior: UpdateBehavior) {
        match std::fs::read_to_string(&self.watched_path) {
            Ok(log_config_str) => match serde_json::from_str::<LogConfig>(&log_config_str) {
                Ok(log_config) => {
                    info!(target: "neard", log_config=?log_config, "Changing the logging config.");
                    if let Err(err) = reload_env_filter(
                        log_config.rust_log.as_deref(),
                        log_config.verbose_module.as_deref(),
                    ) {
                        error!(target: "neard", err=?err, "Failed to reload the logging config.");
                    }
                    return;
                }
                Err(err) => {
                    error!(target: "neard", logging_config_path=%self.watched_path.display(), err=?err, "Ignoring the logging config change because failed to parse logging config file.");
                    return;
                }
            },
            Err(err) => {
                if let UpdateBehavior::UpdateOrReset = update_behavior {
                    info!(target: "neard", logging_config_path=%self.watched_path.display(), err=?err, "Reset the logging config because the logging config file doesn't exist.");
                    if let Err(err) = reload_env_filter(None, None) {
                        error!(target: "neard", err=?err, "Failed to reload the logging config");
                    }
                }
            }
        }
    }
}
