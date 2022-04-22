use near_o11y::reload_env_filter;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
        // Log to stdout, because otherwise these messages are about controlling logging.
        // If an issue with controlling logging occurs, and logging is disabled, the user may not be
        // able to enable logging.
        println!("Received SIGHUP, reloading logging config");
        match std::fs::read_to_string(&self.watched_path) {
            Ok(log_config_str) => {
                match serde_json::from_str::<LogConfig>(&log_config_str) {
                    Ok(log_config) => {
                        println!("Changing EnvFilter to {:?}", log_config);
                        if let Err(err) = reload_env_filter(
                            log_config.rust_log.as_deref(),
                            log_config.verbose_module.as_deref(),
                        ) {
                            println!("Failed to reload EnvFilter: {:?}", err);
                        }
                        // If file doesn't exist or isn't parse-able, the tail of this function will
                        // reset the config to `RUST_LOG`.
                        return;
                    }
                    Err(err) => {
                        println!("Ignoring the logging config change because failed to parse logging config file {}: {:?}", self.watched_path.display(), err);
                        return;
                    }
                }
            }
            Err(err) => {
                println!(
                    "Resetting EnvFilter, because failed to read logging config file {}: {:?}",
                    self.watched_path.display(),
                    err
                );
            }
        }
        // Reset EnvFilter to `RUST_LOG`.
        if let Err(err) = reload_env_filter(None, None) {
            println!("Failed to reload EnvFilter: {:?}", err);
        }
    }
}
