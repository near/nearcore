use near_o11y::{reload_env_filter, ReloadError};
use serde::{Deserialize, Serialize};
use std::io;
use std::io::ErrorKind;
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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
enum LogConfigError {
    #[error("Failed to reload the logging config")]
    Reload(#[source] ReloadError),
    #[error("Failed to reload the logging config")]
    Parse(#[source] serde_json::Error),
    #[error("Can't open or read the logging config file")]
    OpenAndRead(#[source] io::Error),
}

impl LogConfigWatcher {
    fn do_update(&self, update_behavior: UpdateBehavior) -> Result<(), LogConfigError> {
        match std::fs::read_to_string(&self.watched_path) {
            Ok(log_config_str) => {
                let log_config = serde_json::from_str::<LogConfig>(&log_config_str)
                    .map_err(LogConfigError::Parse)?;
                info!(target: "neard", log_config=?log_config, "Changing the logging config.");
                return reload_env_filter(
                    log_config.rust_log.as_deref(),
                    log_config.verbose_module.as_deref(),
                )
                .map_err(LogConfigError::Reload);
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {
                    if let UpdateBehavior::UpdateOrReset = update_behavior {
                        info!(target: "neard", logging_config_path=%self.watched_path.display(), ?err, "Reset the logging config because the logging config file doesn't exist.");
                        return reload_env_filter(None, None).map_err(LogConfigError::Reload);
                    }
                    Ok(())
                }
                _ => Err(err).map_err(LogConfigError::OpenAndRead),
            },
        }
    }

    pub fn update(&self, update_behavior: UpdateBehavior) {
        if let Err(err) = self.do_update(update_behavior) {
            error!(target: "neard", ?err, "Failed to update the logging config.");
        }
    }
}
