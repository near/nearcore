use crate::config::Config;
use near_chain_configs::UpdateableClientConfig;
use near_dyn_configs::{UpdateableConfigLoaderError, UpdateableConfigs};
use near_o11y::log_config::LogConfig;
use serde::Deserialize;
use std::path::{Path, PathBuf};

const LOG_CONFIG_FILENAME: &str = "log_config.json";

/// This function gets called at the startup and each time a config needs to be reloaded.
pub fn read_updateable_configs(
    home_dir: &Path,
) -> Result<UpdateableConfigs, UpdateableConfigLoaderError> {
    let mut errs = vec![];
    let log_config = match read_log_config(home_dir) {
        Ok(config) => config,
        Err(err) => {
            errs.push(err);
            None
        }
    };
    let updateable_client_config =
        match Config::from_file(&home_dir.join(crate::config::CONFIG_FILENAME))
            .map(get_updateable_client_config)
        {
            Ok(config) => Some(config),
            Err(err) => {
                errs.push(UpdateableConfigLoaderError::ConfigFileError {
                    file: PathBuf::from(crate::config::CONFIG_FILENAME),
                    err,
                });
                None
            }
        };
    if errs.is_empty() {
        crate::metrics::CONFIG_CORRECT.set(1);
        Ok(UpdateableConfigs { log_config, client_config: updateable_client_config })
    } else {
        tracing::warn!(target: "neard", "Dynamically updateable configs are not valid. Please fix this ASAP otherwise the node will be unable to restart: {:?}", &errs);
        crate::metrics::CONFIG_CORRECT.set(0);
        Err(UpdateableConfigLoaderError::Errors(errs))
    }
}

pub fn get_updateable_client_config(config: Config) -> UpdateableClientConfig {
    // All fields that can be updated while the node is running should be explicitly set here.
    // Keep this list in-sync with `core/dyn-configs/README.md`.
    UpdateableClientConfig { expected_shutdown: config.expected_shutdown }
}

fn read_log_config(home_dir: &Path) -> Result<Option<LogConfig>, UpdateableConfigLoaderError> {
    read_json_config::<LogConfig>(&home_dir.join(LOG_CONFIG_FILENAME))
}

fn read_json_config<T: std::fmt::Debug>(
    path: &Path,
) -> Result<Option<T>, UpdateableConfigLoaderError>
where
    for<'a> T: Deserialize<'a>,
{
    match std::fs::read_to_string(path) {
        Ok(config_str) => match serde_json::from_str::<T>(&config_str) {
            Ok(config) => {
                tracing::info!(target: "neard", config=?config, "Changing the config {path:?}.");
                return Ok(Some(config));
            }
            Err(err) => Err(UpdateableConfigLoaderError::Parse { file: path.to_path_buf(), err }),
        },
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => {
                tracing::info!(target: "neard", ?err, "Reset the config {path:?} because the config file doesn't exist.");
                return Ok(None);
            }
            _ => Err(UpdateableConfigLoaderError::OpenAndRead { file: path.to_path_buf(), err }),
        },
    }
}
