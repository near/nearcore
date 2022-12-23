use crate::config::Config;
use near_chain_configs::UpdateableClientConfig;
use near_dyn_configs::UpdateableConfigs;
use near_o11y::log_config::LogConfig;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DynConfigsError {
    #[error("Failed to parse a dynamic config file {file:?}: {err:?}")]
    Parse { file: PathBuf, err: serde_json::Error },
    #[error("Can't open or read a dynamic config file {file:?}: {err:?}")]
    OpenAndRead { file: PathBuf, err: std::io::Error },
    #[error("Can't open or read the config file {file:?}: {err:?}")]
    ConfigFileError { file: PathBuf, err: anyhow::Error },
    #[error("One or multiple dynamic config files reload errors")]
    Errors(Vec<DynConfigsError>),
    #[error("No home dir set")]
    NoHomeDir(),
}

pub fn read_updateable_configs(home_dir: &Path) -> Result<UpdateableConfigs, DynConfigsError> {
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
                errs.push(DynConfigsError::ConfigFileError {
                    file: PathBuf::from(crate::config::CONFIG_FILENAME),
                    err,
                });
                None
            }
        };
    if errs.is_empty() {
        Ok(UpdateableConfigs { log_config, client_config: updateable_client_config })
    } else {
        Err(DynConfigsError::Errors(errs))
    }
}

pub fn get_updateable_client_config(config: Config) -> UpdateableClientConfig {
    UpdateableClientConfig {
        expected_shutdown: config.expected_shutdown,
        max_block_wait_delay: config.consensus.max_block_wait_delay,
        max_block_production_delay: config.consensus.max_block_production_delay,
        min_block_production_delay: config.consensus.min_block_production_delay,
        block_production_tracking_delay: config.consensus.block_production_tracking_delay,
    }
}

fn read_log_config(home_dir: &Path) -> Result<Option<LogConfig>, DynConfigsError> {
    read_json_config::<LogConfig>(&home_dir.join("log_config.json"))
}

fn read_json_config<T: std::fmt::Debug>(path: &Path) -> Result<Option<T>, DynConfigsError>
where
    for<'a> T: Deserialize<'a>,
{
    match std::fs::read_to_string(path) {
        Ok(config_str) => match serde_json::from_str::<T>(&config_str) {
            Ok(config) => {
                tracing::info!(target: "neard", config=?config, "Changing the config {path:?}.");
                return Ok(Some(config));
            }
            Err(err) => Err(DynConfigsError::Parse { file: path.to_path_buf(), err }),
        },
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => {
                tracing::info!(target: "neard", ?err, "Reset the config {path:?} because the logging config file doesn't exist.");
                return Ok(None);
            }
            _ => Err(DynConfigsError::OpenAndRead { file: path.to_path_buf(), err }),
        },
    }
}
