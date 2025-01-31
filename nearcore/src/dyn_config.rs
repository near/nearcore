use crate::config::Config;
use near_chain_configs::UpdatableClientConfig;
use near_dyn_configs::{UpdatableConfigLoaderError, UpdatableConfigs, UpdatableValidatorSigner};
use near_o11y::log_config::LogConfig;
use near_primitives::validator_signer::ValidatorSigner;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const LOG_CONFIG_FILENAME: &str = "log_config.json";

/// This function gets called at the startup and each time a config needs to be reloaded.
pub fn read_updatable_configs(
    home_dir: &Path,
) -> Result<UpdatableConfigs, UpdatableConfigLoaderError> {
    let mut errs = vec![];
    let log_config = match read_log_config(home_dir) {
        Ok(config) => config,
        Err(err) => {
            errs.push(err);
            None
        }
    };
    let config = match Config::from_file(&home_dir.join(crate::config::CONFIG_FILENAME)) {
        Ok(config) => Some(config),
        Err(err) => {
            errs.push(UpdatableConfigLoaderError::ConfigFileError {
                file: PathBuf::from(crate::config::CONFIG_FILENAME),
                err: err.into(),
            });
            None
        }
    };
    let updatable_client_config = config.as_ref().map(get_updatable_client_config);

    let validator_signer = if let Some(config) = config {
        match read_validator_key(home_dir, &config) {
            Ok(validator_key) => UpdatableValidatorSigner::MaybeKey(validator_key),
            Err(err) => {
                errs.push(err);
                UpdatableValidatorSigner::KeyExistenceNotDetermined
            }
        }
    } else {
        UpdatableValidatorSigner::KeyExistenceNotDetermined
    };

    if errs.is_empty() {
        crate::metrics::CONFIG_CORRECT.set(1);
        Ok(UpdatableConfigs {
            log_config,
            client_config: updatable_client_config,
            validator_signer,
        })
    } else {
        tracing::warn!(target: "neard", "Dynamically updatable configs are not valid. Please fix this ASAP otherwise the node will be unable to restart: {:?}", &errs);
        crate::metrics::CONFIG_CORRECT.set(0);
        Err(UpdatableConfigLoaderError::Errors(errs))
    }
}

pub fn get_updatable_client_config(config: &Config) -> UpdatableClientConfig {
    // All fields that can be updated while the node is running should be explicitly set here.
    // Keep this list in-sync with `core/dyn-configs/README.md`.
    UpdatableClientConfig {
        expected_shutdown: config.expected_shutdown,
        resharding_config: config.resharding_config,
        produce_chunk_add_transactions_time_limit: config.produce_chunk_add_transactions_time_limit,
    }
}

fn read_log_config(home_dir: &Path) -> Result<Option<LogConfig>, UpdatableConfigLoaderError> {
    read_json_config::<LogConfig>(&home_dir.join(LOG_CONFIG_FILENAME))
}

// the file can be JSON with comments
fn read_json_config<T>(path: &Path) -> Result<Option<T>, UpdatableConfigLoaderError>
where
    for<'a> T: Deserialize<'a> + std::fmt::Debug,
{
    match std::fs::read_to_string(path) {
        Ok(config_str) => match near_config_utils::strip_comments_from_json_str(&config_str) {
            Ok(config_str_without_comments) => {
                match serde_json::from_str::<T>(&config_str_without_comments) {
                    Ok(config) => {
                        tracing::info!(target: "neard", config=?config, "Changing the config {path:?}.");
                        Ok(Some(config))
                    }
                    Err(err) => {
                        Err(UpdatableConfigLoaderError::Parse { file: path.to_path_buf(), err })
                    }
                }
            }
            Err(err) => {
                Err(UpdatableConfigLoaderError::OpenAndRead { file: path.to_path_buf(), err })
            }
        },
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => {
                tracing::info!(target: "neard", ?err, "Reset the config {path:?} because the config file doesn't exist.");
                Ok(None)
            }
            _ => Err(UpdatableConfigLoaderError::OpenAndRead { file: path.to_path_buf(), err }),
        },
    }
}

fn read_validator_key(
    home_dir: &Path,
    config: &Config,
) -> Result<Option<Arc<ValidatorSigner>>, UpdatableConfigLoaderError> {
    let validator_file: PathBuf = home_dir.join(&config.validator_key_file);
    match crate::config::load_validator_key(&validator_file) {
        Ok(Some(validator_signer)) => {
            tracing::info!(target: "neard", "Hot loading validator key {}.", validator_file.display());
            Ok(Some(validator_signer))
        }
        Ok(None) => {
            tracing::info!(target: "neard", "No validator key {}.", validator_file.display());
            Ok(None)
        }
        Err(err) => {
            Err(UpdatableConfigLoaderError::ValidatorKeyFileError { file: validator_file, err })
        }
    }
}
