#![doc = include_str!("../README.md")]

use near_chain_configs::UpdatableClientConfig;
use near_o11y::log_config::LogConfig;
use near_primitives::validator_signer::ValidatorSigner;
use near_time::Clock;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;

mod metrics;

#[derive(Clone, Default)]
pub enum UpdatableValidatorSigner {
    /// Validator key existence could not be determined.
    #[default]
    KeyExistenceNotDetermined,
    /// The new state of the validator key.
    MaybeKey(Option<Arc<ValidatorSigner>>),
}

#[derive(Clone, Default)]
/// Contains the latest state of configs which can be updated at runtime.
pub struct UpdatableConfigs {
    /// Contents of the file LOG_CONFIG_FILENAME.
    pub log_config: Option<LogConfig>,
    /// Contents of the `config.json` corresponding to the mutable fields of `ClientConfig`.
    pub client_config: Option<UpdatableClientConfig>,
    /// Validator key hot loaded from file.
    pub validator_signer: UpdatableValidatorSigner,
}

/// Pushes the updates to listeners.
#[derive(Default)]
pub struct UpdatableConfigLoader {
    /// Notifies receivers about the new config values available.
    tx: Option<Sender<Result<UpdatableConfigs, Arc<UpdatableConfigLoaderError>>>>,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum UpdatableConfigLoaderError {
    #[error("Failed to parse a dynamic config file {file:?}: {err:?}")]
    Parse { file: PathBuf, err: serde_json::Error },
    #[error("Can't open or read a dynamic config file {file:?}: {err:?}")]
    OpenAndRead { file: PathBuf, err: std::io::Error },
    #[error("Can't open or read the config file {file:?}: {err:?}")]
    ConfigFileError { file: PathBuf, err: anyhow::Error },
    #[error("Can't open or read the validator key file {file:?}: {err:?}")]
    ValidatorKeyFileError { file: PathBuf, err: anyhow::Error },
    #[error("One or multiple dynamic config files reload errors {0:?}")]
    Errors(Vec<UpdatableConfigLoaderError>),
    #[error("No home dir set")]
    NoHomeDir(),
}

impl UpdatableConfigLoader {
    pub fn new(
        updatable_configs: UpdatableConfigs,
        tx: Sender<Result<UpdatableConfigs, Arc<UpdatableConfigLoaderError>>>,
    ) -> Self {
        let mut result = Self { tx: Some(tx) };
        result.reload(Ok(updatable_configs));
        result
    }

    pub fn reload(
        &mut self,
        updatable_configs: Result<UpdatableConfigs, UpdatableConfigLoaderError>,
    ) {
        match updatable_configs {
            Ok(updatable_configs) => {
                near_o11y::reload_log_config(updatable_configs.log_config.as_ref());
                self.tx.as_ref().map(|tx| tx.send(Ok(updatable_configs.clone())));
                Self::update_metrics();
            }
            Err(err) => {
                self.tx.as_ref().map(|tx| tx.send(Err(Arc::new(err))));
            }
        }
    }

    fn update_metrics() {
        metrics::CONFIG_RELOAD_TIMESTAMP.set(Clock::real().now_utc().unix_timestamp());
        metrics::CONFIG_RELOADS.inc();
    }
}
