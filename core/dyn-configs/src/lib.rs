#![doc = include_str!("../README.md")]

use near_chain_configs::UpdateableClientConfig;
use near_o11y::log_config::LogConfig;
use near_primitives::static_clock::StaticClock;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;

mod metrics;

#[derive(Serialize, Deserialize, Clone, Default)]
/// Contains the latest state of configs which can be updated at runtime.
pub struct UpdateableConfigs {
    /// Contents of the file LOG_CONFIG_FILENAME.
    pub log_config: Option<LogConfig>,
    /// Contents of the `config.json` corresponding to the mutable fields of `ClientConfig`.
    pub client_config: Option<UpdateableClientConfig>,
}

/// Pushes the updates to listeners.
#[derive(Default)]
pub struct UpdateableConfigLoader {
    /// Notifies receivers about the new config values available.
    tx: Option<Sender<Result<UpdateableConfigs, Arc<UpdateableConfigLoaderError>>>>,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum UpdateableConfigLoaderError {
    #[error("Failed to parse a dynamic config file {file:?}: {err:?}")]
    Parse { file: PathBuf, err: serde_json::Error },
    #[error("Can't open or read a dynamic config file {file:?}: {err:?}")]
    OpenAndRead { file: PathBuf, err: std::io::Error },
    #[error("Can't open or read the config file {file:?}: {err:?}")]
    ConfigFileError { file: PathBuf, err: anyhow::Error },
    #[error("One or multiple dynamic config files reload errors {0:?}")]
    Errors(Vec<UpdateableConfigLoaderError>),
    #[error("No home dir set")]
    NoHomeDir(),
}

impl UpdateableConfigLoader {
    pub fn new(
        updateable_configs: UpdateableConfigs,
        tx: Sender<Result<UpdateableConfigs, Arc<UpdateableConfigLoaderError>>>,
    ) -> Self {
        let mut result = Self { tx: Some(tx) };
        result.reload(Ok(updateable_configs));
        result
    }

    pub fn reload(
        &mut self,
        updateable_configs: Result<UpdateableConfigs, UpdateableConfigLoaderError>,
    ) {
        match updateable_configs {
            Ok(updateable_configs) => {
                near_o11y::reload_log_config(updateable_configs.log_config.as_ref());
                self.tx.as_ref().map(|tx| tx.send(Ok(updateable_configs.clone())));
                Self::update_metrics();
            }
            Err(err) => {
                self.tx.as_ref().map(|tx| tx.send(Err(Arc::new(err))));
            }
        }
    }

    fn update_metrics() {
        metrics::CONFIG_RELOAD_TIMESTAMP.set(StaticClock::utc().timestamp());
        metrics::CONFIG_RELOADS.inc();
    }
}
