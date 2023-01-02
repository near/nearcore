#![doc = include_str!("../README.md")]

use near_chain_configs::{ClientConfig, UpdateableClientConfig};
use near_o11y::log_config::LogConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Sender;

#[derive(Serialize, Deserialize, Clone, Default)]
/// Contains the latest state of configs which can be updated at runtime.
pub struct UpdateableConfigs {
    /// Contents of the file `log_config.json`.
    pub log_config: Option<LogConfig>,
    /// Contents of the `config.json` corresponding to the mutable fields of `ClientConfig`.
    pub client_config: Option<UpdateableClientConfig>,
}

#[derive(Default)]
pub struct DynConfigStore {
    /// The current version of the updateable configs.
    updateable_configs: UpdateableConfigs,
    /// The default values of the configs, which is the values obtained at the node startup.
    original_updateable_client_config: UpdateableClientConfig,
    /// Notifies receivers about the new config values available.
    tx: Option<Sender<UpdateableConfigs>>,
}

impl DynConfigStore {
    pub fn reload(&mut self, mut updateable_configs: UpdateableConfigs) {
        if updateable_configs.client_config.is_none() {
            updateable_configs.client_config = Some(self.original_updateable_client_config.clone());
        }
        self.tx.as_ref().map(|tx| tx.send(updateable_configs.clone()));
        self.updateable_configs = updateable_configs;
    }

    pub fn new(
        updateable_configs: UpdateableConfigs,
        original_client_config: ClientConfig,
        tx: Sender<UpdateableConfigs>,
    ) -> Self {
        Self {
            updateable_configs,
            original_updateable_client_config: UpdateableClientConfig {
                expected_shutdown: original_client_config.expected_shutdown.get(),
            },
            tx: Some(tx),
        }
    }

    pub fn log_config(&self) -> Option<&LogConfig> {
        self.updateable_configs.log_config.as_ref()
    }

    pub fn updateable_client_config(&self) -> &UpdateableClientConfig {
        if let Some(client_config) = self.updateable_configs.client_config.as_ref() {
            client_config
        } else {
            &self.original_updateable_client_config
        }
    }
}
