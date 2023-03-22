use near_chain_configs::UpdateableClientConfig;
use near_dyn_configs::{UpdateableConfigLoaderError, UpdateableConfigs};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;

#[derive(Debug)]
pub enum ClientConfigUpdateError {}

/// Manages updating the config encapsulating.
pub struct ConfigUpdater {
    /// Receives config updates while the node is running.
    rx_config_update: Receiver<Result<UpdateableConfigs, Arc<UpdateableConfigLoaderError>>>,

    /// Represents the latest Error of reading the dynamically reloadable configs.
    updateable_configs_error: Option<Arc<UpdateableConfigLoaderError>>,
}

impl ConfigUpdater {
    pub fn new(
        rx_config_update: Receiver<Result<UpdateableConfigs, Arc<UpdateableConfigLoaderError>>>,
    ) -> Self {
        Self { rx_config_update, updateable_configs_error: None }
    }

    /// Check if any of the configs were updated.
    /// If they did, the receiver (rx_config_update) will contain a clone of the new configs.
    pub fn try_update(&mut self, update_client_config_fn: &dyn Fn(UpdateableClientConfig)) {
        while let Ok(maybe_updateable_configs) = self.rx_config_update.try_recv() {
            match maybe_updateable_configs {
                Ok(updateable_configs) => {
                    if let Some(client_config) = updateable_configs.client_config {
                        update_client_config_fn(client_config);
                        tracing::info!(target: "config", "Updated ClientConfig");
                    }
                    self.updateable_configs_error = None;
                }
                Err(err) => {
                    self.updateable_configs_error = Some(err.clone());
                }
            }
        }
    }

    /// Prints an error if it's present.
    pub fn report_status(&self) {
        if let Some(updateable_configs_error) = &self.updateable_configs_error {
            tracing::warn!(
                target: "stats",
                "Dynamically updateable configs are not valid. Please fix this ASAP otherwise the node will probably crash after restart: {}",
                *updateable_configs_error);
        }
    }
}
