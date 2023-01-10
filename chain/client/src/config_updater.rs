use near_dyn_configs::{DynConfigsError, UpdateableConfigs};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;

#[derive(Debug)]
pub enum ClientConfigUpdateError {}

/// Manages updating the config encapsulating.
pub struct ConfigUpdater {
    /// Receives config updates while the node is running.
    rx_config_update: Receiver<Result<UpdateableConfigs, Arc<DynConfigsError>>>,

    /// Represents the latest Error of reading the dynamically reloadable configs.
    updateable_configs_error: Option<Arc<DynConfigsError>>,
}

impl ConfigUpdater {
    pub fn new(
        rx_config_update: Receiver<Result<UpdateableConfigs, Arc<DynConfigsError>>>,
    ) -> Self {
        Self { rx_config_update, updateable_configs_error: None }
    }

    pub fn update_if_needed(
        &mut self,
        client: &mut crate::Client,
    ) -> Result<(), ClientConfigUpdateError> {
        // Check if any of the configs were updated. If they did, the receiver
        // will contain a clone of the new configs.
        while let Ok(maybe_updateable_configs) = self.rx_config_update.try_recv() {
            match maybe_updateable_configs {
                Ok(updateable_configs) => {
                    if let Some(client_config) = updateable_configs.client_config {
                        client.update_client_config(client_config);
                        tracing::info!(target: "config", "Updated ClientConfig");
                    }
                    self.updateable_configs_error = None;
                }
                Err(err) => {
                    self.updateable_configs_error = Some(err.clone());
                }
            }
        }
        Ok(())
    }

    pub fn log_error(&self) {
        if let Some(updateable_configs_error) = &self.updateable_configs_error {
            tracing::warn!(target: "stats", "Dynamically updateable configs are not valid. Please fix this ASAP otherwise the node will be unable to restart: {}", *updateable_configs_error);
        }
    }
}
