#![doc = include_str!("../README.md")]

use near_chain_configs::ClientConfig;
use near_o11y::log_config::LogConfig;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DynConfig {
    /// Graceful shutdown at expected blockheight.
    pub expected_shutdown: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DynConfigs {
    pub dyn_config: Option<DynConfig>,
    pub log_config: Option<LogConfig>,
    pub client_config: Option<ClientConfig>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DynConfigStore {
    dyn_configs: DynConfigs,
}

impl DynConfigStore {
    pub fn reload(&mut self, dyn_configs: DynConfigs) {
        self.dyn_configs = dyn_configs;
    }

    pub fn new(dyn_configs: DynConfigs) -> Self {
        Self { dyn_configs }
    }

    pub fn dyn_config(&self) -> Option<&DynConfig> {
        self.dyn_configs.dyn_config.as_ref()
    }

    pub fn log_config(&self) -> Option<&LogConfig> {
        self.dyn_configs.log_config.as_ref()
    }

    pub fn client_config(&self) -> Option<&ClientConfig> {
        self.dyn_configs.client_config.as_ref()
    }
}
