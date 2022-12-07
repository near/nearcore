#![doc = include_str!("../README.md")]

use near_chain_configs::Consensus;
use near_o11y::log_config::LogConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Sender;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DynConfig {
    /// Graceful shutdown at expected blockheight.
    pub expected_shutdown: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
/// Contains the latest state of configs which can be updated at runtime.
pub struct UpdateableConfigs {
    /// Contents of the file `dyn_config.json`.
    pub dyn_config: Option<DynConfig>,
    /// Contents of the file `log_config.json`.
    pub log_config: Option<LogConfig>,
    /// Contents of the `consensus` section of the file `config.json`.
    pub consensus: Option<Consensus>,
}

#[derive(Default)]
pub struct DynConfigStore {
    dyn_configs: UpdateableConfigs,
    original_consensus: Consensus,
    tx: Option<Sender<UpdateableConfigs>>,
}

impl DynConfigStore {
    pub fn reload(&mut self, mut dyn_configs: UpdateableConfigs) {
        if dyn_configs.consensus.is_none() {
            dyn_configs.consensus = Some(self.original_consensus.clone());
        }
        self.tx.as_ref().map(|tx| tx.send(dyn_configs.clone()));
        self.dyn_configs = dyn_configs;
    }

    pub fn new(
        dyn_configs: UpdateableConfigs,
        original_consensus: Consensus,
        tx: Sender<UpdateableConfigs>,
    ) -> Self {
        Self { dyn_configs, original_consensus, tx: Some(tx) }
    }

    pub fn dyn_config(&self) -> Option<&DynConfig> {
        self.dyn_configs.dyn_config.as_ref()
    }

    pub fn log_config(&self) -> Option<&LogConfig> {
        self.dyn_configs.log_config.as_ref()
    }

    pub fn consensus(&self) -> &Consensus {
        if let Some(consensus) = self.dyn_configs.consensus.as_ref() {
            consensus
        } else {
            &self.original_consensus
        }
    }
}
