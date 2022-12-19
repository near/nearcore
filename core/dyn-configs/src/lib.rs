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
pub struct DynConfigs {
    pub dyn_config: Option<DynConfig>,
    pub log_config: Option<LogConfig>,
    pub consensus: Option<Consensus>,
}

#[derive(Default)]
pub struct DynConfigStore {
    dyn_configs: DynConfigs,
    original_consensus: Consensus,
    tx: Option<Sender<DynConfigs>>,
}

impl DynConfigStore {
    pub fn reload(&mut self, mut dyn_configs: DynConfigs) {
        if dyn_configs.consensus.is_none() {
            dyn_configs.consensus = Some(self.original_consensus.clone());
        }
        self.tx.as_ref().map(|tx| tx.send(dyn_configs.clone()));
        self.dyn_configs = dyn_configs;
    }

    pub fn new(
        dyn_configs: DynConfigs,
        original_consensus: Consensus,
        tx: Sender<DynConfigs>,
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
