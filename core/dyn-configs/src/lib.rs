#![doc = include_str!("../README.md")]

use near_chain_configs::Consensus;
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
    pub consensus: Option<Consensus>,
}

#[derive(Default)]
pub struct DynConfigStore {
    dyn_configs: DynConfigs,
    original_consensus: Consensus,
    callbacks: Vec<Box<dyn Fn(Option<&Consensus>) + Send + Sync + 'static>>,
}

impl DynConfigStore {
    pub fn reload(&mut self, dyn_configs: DynConfigs) {
        self.dyn_configs = dyn_configs;
        for f in &self.callbacks {
            f(self.consensus());
        }
    }

    pub fn new(dyn_configs: DynConfigs, original_consensus: Consensus) -> Self {
        Self { dyn_configs, original_consensus, callbacks: vec![] }
    }

    pub fn dyn_config(&self) -> Option<&DynConfig> {
        self.dyn_configs.dyn_config.as_ref()
    }

    pub fn log_config(&self) -> Option<&LogConfig> {
        self.dyn_configs.log_config.as_ref()
    }

    pub fn consensus(&self) -> Option<&Consensus> {
        if let Some(consensus) = self.dyn_configs.consensus.as_ref() {
            Some(consensus)
        } else {
            Some(&self.original_consensus)
        }
    }

    pub fn register_update_callback(
        &mut self,
        f: Box<dyn Fn(Option<&Consensus>) + Send + Sync + 'static>,
    ) {
        self.callbacks.push(f);
    }
}
