use std::collections::BTreeMap;
use near_primitives::types::EpochHeight;
use std::sync::Arc;
use crate::ChainConfig;
use crate::config::ChainConfigLoader;
use crate::genesis_config::GenesisConfigLoader;

pub const RESOURCES_DIR: &str = "../res/";
pub const CONFIG_CHANGE_FILENAME: &str = "genesis.json";

/// Stores chain config for each epoch where it was updated.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChainConfigStore {
    /// Mirko: dodaj tu komentar
    pub initial_chain_config: Arc<ChainConfig>,
    /// Maps epoch to the config.
    store: BTreeMap<EpochHeight, Arc<ChainConfigLoader>>,
}

impl ChainConfigStore {
    pub fn new(genesis_config_loader: GenesisConfigLoader) -> Self {
        let mirko = String::from(RESOURCES_DIR) + CONFIG_CHANGE_FILENAME;
        println!("Mirko: {}", mirko);
        let initial_chain_config = Arc::new(ChainConfig::new(genesis_config_loader));
        let mut store = BTreeMap::new();
        // Mirko: tu dodaj čitanja iz fileova
        Self { initial_chain_config, store }
    }

    pub fn from_chain_config(chain_config: ChainConfig) -> Self {
        let initial_chain_config = Arc::new(chain_config);
        let mut store = BTreeMap::new();
        // Mirko: tu dodaj čitanja iz fileova
        Self { initial_chain_config, store }
    }

    pub fn test(chain_config: ChainConfig) -> Self {
        let initial_chain_config = Arc::new(chain_config);
        let mut store = BTreeMap::new();
        Self { initial_chain_config, store }
    }

    pub fn get_config(&self, epoch_height: EpochHeight) -> &Arc<ChainConfig> {
        // Mirko: fixaj da se ne vraca samo initial config nego da se zapravo applya store
        &self.initial_chain_config
    }
}
