use std::collections::BTreeMap;
use near_primitives::types::EpochHeight;
use std::sync::Arc;
use crate::ChainConfig;
use crate::config::ChainConfigLoader;
use crate::genesis_config::GenesisConfigLoader;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use serde_json;

pub const RESOURCES_DIR: &str = "../res/";
pub const CONFIG_CHANGE_FILENAME: &str = "config_change_list.json";

#[derive(Debug, serde::Deserialize)]
pub struct ConfigChangeList {
    epoch_changes: Vec<i32>,
}

/// Stores chain config for each epoch where it was updated.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChainConfigStore {
    /// Mirko: dodaj tu komentar
    pub initial_chain_config: Arc<ChainConfig>,
    /// Maps epoch to the config.
    store: BTreeMap<EpochHeight, Arc<ChainConfigLoader>>,
}

impl ChainConfigStore {
    /*
    let mut file = File::open(path).expect("Failed to open genesis config file.");
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .expect("Failed to read the genesis config file to string. ");
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_str)
                .expect("Failed to strip comments from Genesis config file.");
        serde_json::from_str(&json_str_without_comments)
            .expect("Failed to deserialize the genesis records.")
     */
    pub fn new(genesis_config_loader: GenesisConfigLoader) -> Self {
        let initial_chain_config = Arc::new(ChainConfig::new(genesis_config_loader));
        let mut store = BTreeMap::new();
        // Mirko: tu dodaj čitanja iz fileova
        Self::populate_config_store(&store);
        Self { initial_chain_config, store }
    }

    fn populate_config_store(store: &BTreeMap<EpochHeight, Arc<ChainConfigLoader>>) {
        //let path = Path::new(String::from(RESOURCES_DIR) + CONFIG_CHANGE_FILENAME);
        let mut file = File::open(String::from(RESOURCES_DIR) + CONFIG_CHANGE_FILENAME).expect("Failed to open config change list file.");
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .expect("Failed to read the config change list file to string. ");
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_str)
                .expect("Failed to strip comments from config change list file.");
        let config_change_list: ConfigChangeList = serde_json::from_str(&json_str_without_comments)
            .expect("Failed to deserialize the config change list file.");

        println!("Mirko: config_change_list: {:?}", config_change_list);
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
