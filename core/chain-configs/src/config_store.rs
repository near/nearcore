use std::collections::BTreeMap;
use near_primitives::types::EpochHeight;
use std::sync::Arc;
use crate::ChainConfig;
use crate::config::ChainConfigPatch;
use crate::genesis_config::GenesisConfigSnapshot;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use serde_json;
use std::env;

pub const RESOURCES_DIR: &str = "core/chain-configs/res/";
pub const CONFIG_CHANGE_FILENAME: &str = "config_change_list.json";

/// Stores epochs when new config changes should be applied.
/// This list is used to load files with config changes.
#[derive(Debug, serde::Deserialize)]
pub struct ConfigChangeList {
    epoch_changes: Vec<EpochHeight>,
}

/// Stores chain config for each epoch where it was updated.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChainConfigStore {
    /// Mirko: dodaj tu komentar
    pub initial_chain_config: Arc<ChainConfig>,
    /// Maps epoch to the config.
    store: BTreeMap<EpochHeight, Arc<ChainConfigPatch>>,
}

impl ChainConfigStore {
    pub fn new(genesis_config_snapshot: GenesisConfigSnapshot) -> Self {
        let initial_chain_config = Arc::new(ChainConfig::new(genesis_config_snapshot));
        let mut store = BTreeMap::new();
        Self::populate_config_store(&mut store);
        Self { initial_chain_config, store }
    }

    fn populate_config_store(store: &mut BTreeMap<EpochHeight, Arc<ChainConfigPatch>>) {
        let config_change_list = Self::get_config_change_list();

        for epoch_height in config_change_list.epoch_changes {
            let chain_config_patch = Self::load_chain_config(epoch_height);
            store.insert(epoch_height, Arc::new(chain_config_patch));
        }
    }

    fn load_chain_config(epoch_height: EpochHeight) -> ChainConfigPatch {
        let current_dir = env::current_dir().expect("Failed to get the current directory");
        let path = current_dir.join(RESOURCES_DIR).join(epoch_height.to_string() + ".json");
        let mut file = File::open(&path).expect("Failed to open config change file.");
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .expect("Failed to read the config change file to string. ");
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_str)
                .expect("Failed to strip comments from config change file.");
        serde_json::from_str(&json_str_without_comments)
            .expect("Failed to deserialize the config change file.")
    }

    fn get_config_change_list() -> ConfigChangeList {
        let current_dir = env::current_dir().expect("Failed to get the current directory");
        let path = current_dir.join(RESOURCES_DIR).join(CONFIG_CHANGE_FILENAME);
        let mut file = File::open(&path).expect("Failed to open config change list file.");
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .expect("Failed to read the config change list file to string. ");
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_str)
                .expect("Failed to strip comments from config change list file.");
        serde_json::from_str(&json_str_without_comments)
            .expect("Failed to deserialize the config change list file.")
    }

    pub fn from_chain_config(chain_config: ChainConfig) -> Self {
        let initial_chain_config = Arc::new(chain_config);
        let mut store = BTreeMap::new();
        Self::populate_config_store(&mut store);
        Self { initial_chain_config, store }
    }

    /// Instantiates new chain config with provided chain config without any overrides.
    pub fn test(chain_config: ChainConfig) -> Self {
        let initial_chain_config = Arc::new(chain_config);
        let store = BTreeMap::new();
        Self { initial_chain_config, store }
    }

    /// Returns initial config with applied all relevat config overrides for provided epoch.
    pub fn get_config(&self, epoch_height: EpochHeight) -> Arc<ChainConfig> {
        let mut config = self.initial_chain_config.as_ref().clone();
        for (_, config_change_patch) in self.store.range(..=epoch_height) {
            config = config.apply_patch(config_change_patch.as_ref());
        }
        Arc::new(config)
    }
}
