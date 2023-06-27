use std::collections::BTreeMap;
use near_primitives::types::EpochHeight;
use std::sync::Arc;
use crate::ChainConfig;
use crate::config::ChainConfigPatch;
use crate::genesis_config::GenesisConfigSnapshot;
use std::fs::File;
use std::io::Read;
use serde_json;
use std::env;

pub const RESOURCES_DIR: &str = "core/chain-configs/res/";
pub const CONFIG_PATCH_LIST_FILENAME: &str = "config_patch_list.json";

/// Stores epochs when new config patches should be applied.
/// This list is used to load files with config patches.
#[derive(Debug, serde::Deserialize)]
pub struct ConfigPatchList {
    epoch_patches: Vec<EpochHeight>,
}

/// Stores chain config for each epoch where it was updated.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChainConfigStore {
    /// Initial chain config when bootstrapping network at epoch 0.
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
        let config_patch_list = Self::get_config_patch_list();

        for epoch_height in config_patch_list.epoch_patches {
            let chain_config_patch = Self::load_chain_config(epoch_height);
            store.insert(epoch_height, Arc::new(chain_config_patch));
        }
    }

    fn load_chain_config(epoch_height: EpochHeight) -> ChainConfigPatch {
        let current_dir = env::current_dir().expect("Failed to get the current directory");
        let path = current_dir.join(RESOURCES_DIR).join(epoch_height.to_string() + ".json");
        let mut file = File::open(&path).expect("Failed to open chain config patch file.");
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .expect("Failed to read the chain config patch file to string. ");
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_str)
                .expect("Failed to strip comments from chain config patch file.");
        serde_json::from_str(&json_str_without_comments)
            .expect("Failed to deserialize the chain config patch file.")
    }

    fn get_config_patch_list() -> ConfigPatchList {
        let current_dir = env::current_dir().expect("Failed to get the current directory");
        let path = current_dir.join(RESOURCES_DIR).join(CONFIG_PATCH_LIST_FILENAME);
        let mut file = File::open(&path).expect("Failed to open config patch list file.");
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .expect("Failed to read the config patch list file to string. ");
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_str)
                .expect("Failed to strip comments from config patch list file.");
        serde_json::from_str(&json_str_without_comments)
            .expect("Failed to deserialize the config patch list file.")
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
        for (_, config_patch) in self.store.range(..=epoch_height) {
            config = config.apply_patch(config_patch.as_ref());
        }
        Arc::new(config)
    }
}
