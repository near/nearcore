use std::collections::BTreeMap;
use std::sync::Arc;
use crate::ChainConfig;
use crate::config::ChainConfigPatch;
use crate::genesis_config::GenesisConfig;
use std::fs::File;
use std::io::Read;
use serde_json;
use std::env;
use std::ops::Bound;
use near_primitives::types::ProtocolVersion;

pub const RESOURCES_DIR: &str = "core/chain-configs/res/";
pub const CONFIG_PATCH_LIST_FILENAME: &str = "config_patch_list.json";

/// Stores protocol versions when new config patches should be applied.
/// This list is used to load files with config patches.
#[derive(Debug, serde::Deserialize)]
pub struct ConfigPatchList {
    protocol_version_patches: Vec<ProtocolVersion>,
}

/// Stores chain config for each protocol version where it was updated.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChainConfigStore {
    /// Initial chain config at genesis block.
    pub initial_chain_config: Arc<ChainConfig>,
    /// Maps protocol version to the config.
    store: BTreeMap<ProtocolVersion, Arc<ChainConfig>>,
}

impl ChainConfigStore {
    /// Constructs a new store. Initial chain config is read from provided genesis_config.
    /// Stored values are read from config path list file and specific patches are read from the
    /// files that are named after the protocol version when they should be applied.
    pub fn new(genesis_config: GenesisConfig) -> Self {
        let initial_chain_config = Arc::new(ChainConfig::new(genesis_config));
        let mut store = BTreeMap::new();
        Self::populate_config_store(*initial_chain_config, &mut store);
        println!("Mirko: ide init config");
        println!("{:?}", initial_chain_config);
        println!("Mirko: ide store");
        println!("{:?}", store);
        println!("Mirko: ide chain config");
        println!("{:?}", store
            .range((Bound::Unbounded, Bound::Included(0)))
            .next_back()
            .unwrap_or_else(|| {
                panic!("Not found ChainConfig for protocol version {}", protocol_version)
            })
            .1);
        println!("{:?}", store
            .range((Bound::Unbounded, Bound::Included(5)))
            .next_back()
            .unwrap_or_else(|| {
                panic!("Not found ChainConfig for protocol version {}", protocol_version)
            })
            .1);
        println!("{:?}", store
            .range((Bound::Unbounded, Bound::Included(10)))
            .next_back()
            .unwrap_or_else(|| {
                panic!("Not found ChainConfig for protocol version {}", protocol_version)
            })
            .1);
        Self { initial_chain_config, store }
    }

    fn populate_config_store(
        initial_chain_config: ChainConfig,
        store: &mut BTreeMap<ProtocolVersion, Arc<ChainConfig>>,
    ) {
        let config_patch_list = Self::get_config_patch_list();
        let mut curr_config = initial_chain_config.clone();

        for protocol_veriosn in config_patch_list.protocol_version_patches {
            let chain_config_patch = Self::load_chain_config_patch(protocol_veriosn);
            curr_config = curr_config.apply_patch(&chain_config_patch);

            store.insert(protocol_veriosn, Arc::new(curr_config.clone()));
        }
    }

    fn load_chain_config_patch(protocol_version: ProtocolVersion) -> ChainConfigPatch {
        let current_dir = env::current_dir().expect("Failed to get the current directory");
        let path = current_dir.join(RESOURCES_DIR).join(protocol_version.to_string() + ".json");
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
        Self::populate_config_store(*initial_chain_config, &mut store);
        Self { initial_chain_config, store }
    }

    /// Instantiates new chain config with provided chain config without any overrides.
    pub fn test(chain_config: ChainConfig) -> Self {
        let initial_chain_config = Arc::new(chain_config);
        let store = BTreeMap::new();
        Self { initial_chain_config, store }
    }

    /// Returns initial config with applied all relevant config overrides for provided protocol version.
    pub fn get_config(&self, protocol_version: ProtocolVersion) -> &Arc<ChainConfig> {
        self.store
            .range((Bound::Unbounded, Bound::Included(protocol_version)))
            .next_back()
            .unwrap_or_else(|| {
                panic!("Not found ChainConfig for protocol version {}", protocol_version)
            })
            .1
    }
}
