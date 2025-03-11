use std::path::{Path, PathBuf};
use std::sync::Arc;

use near_chain_configs::{DEFAULT_GC_NUM_EPOCHS_TO_KEEP, GenesisConfig};
use near_epoch_manager::EpochManagerHandle;
use near_parameters::RuntimeConfigStore;
use near_store::config::StateSnapshotType;
use near_store::{StateSnapshotConfig, Store, TrieConfig};
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};

use super::NightshadeRuntime;

impl NightshadeRuntime {
    pub fn test_with_runtime_config_store(
        home_dir: &Path,
        store: Store,
        compiled_contract_cache: Box<dyn ContractRuntimeCache>,
        genesis_config: &GenesisConfig,
        epoch_manager: Arc<EpochManagerHandle>,
        runtime_config_store: RuntimeConfigStore,
        state_snapshot_type: StateSnapshotType,
    ) -> Arc<Self> {
        Self::new(
            store,
            compiled_contract_cache,
            genesis_config,
            epoch_manager,
            None,
            None,
            Some(runtime_config_store),
            DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
            Default::default(),
            StateSnapshotConfig {
                state_snapshot_type,
                home_dir: home_dir.to_path_buf(),
                hot_store_path: PathBuf::from("data"),
                state_snapshot_subdir: PathBuf::from("state_snapshot"),
            },
        )
    }

    pub fn test_with_trie_config(
        home_dir: &Path,
        store: Store,
        compiled_contract_cache: Box<dyn ContractRuntimeCache>,
        genesis_config: &GenesisConfig,
        epoch_manager: Arc<EpochManagerHandle>,
        runtime_config_store: Option<RuntimeConfigStore>,
        trie_config: TrieConfig,
        state_snapshot_type: StateSnapshotType,
        gc_num_epochs_to_keep: u64,
    ) -> Arc<Self> {
        Self::new(
            store,
            compiled_contract_cache,
            genesis_config,
            epoch_manager,
            None,
            None,
            runtime_config_store,
            gc_num_epochs_to_keep,
            trie_config,
            StateSnapshotConfig {
                state_snapshot_type,
                home_dir: home_dir.to_path_buf(),
                hot_store_path: PathBuf::from("data"),
                state_snapshot_subdir: PathBuf::from("state_snapshot"),
            },
        )
    }

    pub fn test(
        home_dir: &Path,
        store: Store,
        genesis_config: &GenesisConfig,
        epoch_manager: Arc<EpochManagerHandle>,
    ) -> Arc<Self> {
        Self::test_with_runtime_config_store(
            home_dir,
            store,
            FilesystemContractRuntimeCache::with_memory_cache(home_dir, None::<&str>, 1)
                .expect("filesystem contract cache")
                .handle(),
            genesis_config,
            epoch_manager,
            RuntimeConfigStore::test(),
            StateSnapshotType::ForReshardingOnly,
        )
    }
}
