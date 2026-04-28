use super::NightshadeRuntime;
use near_chain_configs::{
    DEFAULT_GC_NUM_EPOCHS_TO_KEEP, DEFAULT_STATE_PARTS_COMPRESSION_LEVEL, GenesisConfig,
};
use near_epoch_manager::EpochManagerHandle;
use near_parameters::RuntimeConfigStore;
use near_store::{StateSnapshotConfig, Store, TrieConfig};
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};
use std::path::Path;
use std::sync::Arc;

impl NightshadeRuntime {
    pub fn test_with_runtime_config_store(
        home_dir: &Path,
        store: Store,
        compiled_contract_cache: Box<dyn ContractRuntimeCache>,
        genesis_config: &GenesisConfig,
        epoch_manager: Arc<EpochManagerHandle>,
        runtime_config_store: RuntimeConfigStore,
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
            StateSnapshotConfig::enabled(home_dir.join("data")),
            DEFAULT_STATE_PARTS_COMPRESSION_LEVEL,
            false,
            true,
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
        gc_num_epochs_to_keep: u64,
        is_cloud_archival_writer: bool,
        snapshot_every_n_epochs: u64,
        save_receipt_to_tx: bool,
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
            StateSnapshotConfig::enabled_with_cadence(
                home_dir.join("data"),
                snapshot_every_n_epochs,
            ),
            DEFAULT_STATE_PARTS_COMPRESSION_LEVEL,
            is_cloud_archival_writer,
            save_receipt_to_tx,
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
            FilesystemContractRuntimeCache::with_memory_cache(
                home_dir,
                None::<&str>,
                "contract.cache",
                1,
                None,
            )
            .expect("filesystem contract cache")
            .handle(),
            genesis_config,
            epoch_manager,
            RuntimeConfigStore::test(),
        )
    }
}
