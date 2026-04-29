use super::NightshadeRuntime;
use near_async::futures::AsyncComputationSpawner;
use near_async::thread_pool::contract_compilation_pool;
use near_chain_configs::{
    DEFAULT_GC_NUM_EPOCHS_TO_KEEP, DEFAULT_STATE_PARTS_COMPRESSION_LEVEL, GenesisConfig,
};
use near_epoch_manager::EpochManagerHandle;
use near_parameters::RuntimeConfigStore;
use near_store::{StateSnapshotConfig, Store, TrieConfig};
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};
use std::path::Path;
use std::sync::Arc;

/// Default compile spawner used by the test-only `NightshadeRuntime`
/// constructors when callers do not pass a custom spawner. Production code
/// (e.g. `nearcore::start_with_config`) constructs the runtime via
/// `NightshadeRuntime::new` directly and supplies the production
/// `contract_compilation_pool()`.
fn default_test_compile_spawner() -> Arc<dyn AsyncComputationSpawner> {
    contract_compilation_pool().clone()
}

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
            default_test_compile_spawner(),
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
        Self::test_with_trie_config_and_spawner(
            home_dir,
            store,
            compiled_contract_cache,
            genesis_config,
            epoch_manager,
            runtime_config_store,
            trie_config,
            gc_num_epochs_to_keep,
            is_cloud_archival_writer,
            snapshot_every_n_epochs,
            save_receipt_to_tx,
            default_test_compile_spawner(),
        )
    }

    /// Like `test_with_trie_config`, but lets the caller install a custom
    /// `AsyncComputationSpawner` for background contract compilation. Used
    /// by the test-loop framework to provide a deterministic spawner so
    /// that pending-compile-queue advancement is reproducible across runs.
    pub fn test_with_trie_config_and_spawner(
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
        compile_contracts_spawner: Arc<dyn AsyncComputationSpawner>,
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
            compile_contracts_spawner,
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
