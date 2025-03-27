use near_chain_configs::{DEFAULT_GC_NUM_EPOCHS_TO_KEEP, Genesis};
use near_epoch_manager::EpochManagerHandle;
use near_parameters::RuntimeConfigStore;
use near_store::genesis::initialize_genesis_state;
use near_store::{Store, TrieConfig};
use near_vm_runner::ContractRuntimeCache;
use nearcore::NightshadeRuntime;
use std::path::PathBuf;
use std::sync::Arc;

use super::test_env_builder::TestEnvBuilder;

pub trait TestEnvNightshadeSetupExt {
    fn nightshade_runtimes(self, genesis: &Genesis) -> Self;
    fn nightshade_runtimes_congestion_control_disabled(self, genesis: &Genesis) -> Self;
    fn nightshade_runtimes_with_runtime_config_store(
        self,
        genesis: &Genesis,
        runtime_configs: Vec<RuntimeConfigStore>,
    ) -> Self;
    fn nightshade_runtimes_with_trie_config(
        self,
        genesis: &Genesis,
        trie_configs: Vec<TrieConfig>,
    ) -> Self;
}

impl TestEnvNightshadeSetupExt for TestEnvBuilder {
    fn nightshade_runtimes(self, genesis: &Genesis) -> Self {
        let runtime_configs = vec![RuntimeConfigStore::test(); self.num_clients()];
        self.nightshade_runtimes_with_runtime_config_store(genesis, runtime_configs)
    }

    fn nightshade_runtimes_congestion_control_disabled(self, genesis: &Genesis) -> Self {
        let runtime_config_store = RuntimeConfigStore::test_congestion_control_disabled();
        let runtime_configs = vec![runtime_config_store; self.num_clients()];
        self.nightshade_runtimes_with_runtime_config_store(genesis, runtime_configs)
    }

    fn nightshade_runtimes_with_runtime_config_store(
        self,
        genesis: &Genesis,
        runtime_configs: Vec<RuntimeConfigStore>,
    ) -> Self {
        let nightshade_runtime_creator = |home_dir: PathBuf,
                                          store: Store,
                                          contract_cache: Box<dyn ContractRuntimeCache>,
                                          epoch_manager: Arc<EpochManagerHandle>,
                                          runtime_config: RuntimeConfigStore,
                                          _| {
            // TODO: It's not ideal to initialize genesis state with the nightshade runtime here for tests
            // Tests that don't use nightshade runtime have genesis initialized in kv_runtime.
            // We should instead try to do this while configuring store.
            let home_dir = home_dir.as_path();
            initialize_genesis_state(store.clone(), genesis, Some(home_dir));
            NightshadeRuntime::test_with_runtime_config_store(
                home_dir,
                store,
                contract_cache,
                &genesis.config,
                epoch_manager,
                runtime_config,
            )
        };
        let dummy_trie_configs = vec![TrieConfig::default(); self.num_clients()];
        self.internal_initialize_nightshade_runtimes(
            runtime_configs,
            dummy_trie_configs,
            nightshade_runtime_creator,
        )
    }

    fn nightshade_runtimes_with_trie_config(
        self,
        genesis: &Genesis,
        trie_configs: Vec<TrieConfig>,
    ) -> Self {
        let nightshade_runtime_creator =
            |home_dir: PathBuf,
             store: Store,
             contract_cache: Box<dyn ContractRuntimeCache>,
             epoch_manager: Arc<EpochManagerHandle>,
             runtime_config_store: RuntimeConfigStore,
             trie_config: TrieConfig| {
                // TODO: It's not ideal to initialize genesis state with the nightshade runtime here for tests
                // Tests that don't use nightshade runtime have genesis initialized in kv_runtime.
                // We should instead try to do this while configuring store.
                let home_dir = home_dir.as_path();
                initialize_genesis_state(store.clone(), genesis, Some(home_dir));
                NightshadeRuntime::test_with_trie_config(
                    home_dir,
                    store,
                    contract_cache,
                    &genesis.config,
                    epoch_manager,
                    Some(runtime_config_store),
                    trie_config,
                    DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
                )
            };
        let dummy_runtime_configs =
            vec![RuntimeConfigStore::test_congestion_control_disabled(); self.num_clients()];
        self.internal_initialize_nightshade_runtimes(
            dummy_runtime_configs,
            trie_configs,
            nightshade_runtime_creator,
        )
    }
}
