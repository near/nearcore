use near_chain::types::RuntimeAdapter;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnvBuilder;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_store::genesis::initialize_genesis_state;
use nearcore::NightshadeRuntime;
use std::{path::Path, sync::Arc};

pub trait TestEnvNightshadeSetupExt {
    fn nightshade_runtimes(self, genesis: &Genesis) -> Self;
    fn nightshade_runtimes_with_runtime_config_store(
        self,
        genesis: &Genesis,
        runtime_configs: Vec<RuntimeConfigStore>,
    ) -> Self;
}

impl TestEnvNightshadeSetupExt for TestEnvBuilder {
    fn nightshade_runtimes(self, genesis: &Genesis) -> Self {
        let (builder, stores, epoch_managers) =
            self.internal_ensure_epoch_managers_for_nightshade_runtime();
        let runtimes = stores
            .into_iter()
            .zip(epoch_managers)
            .map(|(store, epoch_manager)| {
                // TODO: It's not ideal to initialize genesis state with the nightshade runtime here for tests
                // Tests that don't use nightshade runtime have genesis initialized in kv_runtime.
                // We should instead try to do this while configuring store.
                let home_dir = Path::new("../../../..");
                initialize_genesis_state(store.clone(), genesis, Some(home_dir));
                NightshadeRuntime::test(home_dir, store, &genesis.config, epoch_manager)
                    as Arc<dyn RuntimeAdapter>
            })
            .collect();
        builder.runtimes(runtimes)
    }

    fn nightshade_runtimes_with_runtime_config_store(
        self,
        genesis: &Genesis,
        runtime_configs: Vec<RuntimeConfigStore>,
    ) -> Self {
        let (builder, stores, epoch_managers) =
            self.internal_ensure_epoch_managers_for_nightshade_runtime();
        assert_eq!(runtime_configs.len(), epoch_managers.len());
        let runtimes = stores
            .into_iter()
            .zip(epoch_managers)
            .zip(runtime_configs)
            .map(|((store, epoch_manager), runtime_config)| {
                // TODO: It's not ideal to initialize genesis state with the nightshade runtime here for tests
                // Tests that don't use nightshade runtime have genesis initialized in kv_runtime.
                // We should instead try to do this while configuring store.
                let home_dir = Path::new("../../../..");
                initialize_genesis_state(store.clone(), genesis, Some(home_dir));
                NightshadeRuntime::test_with_runtime_config_store(
                    home_dir,
                    store,
                    &genesis.config,
                    epoch_manager,
                    runtime_config,
                ) as Arc<dyn RuntimeAdapter>
            })
            .collect();
        builder.runtimes(runtimes)
    }
}
