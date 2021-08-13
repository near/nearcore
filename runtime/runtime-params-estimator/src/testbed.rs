use genesis_populate::state_dump::StateDump;
use near_chain_configs::Genesis;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::types::{Gas, MerkleHash};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::{ShardTries, StoreCompiledContractCache};
use near_vm_logic::VMLimitConfig;
use nearcore::get_store_path;
use node_runtime::{ApplyState, Runtime};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct RuntimeTestbed {
    /// Directory where we temporarily keep the storage.
    #[allow(dead_code)]
    workdir: tempfile::TempDir,
    pub tries: ShardTries,
    pub root: MerkleHash,
    pub runtime: Runtime,
    pub genesis: Genesis,
    prev_receipts: Vec<Receipt>,
    apply_state: ApplyState,
    epoch_info_provider: MockEpochInfoProvider,
}

impl RuntimeTestbed {
    /// Copies dump from another directory and loads the state from it.
    pub fn from_state_dump(dump_dir: &Path) -> Self {
        let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
        println!("workdir {}", workdir.path().display());
        let store_path = get_store_path(workdir.path());
        let StateDump { store, roots } = StateDump::from_dir(dump_dir, &store_path);
        let tries = ShardTries::new(store.clone(), 1);

        let genesis = Genesis::from_file(dump_dir.join("genesis.json"));
        assert!(roots.len() <= 1, "Parameter estimation works with one shard only.");
        assert!(!roots.is_empty(), "No state roots found.");
        let root = roots[0];

        let mut runtime_config = RuntimeConfig::default();

        runtime_config.wasm_config.limit_config = VMLimitConfig {
            max_total_log_length: u64::max_value(),
            max_number_registers: u64::max_value(),
            max_gas_burnt: u64::max_value(),
            max_register_size: u64::max_value(),
            max_number_logs: u64::max_value(),

            max_actions_per_receipt: u64::max_value(),
            max_promises_per_function_call_action: u64::max_value(),
            max_number_input_data_dependencies: u64::max_value(),

            max_total_prepaid_gas: u64::max_value(),
            max_number_bytes_method_names: u64::max_value(),

            ..Default::default()
        };

        let runtime = Runtime::new();
        let prev_receipts = vec![];

        let apply_state = ApplyState {
            // Put each runtime into a separate shard.
            block_index: 1,
            // Epoch length is long enough to avoid corner cases.
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            epoch_id: Default::default(),
            epoch_height: 0,
            gas_price: 0,
            block_timestamp: 0,
            gas_limit: None,
            random_seed: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(runtime_config),
            cache: Some(Arc::new(StoreCompiledContractCache { store: tries.get_store() })),
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        };

        Self {
            workdir,
            tries,
            root,
            runtime,
            prev_receipts,
            apply_state,
            epoch_info_provider: MockEpochInfoProvider::default(),
            genesis,
        }
    }

    pub fn process_block(
        &mut self,
        transactions: &[SignedTransaction],
        allow_failures: bool,
    ) -> Gas {
        let apply_result = self
            .runtime
            .apply(
                self.tries.get_trie_for_shard(0),
                self.root,
                &None,
                &self.apply_state,
                &self.prev_receipts,
                transactions,
                &self.epoch_info_provider,
                None,
            )
            .unwrap();

        let (store_update, root) = self.tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        self.root = root;
        store_update.commit().unwrap();
        self.apply_state.block_index += 1;

        let mut total_burnt_gas = 0;
        if !allow_failures {
            for outcome in &apply_result.outcomes {
                total_burnt_gas += outcome.outcome.gas_burnt;
                match &outcome.outcome.status {
                    ExecutionStatus::Failure(e) => panic!("Execution failed {:#?}", e),
                    _ => (),
                }
            }
        }
        self.prev_receipts = apply_result.outgoing_receipts;
        total_burnt_gas
    }

    pub fn process_blocks_until_no_receipts(&mut self, allow_failures: bool) {
        while !self.prev_receipts.is_empty() {
            self.process_block(&[], allow_failures);
        }
    }

    pub fn dump_state(&mut self) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let mut genesis_path = self.workdir.path().to_path_buf();
        genesis_path.push("genesis.json");
        self.genesis.to_file(genesis_path.as_path());

        let state_dump = StateDump { store: self.tries.get_store(), roots: vec![self.root] };
        state_dump.save_to_dir(self.workdir.path().to_path_buf())?;
        Ok(self.workdir.path().to_path_buf())
    }
}
