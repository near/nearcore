use genesis_populate::state_dump::StateDump;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::types::{Gas, MerkleHash};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::{ShardTries, ShardUId, Store, StoreCompiledContractCache};
use near_vm_logic::VMLimitConfig;
use node_runtime::{ApplyState, Runtime};
use std::path::Path;
use std::sync::Arc;

pub struct RuntimeTestbed {
    /// Directory where we temporarily keep the storage.
    _workdir: tempfile::TempDir,
    tries: ShardTries,
    root: MerkleHash,
    runtime: Runtime,
    prev_receipts: Vec<Receipt>,
    apply_state: ApplyState,
    epoch_info_provider: MockEpochInfoProvider,
}

impl RuntimeTestbed {
    /// Copies dump from another directory and loads the state from it.
    pub fn from_state_dump(dump_dir: &Path, in_memory_db: bool) -> Self {
        let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
        let StateDump { store, roots } =
            StateDump::from_dir(dump_dir, workdir.path(), in_memory_db);
        // Ensure decent RocksDB SST file layout.
        store.compact().expect("compaction failed");
        let tries = ShardTries::test(store, 1);

        assert!(roots.len() <= 1, "Parameter estimation works with one shard only.");
        assert!(!roots.is_empty(), "No state roots found.");
        let root = roots[0];

        let mut runtime_config =
            RuntimeConfigStore::new(None).get_config(PROTOCOL_VERSION).as_ref().clone();

        // Override vm limits config to simplify block processing.
        runtime_config.wasm_config.limit_config = VMLimitConfig {
            max_total_log_length: u64::MAX,
            max_number_registers: u64::MAX,
            max_gas_burnt: u64::MAX,
            max_register_size: u64::MAX,
            max_number_logs: u64::MAX,

            max_actions_per_receipt: u64::MAX,
            max_promises_per_function_call_action: u64::MAX,
            max_number_input_data_dependencies: u64::MAX,

            max_total_prepaid_gas: u64::MAX,

            ..VMLimitConfig::test()
        };
        runtime_config.account_creation_config.min_allowed_top_level_account_length = 0;

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
            cache: Some(Box::new(StoreCompiledContractCache::new(&tries.get_store()))),
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        };

        Self {
            _workdir: workdir,
            tries,
            root,
            runtime,
            prev_receipts,
            apply_state,
            epoch_info_provider: MockEpochInfoProvider::default(),
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
                self.tries.get_trie_for_shard(ShardUId::single_shard(), self.root.clone()),
                &None,
                &self.apply_state,
                &self.prev_receipts,
                transactions,
                &self.epoch_info_provider,
                Default::default(),
            )
            .unwrap();

        let (store_update, root) =
            self.tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard());
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

    /// Returns the number of blocks required to reach quiescence
    pub fn process_blocks_until_no_receipts(&mut self, allow_failures: bool) -> usize {
        let mut n = 0;
        while !self.prev_receipts.is_empty() {
            self.process_block(&[], allow_failures);
            n += 1;
        }
        n
    }

    /// Flushes RocksDB memtable
    pub fn flush_db_write_buffer(&mut self) {
        self.tries.get_store().flush().unwrap();
    }

    pub fn store(&mut self) -> Store {
        self.tries.get_store()
    }
}
