use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use borsh::BorshDeserialize;

use near_primitives::receipt::Receipt;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::types::{Gas, MerkleHash, StateRoot};
use near_store::{create_store, ColState, Trie};
use near_vm_logic::VMLimitConfig;
use neard::get_store_path;
use node_runtime::config::RuntimeConfig;
use node_runtime::{ApplyState, Runtime};

const STATE_DUMP_FILE: &str = "state_dump";
const GENESIS_ROOTS_FILE: &str = "genesis_roots";

pub struct RuntimeTestbed {
    /// Directory where we temporarily keep the storage.
    #[allow(dead_code)]
    workdir: tempfile::TempDir,
    trie: Arc<Trie>,
    root: MerkleHash,
    runtime: Runtime,
    prev_receipts: Vec<Receipt>,
    apply_state: ApplyState,
}

impl RuntimeTestbed {
    /// Copies dump from another directory and loads the state from it.
    pub fn from_state_dump(dump_dir: &Path) -> Self {
        let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
        println!("workdir {}", workdir.path().to_str().unwrap());
        let store = create_store(&get_store_path(workdir.path()));
        let trie = Arc::new(Trie::new(store.clone()));

        let mut state_file = dump_dir.to_path_buf();
        state_file.push(STATE_DUMP_FILE);
        store.load_from_file(ColState, state_file.as_path()).expect("Failed to read state dump");
        let mut roots_files = dump_dir.to_path_buf();
        roots_files.push(GENESIS_ROOTS_FILE);
        let mut file = File::open(roots_files).expect("Failed to open genesis roots file.");
        let mut data = vec![];
        file.read_to_end(&mut data).expect("Failed to read genesis roots file.");
        let state_roots: Vec<StateRoot> =
            BorshDeserialize::try_from_slice(&data).expect("Failed to deserialize genesis roots");
        assert!(state_roots.len() <= 1, "Parameter estimation works with one shard only.");
        assert!(!state_roots.is_empty(), "No state roots found.");
        let root = state_roots[0];

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

        let runtime = Runtime::new(runtime_config);
        let prev_receipts = vec![];

        let apply_state = ApplyState {
            // Put each runtime into a separate shard.
            block_index: 0,
            // Epoch length is long enough to avoid corner cases.
            epoch_length: 4,
            epoch_height: 0,
            gas_price: 1,
            block_timestamp: 0,
            gas_limit: None,
        };
        Self { workdir, trie, root, runtime, prev_receipts, apply_state }
    }

    pub fn process_block(
        &mut self,
        transactions: &[SignedTransaction],
        allow_failures: bool,
    ) -> Gas {
        let apply_result = self
            .runtime
            .apply(
                self.trie.clone(),
                self.root,
                &None,
                &self.apply_state,
                &self.prev_receipts,
                transactions,
            )
            .unwrap();

        let (store_update, root) = apply_result.trie_changes.into(self.trie.clone()).unwrap();
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
}
