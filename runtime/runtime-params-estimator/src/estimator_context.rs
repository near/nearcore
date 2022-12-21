use super::transaction_builder::TransactionBuilder;
use crate::config::{Config, GasMetric};
use crate::gas_cost::GasCost;
use genesis_populate::get_account_id;
use genesis_populate::state_dump::StateDump;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::types::{Gas, MerkleHash};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::{ShardTries, ShardUId, Store, StoreCompiledContractCache};
use near_store::{TrieCache, TrieCachingStorage, TrieConfig};
use near_vm_logic::{ExtCosts, VMLimitConfig};
use node_runtime::{ApplyState, Runtime};
use std::collections::HashMap;
use std::sync::Arc;

/// Global context shared by all cost calculating functions.
pub(crate) struct EstimatorContext<'c> {
    pub(crate) config: &'c Config,
    pub(crate) cached: CachedCosts,
}

#[derive(Default)]
pub(crate) struct CachedCosts {
    pub(crate) action_receipt_creation: Option<GasCost>,
    pub(crate) action_sir_receipt_creation: Option<GasCost>,
    pub(crate) action_add_function_access_key_base: Option<GasCost>,
    pub(crate) deploy_contract_base: Option<GasCost>,
    pub(crate) noop_function_call_cost: Option<GasCost>,
    pub(crate) storage_read_base: Option<GasCost>,
    pub(crate) contract_loading_base_per_byte: Option<(GasCost, GasCost)>,
    pub(crate) compile_cost_base_per_byte: Option<(GasCost, GasCost)>,
    pub(crate) compile_cost_base_per_byte_v2: Option<(GasCost, GasCost)>,
    pub(crate) gas_metering_cost_base_per_op: Option<(GasCost, GasCost)>,
    pub(crate) apply_block: Option<GasCost>,
    pub(crate) touching_trie_node_read: Option<GasCost>,
    pub(crate) touching_trie_node_write: Option<GasCost>,
    #[cfg(feature = "protocol_feature_ed25519_verify")]
    pub(crate) ed25519_verify_base: Option<GasCost>,
}

impl<'c> EstimatorContext<'c> {
    pub(crate) fn new(config: &'c Config) -> Self {
        let cached = CachedCosts::default();
        Self { cached, config }
    }

    pub(crate) fn testbed(&mut self) -> Testbed<'_> {
        // Copies dump from another directory and loads the state from it.
        let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
        let StateDump { store, roots } = StateDump::from_dir(
            &self.config.state_dump_path,
            workdir.path(),
            self.config.in_memory_db,
        );
        // Ensure decent RocksDB SST file layout.
        store.compact().expect("compaction failed");

        assert!(roots.len() <= 1, "Parameter estimation works with one shard only.");
        assert!(!roots.is_empty(), "No state roots found.");
        let root = roots[0];

        // Create ShardTries with relevant settings adjusted for estimator.
        let shard_uids = [ShardUId { shard_id: 0, version: 0 }];
        let mut trie_config = near_store::TrieConfig::default();
        trie_config.enable_receipt_prefetching = true;
        let tries = ShardTries::new(
            store.clone(),
            trie_config,
            &shard_uids,
            near_store::flat_state::FlatStateFactory::new(store.clone()),
        );

        Testbed {
            config: self.config,
            _workdir: workdir,
            tries,
            root,
            runtime: Runtime::new(),
            prev_receipts: Vec::new(),
            apply_state: Self::make_apply_state(store.clone()),
            epoch_info_provider: MockEpochInfoProvider::default(),
            transaction_builder: TransactionBuilder::new(
                (0..self.config.active_accounts)
                    .map(|index| get_account_id(index as u64))
                    .collect(),
            ),
        }
    }

    fn make_apply_state(store: Store) -> ApplyState {
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

        ApplyState {
            // Put each runtime into a separate shard.
            block_height: 1,
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
            cache: Some(Box::new(StoreCompiledContractCache::new(&store))),
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        }
    }
}

/// A single isolated instance of runtime.
///
/// We use it to time processing a bunch of blocks.
pub(crate) struct Testbed<'c> {
    pub(crate) config: &'c Config,
    /// Directory where we temporarily keep the storage.
    _workdir: tempfile::TempDir,
    tries: ShardTries,
    root: MerkleHash,
    runtime: Runtime,
    prev_receipts: Vec<Receipt>,
    apply_state: ApplyState,
    epoch_info_provider: MockEpochInfoProvider,
    transaction_builder: TransactionBuilder,
}

impl Testbed<'_> {
    pub(crate) fn transaction_builder(&mut self) -> &mut TransactionBuilder {
        &mut self.transaction_builder
    }

    /// Apply and measure provided blocks one-by-one.
    /// Because some transactions can span multiple blocks, each input block
    /// might trigger multiple blocks in execution. The returned results are
    /// exactly one per input block, regardless of how many blocks needed to be
    /// executed. To avoid surprises in how many blocks are actually executed,
    /// `block_latency` must be specified and the function will panic if it is
    /// wrong. A latency of 0 means everything is done within a single block.
    #[track_caller]
    pub(crate) fn measure_blocks(
        &mut self,
        blocks: Vec<Vec<SignedTransaction>>,
        block_latency: usize,
    ) -> Vec<(GasCost, HashMap<ExtCosts, u64>)> {
        let allow_failures = false;

        let mut res = Vec::with_capacity(blocks.len());

        for block in blocks {
            node_runtime::with_ext_cost_counter(|cc| cc.clear());
            let extra_blocks;
            let gas_cost = {
                self.clear_caches();
                let start = GasCost::measure(self.config.metric);
                self.process_block_impl(&block, allow_failures);
                extra_blocks = self.process_blocks_until_no_receipts(allow_failures);
                start.elapsed()
            };
            assert_eq!(block_latency, extra_blocks);

            let mut ext_costs: HashMap<ExtCosts, u64> = HashMap::new();
            node_runtime::with_ext_cost_counter(|cc| {
                for (c, v) in cc.drain() {
                    ext_costs.insert(c, v);
                }
            });
            res.push((gas_cost, ext_costs));
        }

        res
    }

    pub(crate) fn process_block(&mut self, block: Vec<SignedTransaction>, block_latency: usize) {
        let allow_failures = false;
        self.process_block_impl(&block, allow_failures);
        let extra_blocks = self.process_blocks_until_no_receipts(allow_failures);
        assert_eq!(block_latency, extra_blocks);
    }

    pub(crate) fn trie_caching_storage(&mut self) -> TrieCachingStorage {
        let store = self.tries.get_store();
        let is_view = false;
        let prefetcher = None;
        let caching_storage = TrieCachingStorage::new(
            store,
            TrieCache::new(&TrieConfig::default(), ShardUId::single_shard(), false),
            ShardUId::single_shard(),
            is_view,
            prefetcher,
        );
        caching_storage
    }

    pub(crate) fn clear_caches(&mut self) {
        // Flush out writes hanging in memtable
        self.tries.get_store().flush().unwrap();

        // OS caches:
        // - only required in time based measurements, since ICount looks at syscalls directly.
        // - requires sudo, therefore this is executed optionally
        if self.config.metric == GasMetric::Time && self.config.drop_os_cache {
            #[cfg(target_os = "linux")]
            crate::utils::clear_linux_page_cache().expect(
                "Failed to drop OS caches. Are you root and is /proc mounted with write access?",
            );
            #[cfg(not(target_os = "linux"))]
            panic!("Cannot drop OS caches on non-linux systems.");
        }
    }

    fn process_block_impl(
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

        let mut store_update = self.tries.store_update();
        self.root = self.tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();
        self.apply_state.block_height += 1;

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
    fn process_blocks_until_no_receipts(&mut self, allow_failures: bool) -> usize {
        let mut n = 0;
        while !self.prev_receipts.is_empty() {
            self.process_block_impl(&[], allow_failures);
            n += 1;
        }
        n
    }
}
