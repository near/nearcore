use super::transaction_builder::TransactionBuilder;
use crate::config::{Config, GasMetric};
use crate::gas_cost::GasCost;
use anyhow::Context;
use genesis_populate::get_account_id;
use genesis_populate::state_dump::StateDump;
use near_parameters::config::CongestionControlConfig;
use near_parameters::{ExtCosts, RuntimeConfigStore};
use near_primitives::congestion_info::{BlockCongestionInfo, ExtendedCongestionInfo};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::state::FlatStateValue;
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::types::{Gas, MerkleHash};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter, StoreUpdateCommit};
use near_store::flat::{
    BlockInfo, FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStorage,
    FlatStorageManager, FlatStorageReadyStatus, FlatStorageStatus,
};
use near_store::{ShardTries, ShardUId, StateSnapshotConfig, TrieUpdate};
use near_store::{TrieCache, TrieCachingStorage, TrieConfig};
use near_vm_runner::logic::LimitConfig;
use near_vm_runner::FilesystemContractRuntimeCache;
use node_runtime::{ApplyState, Runtime};
use std::collections::HashMap;
use std::iter;
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
    pub(crate) touching_trie_node_write: Option<GasCost>,
    pub(crate) ed25519_verify_base: Option<GasCost>,
    pub(crate) function_call_base: Option<GasCost>,
    #[cfg(feature = "nightly")]
    pub(crate) yield_create_base: Option<GasCost>,
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
            false,
        );
        // Ensure decent RocksDB SST file layout.
        store.compact().expect("compaction failed");

        assert!(roots.len() <= 1, "Parameter estimation works with one shard only.");
        assert!(!roots.is_empty(), "No state roots found.");
        let root = roots[0];

        let shard_uid = ShardUId::single_shard();
        let flat_store = store.flat_store();
        let flat_storage_manager = FlatStorageManager::new(flat_store.clone());
        let mut store_update = flat_store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus {
                flat_head: BlockInfo::genesis(CryptoHash::hash_borsh(0usize), 0),
            }),
        );
        store_update.commit().unwrap();
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();

        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
        self.generate_deltas(&flat_storage);

        // Create ShardTries with relevant settings adjusted for estimator.
        let mut trie_config = near_store::TrieConfig::default();
        trie_config.enable_receipt_prefetching = true;
        if self.config.memtrie {
            trie_config.load_mem_tries_for_shards = vec![shard_uid];
        }
        let tries = ShardTries::new(
            store,
            trie_config,
            &[shard_uid],
            flat_storage_manager,
            StateSnapshotConfig::default(),
        );
        if self.config.memtrie {
            // NOTE: Since the store loaded from the state dump only contains the state, we directly provide the state root
            // instead of  letting the loader code to locate it from the ChunkExtra (which is missing from the store).
            tries
                .load_mem_trie(&shard_uid, Some(root), true)
                .context("Failed load memtries for single shard")
                .unwrap();
        }
        let cache = FilesystemContractRuntimeCache::new(workdir.path(), None::<&str>)
            .expect("create contract cache");

        Testbed {
            config: self.config,
            _workdir: workdir,
            tries,
            root,
            runtime: Runtime::new(),
            prev_receipts: Vec::new(),
            apply_state: Self::make_apply_state(cache),
            epoch_info_provider: MockEpochInfoProvider::default(),
            transaction_builder: TransactionBuilder::new(
                (0..self.config.active_accounts)
                    .map(|index| get_account_id(index as u64))
                    .collect(),
            ),
        }
    }

    fn make_apply_state(cache: FilesystemContractRuntimeCache) -> ApplyState {
        let mut runtime_config =
            RuntimeConfigStore::new(None).get_config(PROTOCOL_VERSION).as_ref().clone();
        let wasm_config = Arc::make_mut(&mut runtime_config.wasm_config);
        wasm_config.enable_all_features();
        wasm_config.make_free();
        // Override vm limits config to simplify block processing.
        wasm_config.limit_config = LimitConfig {
            max_total_log_length: u64::MAX,
            max_number_registers: u64::MAX,
            max_gas_burnt: u64::MAX,
            max_register_size: u64::MAX,
            max_number_logs: u64::MAX,

            max_actions_per_receipt: u64::MAX,
            max_promises_per_function_call_action: u64::MAX,
            max_number_input_data_dependencies: u64::MAX,
            max_length_storage_key: u64::MAX,

            max_total_prepaid_gas: u64::MAX,

            ..wasm_config.limit_config
        };
        runtime_config.account_creation_config.min_allowed_top_level_account_length = 0;
        // Disable congestion control to simplify measuring large workloads.
        runtime_config.congestion_control_config = CongestionControlConfig::test_disabled();

        let shard_id = ShardUId::single_shard().shard_id();
        let congestion_info = if ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
            [(shard_id, ExtendedCongestionInfo::default())].into()
        } else {
            Default::default()
        };
        let congestion_info = BlockCongestionInfo::new(congestion_info);

        ApplyState {
            apply_reason: None,
            // Put each runtime into a separate shard.
            block_height: 1,
            // Epoch length is long enough to avoid corner cases.
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            shard_id,
            epoch_id: Default::default(),
            epoch_height: 0,
            gas_price: 0,
            block_timestamp: 0,
            gas_limit: None,
            random_seed: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(runtime_config),
            cache: Some(Box::new(cache)),
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
            congestion_info,
        }
    }

    /// Construct a chain of fake blocks with fake deltas for flat storage.
    ///
    /// Use `hash(height)` as the supposed block hash.
    /// Keys are randomly generated, values are a constant that's not even stored.
    ///
    /// The blocks aren't valid, nor are the values stored anywhere. They only
    /// exist within `FlatStorage` and simulate the performance decrease
    /// observed when the flat head lags behind.
    fn generate_deltas(&self, flat_storage: &FlatStorage) {
        // Assumption: One delta per non-final block, which is configurable.
        // There could be forks but that's considered to e outside the normal
        // operating conditions for this estimation.
        let num_deltas = self.config.finality_lag;
        // Number of keys changed is the same for all deltas and configurable.
        let num_changes_per_delta = self.config.fs_keys_per_delta;
        // This is the longest key we allow in storage.
        let delta_key_len = 2000;
        for idx in 0..num_deltas {
            // We want different keys and to avoid all optimization potential.
            // But the values are never read, so let's just use a dummy constant.
            let random_data = iter::repeat_with(|| {
                (
                    crate::utils::random_vec(delta_key_len),
                    Some(FlatStateValue::value_ref(b"this is never stored or accessed, we only need it to blow up in-memory deltas")),
                )
            })
            .take(num_changes_per_delta);
            let height = 1 + idx as u64;
            let block = BlockInfo {
                hash: fs_fake_block_height_to_hash(height),
                height,
                prev_hash: fs_fake_block_height_to_hash(height - 1),
            };

            flat_storage
                .add_delta(FlatStateDelta {
                    changes: FlatStateChanges::from(random_data),
                    metadata: FlatStateDeltaMetadata { block, prev_block_with_changes: None },
                })
                .unwrap();
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
            assert_eq!(
                block_latency, extra_blocks,
                "block latency {block_latency} does not match expected {extra_blocks}"
            );

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
        let trie = self.trie();
        let apply_result = self
            .runtime
            .apply(
                trie,
                &None,
                &self.apply_state,
                &self.prev_receipts,
                transactions,
                &self.epoch_info_provider,
                Default::default(),
            )
            .unwrap();

        let store = self.tries.get_store();
        let mut store_update = store.store_update();
        let shard_uid = ShardUId::single_shard();
        self.root = self.tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
        if self.config.memtrie {
            let memtrie_root = self
                .tries
                .apply_memtrie_changes(
                    &apply_result.trie_changes,
                    shard_uid,
                    self.apply_state.block_height,
                )
                .unwrap_or_else(|| {
                    panic!("Memtrie is enabled but failed to apply memtrie changes")
                });
            assert_eq!(self.root, memtrie_root);
        }
        near_store::flat::FlatStateChanges::from_state_changes(&apply_result.state_changes)
            .apply_to_flat_state(&mut store_update.flat_store_update(), shard_uid);
        store_update.commit().unwrap();
        self.apply_state.block_height += 1;
        if let Some(congestion_info) = apply_result.congestion_info {
            self.apply_state
                .congestion_info
                .insert(shard_uid.shard_id(), ExtendedCongestionInfo::new(congestion_info, 0));
        }

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

    /// Process just the verification of a transaction, without action execution.
    ///
    /// Use this method for measuring the SEND cost of actions. This is the
    /// workload done on the sender's shard before an action receipt is created.
    /// Network costs for sending are not included.
    pub(crate) fn verify_transaction(
        &mut self,
        tx: &SignedTransaction,
        metric: GasMetric,
    ) -> GasCost {
        let mut state_update = TrieUpdate::new(self.trie());
        // gas price and block height can be anything, it doesn't affect performance
        // but making it too small affects max_depth and thus pessimistic inflation
        let gas_price = 100_000_000;
        let block_height = None;
        // do a full verification
        let verify_signature = true;

        let clock = GasCost::measure(metric);
        node_runtime::verify_and_charge_transaction(
            &self.apply_state.config,
            &mut state_update,
            gas_price,
            tx,
            verify_signature,
            block_height,
            PROTOCOL_VERSION,
        )
        .expect("tx verification should not fail in estimator");
        clock.elapsed()
    }

    /// Process only the execution step of an action receipt.
    ///
    /// Use this method to estimate action exec costs.
    pub(crate) fn apply_action_receipt(&mut self, receipt: &Receipt, metric: GasMetric) -> GasCost {
        let mut state_update = TrieUpdate::new(self.trie());
        let mut outgoing_receipts = vec![];
        let mut validator_proposals = vec![];
        let mut stats = node_runtime::ApplyStats::default();
        // TODO: mock is not accurate, potential DB requests are skipped in the mock!
        let epoch_info_provider = MockEpochInfoProvider::new([].into_iter());
        let clock = GasCost::measure(metric);
        let exec_result = node_runtime::estimator::apply_action_receipt(
            &mut state_update,
            &self.apply_state,
            receipt,
            &mut outgoing_receipts,
            &mut validator_proposals,
            &mut stats,
            &epoch_info_provider,
        )
        .expect("applying receipt in estimator should not fail");
        let gas = clock.elapsed();
        match exec_result.outcome.status {
            ExecutionStatus::Unknown => panic!("receipt not applied"),
            ExecutionStatus::Failure(err) => panic!("failed apply, {err:?}"),
            ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_) => (),
        }
        gas
    }

    /// Instantiate a new trie for the estimator.
    fn trie(&mut self) -> near_store::Trie {
        // We generated `finality_lag` fake blocks earlier, so the fake height
        // will be at the same number.
        let tip_height = self.config.finality_lag;
        let tip = fs_fake_block_height_to_hash(tip_height as u64);
        self.tries.get_trie_with_block_hash_for_shard(
            ShardUId::single_shard(),
            self.root,
            &tip,
            false,
        )
    }
}

/// Maps fake block heights to block hashes.
///
/// This is ued to generate and access fake deltas for flat storage.
fn fs_fake_block_height_to_hash(height: u64) -> CryptoHash {
    CryptoHash::hash_borsh(height)
}
