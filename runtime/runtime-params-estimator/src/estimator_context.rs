use near_primitives::shard_layout::ShardUId;
use std::collections::HashMap;

use near_primitives::transaction::SignedTransaction;
use near_store::{TrieCache, TrieCachingStorage};
use near_vm_logic::ExtCosts;

use crate::config::{Config, GasMetric};
use crate::gas_cost::GasCost;
use crate::testbed::RuntimeTestbed;
use genesis_populate::get_account_id;

use super::transaction_builder::TransactionBuilder;

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
}

impl<'c> EstimatorContext<'c> {
    pub(crate) fn new(config: &'c Config) -> Self {
        let cached = CachedCosts::default();
        Self { cached, config }
    }

    pub(crate) fn testbed(&mut self) -> Testbed<'_> {
        let inner = RuntimeTestbed::from_state_dump(&self.config.state_dump_path);
        Testbed {
            config: self.config,
            inner,
            transaction_builder: TransactionBuilder::new(
                (0..self.config.active_accounts)
                    .map(|index| get_account_id(index as u64))
                    .collect(),
            ),
        }
    }
}

/// A single isolated instance of runtime.
///
/// We use it to time processing a bunch of blocks.
pub(crate) struct Testbed<'c> {
    pub(crate) config: &'c Config,
    inner: RuntimeTestbed,
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
                self.inner.process_block(&block, allow_failures);
                extra_blocks = self.inner.process_blocks_until_no_receipts(allow_failures);
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
        self.inner.process_block(&block, allow_failures);
        let extra_blocks = self.inner.process_blocks_until_no_receipts(allow_failures);
        assert_eq!(block_latency, extra_blocks);
    }

    pub(crate) fn trie_caching_storage(&mut self) -> TrieCachingStorage {
        let store = self.inner.store();
        let caching_storage = TrieCachingStorage::new(
            store,
            TrieCache::new(0, false),
            ShardUId::single_shard(),
            false,
        );
        caching_storage
    }

    pub(crate) fn clear_caches(&mut self) {
        // Flush out writes hanging in memtable
        self.inner.flush_db_write_buffer();

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
}
