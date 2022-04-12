use near_primitives::hash::{hash, CryptoHash};
use near_primitives::shard_layout::ShardUId;
use rand::prelude::SliceRandom;
use std::collections::HashMap;

use near_primitives::transaction::SignedTransaction;
use near_primitives::types::TrieCacheMode;
use near_store::{RawTrieNode, RawTrieNodeWithSize, TrieCache, TrieCachingStorage, TrieStorage};
use near_vm_logic::ExtCosts;

use crate::config::{Config, GasMetric};
use crate::gas_cost::GasCost;
use crate::testbed::RuntimeTestbed;
use crate::utils::get_account_id;

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
    pub(crate) action_function_call_base_per_byte_v2: Option<(GasCost, GasCost)>,
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
                (0..self.config.active_accounts).map(get_account_id).collect(),
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

impl<'c> Testbed<'c> {
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
    pub(crate) fn measure_blocks<'a>(
        &'a mut self,
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

    pub(crate) fn measure_trie_node_reads(
        &mut self,
        iters: usize,
        num_values: usize,
    ) -> Vec<(GasCost, HashMap<ExtCosts, u64>)> {
        (0..iters)
            .map(|_| {
                let tb = self.transaction_builder();

                let value_len: usize = 2000;
                let signer = tb.random_account();
                let values: Vec<_> = (0..num_values)
                    .map(|_| {
                        let v = tb.random_vec(value_len);
                        let h = hash(&v);
                        let node = RawTrieNode::Extension(v, h);
                        let node_with_size = RawTrieNodeWithSize { node, memory_usage: 1 };
                        node_with_size.encode().unwrap()
                    })
                    .collect();
                let mut setup_block = Vec::new();
                let mut blocks = vec![];
                for (i, value) in values.iter().cloned().enumerate() {
                    let key = vec![i as u8];
                    setup_block.push(tb.account_insert_key_bytes(signer.clone(), key, value));

                    if i % 600 == 599 {
                        blocks.push(setup_block.clone());
                        setup_block = vec![];
                    }
                }
                for v in blocks.iter() {
                    eprintln!("{}", v.len());
                }
                let value_hashes: Vec<_> = values.iter().map(|value| hash(value)).collect();
                self.measure_blocks(blocks, 0);

                let store = self.inner.store();
                let caching_storage =
                    TrieCachingStorage::new(store, TrieCache::new(), ShardUId::single_shard());
                caching_storage.set_mode(TrieCacheMode::CachingChunk);

                let results: Vec<_> = (0..2)
                    .map(|_| {
                        self.clear_caches();
                        let start = GasCost::measure(self.config.metric);
                        let sum: usize = value_hashes
                            .iter()
                            .map(|key| {
                                let bytes = match caching_storage.retrieve_raw_bytes(key) {
                                    Ok(bytes) => bytes,
                                    _ => {
                                        // eprintln!("issue");
                                        return 0;
                                    }
                                };
                                let node = RawTrieNodeWithSize::decode(&bytes).unwrap();
                                match node.node {
                                    RawTrieNode::Extension(v, _) => v.len(),
                                    _ => {
                                        unreachable!();
                                    }
                                }
                            })
                            .sum();
                        // assert_eq!(sum, num_values * value_len);
                        (start.elapsed(), HashMap::new())
                    })
                    .collect();

                results.iter().for_each(|(cost, _)| {
                    eprintln!("cost = {:?}", cost);
                });

                results[results.len() - 1].clone()
            })
            .collect()
    }

    fn clear_caches(&mut self) {
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
