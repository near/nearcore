use std::collections::HashMap;

use near_primitives::transaction::SignedTransaction;
use near_vm_logic::ExtCosts;

use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::{get_account_id, Config};
use crate::v2::gas_cost::GasCost;

use super::transaction_builder::TransactionBuilder;

#[derive(Default)]
pub(crate) struct CachedCosts {
    pub(crate) action_receipt_creation: Option<GasCost>,
    pub(crate) action_sir_receipt_creation: Option<GasCost>,
    pub(crate) action_add_function_access_key_base: Option<GasCost>,
    pub(crate) action_deploy_contract_base: Option<GasCost>,
    pub(crate) noop_host_function_call_cost: Option<GasCost>,
    pub(crate) storage_read_base: Option<GasCost>,
    pub(crate) action_function_call_base_per_byte_v2: Option<(GasCost, GasCost)>,
}

/// Global context shared by all cost calculating functions.
pub(crate) struct Ctx<'c> {
    pub(crate) config: &'c Config,
    pub(crate) cached: CachedCosts,
}

impl<'c> Ctx<'c> {
    pub(crate) fn new(config: &'c Config) -> Self {
        let cached = CachedCosts::default();
        Self { cached, config }
    }

    pub(crate) fn test_bed(&mut self) -> TestBed<'_> {
        let inner = RuntimeTestbed::from_state_dump(&self.config.state_dump_path);
        TestBed {
            config: &self.config,
            inner,
            transaction_builder: TransactionBuilder::new(
                (0..self.config.active_accounts).map(get_account_id).collect(),
            ),
        }
    }
}

/// A single isolated instance of near.
///
/// We use it to time processing a bunch of blocks.
pub(crate) struct TestBed<'c> {
    pub(crate) config: &'c Config,
    inner: RuntimeTestbed,
    transaction_builder: TransactionBuilder,
}

impl<'c> TestBed<'c> {
    pub(crate) fn transaction_builder(&mut self) -> &mut TransactionBuilder {
        &mut self.transaction_builder
    }

    pub(crate) fn measure_blocks<'a>(
        &'a mut self,
        blocks: Vec<Vec<SignedTransaction>>,
    ) -> Vec<(GasCost, HashMap<ExtCosts, u64>)> {
        let allow_failures = false;

        let mut res = Vec::with_capacity(blocks.len());

        for block in blocks {
            node_runtime::with_ext_cost_counter(|cc| cc.clear());
            let gas_cost = {
                let start = GasCost::measure(self.config.metric);
                self.inner.process_block(&block, allow_failures);
                self.inner.process_blocks_until_no_receipts(allow_failures);
                start.elapsed()
            };

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
}
