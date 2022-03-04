use std::collections::HashMap;
use std::sync::Arc;

use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::CompiledContractCache;
use near_store::{create_store, StoreCompiledContractCache};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{ExtCosts, ProtocolVersion};
use nearcore::get_store_path;
use tempfile::TempDir;

use crate::config::{Config, GasMetric};
use crate::gas_cost::GasCost;
use crate::testbed::RuntimeTestbed;
use crate::utils::get_account_id;
use crate::vm_estimator::create_context;

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
}

/// A raw runtime to estimate costs that don't need anything external. Also
/// allows to compare VMs using the --vm-kind option.
pub(crate) struct EstimatorRuntime {
    #[allow(dead_code)]
    workdir: TempDir,
    runner: Box<dyn Fn(&ContractCode, &str) -> GasCost>,
}

impl EstimatorRuntime {
    pub fn run(&self, contract: &ContractCode, method: &str) -> GasCost {
        (self.runner)(contract, method)
    }
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

    pub(crate) fn raw_runtime(&mut self) -> EstimatorRuntime {
        let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
        let store = create_store(&get_store_path(workdir.path()));
        let cache_store = Arc::new(StoreCompiledContractCache { store });
        let protocol_version = ProtocolVersion::MAX;
        let config_store = RuntimeConfigStore::new(None);
        let runtime_config = config_store.get_config(protocol_version).as_ref();
        let fees = runtime_config.transaction_costs.clone();
        let mut vm_config = runtime_config.wasm_config.clone();
        let gas_metric = self.config.metric;
        vm_config.limit_config.max_gas_burnt = u64::MAX;
        let runtime = self.config.vm_kind.runtime(vm_config).expect("runtime has not been enabled");

        let runner = Box::new(move |contract: &ContractCode, method_name: &str| {
            let mut fake_external = MockedExternal::new();
            let fake_context = create_context(vec![]);
            let promise_results = vec![];
            let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());

            let start = GasCost::measure(gas_metric);
            let result = runtime.run(
                contract,
                method_name,
                &mut fake_external,
                fake_context.clone(),
                &fees,
                &promise_results,
                protocol_version,
                cache,
            );
            let cost = start.elapsed();
            if let Some(err) = result.1 {
                panic!("Error executing function {}", err);
            }
            cost
        });

        EstimatorRuntime { workdir, runner }
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

    pub(crate) fn measure_blocks<'a>(
        &'a mut self,
        blocks: Vec<Vec<SignedTransaction>>,
    ) -> Vec<(GasCost, HashMap<ExtCosts, u64>)> {
        let allow_failures = false;

        let mut res = Vec::with_capacity(blocks.len());

        for block in blocks {
            node_runtime::with_ext_cost_counter(|cc| cc.clear());
            let gas_cost = {
                self.clear_caches();
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
