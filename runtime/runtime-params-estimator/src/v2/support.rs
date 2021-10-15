use std::collections::HashMap;
use std::path::Path;

use near_primitives::transaction::{Action, DeployContractAction, SignedTransaction};
use near_primitives::types::AccountId;
use near_vm_logic::ExtCosts;

use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::{end_count, get_account_id, start_count, Config};
use crate::v2::gas_cost::GasCost;

use super::ctx::TransactionBuilder;

#[derive(Default)]
pub(crate) struct CachedCosts {
    pub(crate) action_receipt_creation: Option<GasCost>,
    pub(crate) action_sir_receipt_creation: Option<GasCost>,
    pub(crate) action_add_function_access_key_base: Option<GasCost>,
    pub(crate) action_deploy_contract_base: Option<GasCost>,
    pub(crate) noop_host_function_call_cost: Option<GasCost>,
    pub(crate) action_function_call_base_per_byte_v2: Option<(GasCost, GasCost)>,
}

/// Global context shared by all cost calculating functions.
pub(crate) struct Ctx<'c> {
    pub(crate) config: &'c Config,
    pub(crate) cached: CachedCosts,
    contracts_testbed: Option<ContractTestbedProto>,
}

struct ContractTestbedProto {
    state_dump: tempfile::TempDir,
    transaction_builder: TransactionBuilder,
}

impl<'c> Ctx<'c> {
    pub(crate) fn new(config: &'c Config) -> Self {
        let cached = CachedCosts::default();
        Self { cached, config, contracts_testbed: None }
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

    pub(crate) fn test_bed_with_contracts(&mut self) -> TestBed<'_> {
        if self.contracts_testbed.is_none() {
            let code = self.read_resource(if cfg!(feature = "nightly_protocol_features") {
                "test-contract/res/nightly_small_contract.wasm"
            } else {
                "test-contract/res/stable_small_contract.wasm"
            });

            let mut test_bed = self.test_bed();
            let accounts = deploy_contracts(&mut test_bed, code);
            test_bed.inner.dump_state().unwrap();

            let transaction_builder = test_bed.transaction_builder.with_accounts(accounts);
            self.contracts_testbed = Some(ContractTestbedProto {
                state_dump: test_bed.inner.workdir,
                transaction_builder,
            });
        }
        let proto = self.contracts_testbed.as_ref().unwrap();

        let inner = RuntimeTestbed::from_state_dump(proto.state_dump.path());
        TestBed {
            config: &self.config,
            inner,
            transaction_builder: proto.transaction_builder.clone(),
        }
    }

    pub(crate) fn read_resource(&mut self, path: &str) -> Vec<u8> {
        let dir = env!("CARGO_MANIFEST_DIR");
        let path = Path::new(dir).join(path);
        std::fs::read(&path).unwrap_or_else(|err| {
            panic!("failed to load test resource: {}, {}", path.display(), err)
        })
    }
}

fn deploy_contracts(test_bed: &mut TestBed, code: Vec<u8>) -> Vec<AccountId> {
    let mut accounts_with_code = Vec::new();
    for _ in 0..3 {
        let block_size = 100;
        let n_blocks = test_bed.config.warmup_iters_per_block + test_bed.config.iter_per_block;
        let blocks = {
            let mut blocks = Vec::with_capacity(n_blocks);
            for _ in 0..n_blocks {
                let mut block = Vec::with_capacity(block_size);
                for _ in 0..block_size {
                    let tb = test_bed.transaction_builder();
                    let sender = tb.random_unused_account();
                    let receiver = sender.clone();

                    accounts_with_code.push(sender.clone());

                    let actions =
                        vec![Action::DeployContract(DeployContractAction { code: code.clone() })];
                    let tx = tb.transaction_from_actions(sender, receiver, actions);
                    block.push(tx);
                }
                blocks.push(block);
            }
            blocks
        };
        test_bed.measure_blocks(blocks);
    }
    accounts_with_code
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
            let start = start_count(self.config.metric);
            self.inner.process_block(&block, allow_failures);
            self.inner.process_blocks_until_no_receipts(allow_failures);
            let measured = end_count(self.config.metric, &start);

            let gas_cost = GasCost { value: measured.into(), metric: self.config.metric };

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
