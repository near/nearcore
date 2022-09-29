//! State viewer functions to read gas profile information from execution
//! outcomes stored in RocksDB.

use near_primitives::profile::Cost;
use near_primitives::transaction::ExecutionOutcome;
use near_primitives_core::parameter::Parameter;
use node_runtime::config::RuntimeConfig;
use std::collections::BTreeMap;

pub(crate) struct GasFeeCounters {
    counters: BTreeMap<Parameter, u64>,
}

pub(crate) fn extract_gas_counters(
    outcome: ExecutionOutcome,
    config: &RuntimeConfig,
) -> Option<GasFeeCounters> {
    match outcome.metadata {
        near_primitives::transaction::ExecutionMetadata::V1 => None,
        near_primitives::transaction::ExecutionMetadata::V2(meta_data) => {
            let mut counters = BTreeMap::new();
            for param in Parameter::ext_costs() {
                if let Some(cost) = param.cost() {
                    match cost {
                        // this is tricky because ActionCosts is not 1-to-1 with parameters
                        Cost::ActionCost { .. } => todo!(),
                        Cost::ExtCost { ext_cost_kind } => {
                            let parameter_value =
                                ext_cost_kind.value(&config.wasm_config.ext_costs);
                            let gas = meta_data.get_ext_cost(ext_cost_kind);
                            if parameter_value != 0 {
                                assert_eq!(
                                    0,
                                    gas % parameter_value,
                                    "invalid gas profile for given config"
                                );
                                let counter = gas / parameter_value;
                                *counters.entry(*param).or_default() += counter;
                            }
                        }
                        Cost::WasmInstruction => {
                            // this is tricky because we have to figure out how much gas has been burnt for function call execution
                            // let total_gas_burnt_for_fn_calls = ???;
                            // let gas = meta_data.compute_wasm_instruction_cost(total_gas_burnt_for_fn_calls);
                            // let counter = gas / config.wasm_config.regular_op_cost;
                            // counters.insert(param, counter)
                            todo!()
                        }
                    };
                }
                // for cost in Cost::ALL {
            }
            Some(GasFeeCounters { counters })
        }
    }
}

impl std::fmt::Display for GasFeeCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (param, counter) in self.counters.iter() {
            writeln!(f, "{param}: {counter}")?;
        }
        Ok(())
    }
}
