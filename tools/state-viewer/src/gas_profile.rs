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
    outcome: &ExecutionOutcome,
    runtime_config: &RuntimeConfig,
) -> Option<GasFeeCounters> {
    match &outcome.metadata {
        near_primitives::transaction::ExecutionMetadata::V1 => None,
        near_primitives::transaction::ExecutionMetadata::V2(meta_data) => {
            let mut counters = BTreeMap::new();

            for param in Parameter::ext_costs() {
                match param.cost().unwrap_or_else(|| panic!("ext cost {param} must have a cost")) {
                    Cost::ExtCost { ext_cost_kind } => {
                        let parameter_value =
                            ext_cost_kind.value(&runtime_config.wasm_config.ext_costs);
                        let gas = meta_data.get_ext_cost(ext_cost_kind);
                        if parameter_value != 0 && gas != 0 {
                            assert_eq!(
                                0,
                                gas % parameter_value,
                                "invalid gas profile for given config"
                            );
                            let counter = gas / parameter_value;
                            *counters.entry(*param).or_default() += counter;
                        }
                    }
                    _ => unreachable!("{param} must be ExtCost"),
                };
            }

            let num_wasm_ops = meta_data[Cost::WasmInstruction]
                / runtime_config.wasm_config.regular_op_cost as u64;
            if num_wasm_ops != 0 {
                *counters.entry(Parameter::WasmRegularOpCost).or_default() += num_wasm_ops;
            }

            // TODO: Action costs should also be included.
            // This is tricky, however. From just the gas numbers in the profile
            // we cannot know the cost is split to parameters. Because base and byte
            // costs are all merged. Same for different type of access keys.
            // The only viable way right now is go through each action separately and
            // recompute the gas cost from scratch. For promises in function
            // calls that includes looping through outgoing promises and again
            // recomputing the gas costs.
            // And of course one has to consider that some actions will be SIR
            // and some will not be.
            //
            // For now it is not clear if implementing this is even worth it.
            // Alternatively, we could also make the profile data more detailed.

            Some(GasFeeCounters { counters })
        }
    }
}

impl std::fmt::Display for GasFeeCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (param, counter) in self.counters.iter() {
            writeln!(f, "{param:<48} {counter:>16}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::profile::ProfileData;
    use near_primitives::transaction::ExecutionMetadata;
    use near_primitives::types::AccountId;
    use node_runtime::config::RuntimeConfig;

    #[test]
    fn test_extract_gas_counters() {
        let config = RuntimeConfig::test();
        let costs = [
            (Parameter::WasmStorageWriteBase, 137),
            (Parameter::WasmStorageWriteKeyByte, 4629),
            (Parameter::WasmStorageWriteValueByte, 2246),
            // note: actions are not included in profile, yet
            (Parameter::ActionDeployContractExecution, 2 * 184765750000),
            (Parameter::ActionDeployContractSendSir, 2 * 184765750000),
            (Parameter::ActionDeployContractPerByteSendSir, 1024 * 6812999),
            (Parameter::ActionDeployContractPerByteExecution, 1024 * 64572944),
            (Parameter::WasmRegularOpCost, 7000),
        ];

        let outcome = create_execution_outcome(&costs, &config);
        let profile = extract_gas_counters(&outcome, &config).expect("no counters returned");

        insta::assert_display_snapshot!(profile);
    }

    fn create_execution_outcome(
        costs: &[(Parameter, u64)],
        config: &RuntimeConfig,
    ) -> ExecutionOutcome {
        let mut gas_burnt = 0;
        let mut profile_data = ProfileData::new();
        for &(parameter, value) in costs {
            match parameter.cost() {
                Some(Cost::ExtCost { ext_cost_kind }) => {
                    let gas = value * ext_cost_kind.value(&config.wasm_config.ext_costs);
                    profile_data.add_ext_cost(ext_cost_kind, gas);
                    gas_burnt += gas;
                }
                Some(Cost::WasmInstruction) => {
                    let gas = value * config.wasm_config.regular_op_cost as u64;
                    profile_data[Cost::WasmInstruction] += gas;
                    gas_burnt += gas;
                }
                // Multiplying for actions isn't possible because costs can be
                // split into multiple parameters. Caller has to specify exact
                // values.
                Some(Cost::ActionCost { action_cost_kind }) => {
                    profile_data.add_action_cost(action_cost_kind, value);
                    gas_burnt += value;
                }
                _ => unimplemented!(),
            }
        }
        let metadata = ExecutionMetadata::V2(profile_data);
        let account_id: AccountId = "alice.near".to_owned().try_into().unwrap();
        ExecutionOutcome {
            logs: vec![],
            receipt_ids: vec![],
            gas_burnt,
            tokens_burnt: 0,
            executor_id: account_id.clone(),
            status: near_primitives::transaction::ExecutionStatus::SuccessValue(vec![]),
            metadata,
        }
    }
}
