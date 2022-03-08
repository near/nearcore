use crate::tests::fixtures::get_context;
use crate::tests::helpers::*;
use crate::tests::vm_logic_builder::VMLogicBuilder;
use crate::types::Gas;
use crate::{VMConfig, VMLogic};

#[test]
fn test_dont_burn_gas_when_exceeding_attached_gas_limit() {
    let gas_limit = 10u64.pow(14);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit * 2);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit);

    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    promise_batch_action_function_call(&mut logic, index, 0, gas_limit * 2)
        .expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    // Just avoid hard-coding super-precise amount of gas burnt.
    assert!(outcome.burnt_gas < gas_limit / 2);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[test]
fn test_limit_wasm_gas_after_attaching_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit * 2);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit);

    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    promise_batch_action_function_call(&mut logic, index, 0, gas_limit / 2)
        .expect("should add action to receipt");
    logic.gas((op_limit / 2) as u32).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.used_gas, gas_limit);
    assert!(gas_limit / 2 < outcome.burnt_gas);
    assert!(outcome.burnt_gas < gas_limit);
}

#[test]
fn test_cant_burn_more_than_max_gas_burnt_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit * 2);

    logic.gas(op_limit * 3).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit * 2);
}

#[test]
fn test_cant_burn_more_than_prepaid_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit * 2);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit);

    logic.gas(op_limit * 3).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[test]
fn test_hit_max_gas_burnt_limit() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit * 3);

    promise_create(&mut logic, b"rick.test", 0, gas_limit / 2).expect("should create a promise");
    logic.gas(op_limit * 2).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert!(outcome.used_gas > gas_limit * 2);
}

#[test]
fn test_hit_prepaid_gas_limit() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit * 3);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit);

    promise_create(&mut logic, b"rick.test", 0, gas_limit / 2).expect("should create a promise");
    logic.gas(op_limit * 2).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[cfg(feature = "protocol_feature_function_call_weight")]
fn function_call_weight_check(function_calls: impl IntoIterator<Item = (u64, u64)>) {
    use near_primitives::types::GasWeight;

    let gas_limit = 10u64.pow(14);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit);

    for (static_gas, gas_weight) in function_calls {
        let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
        promise_batch_action_function_call_weight(
            &mut logic,
            index,
            0,
            static_gas,
            GasWeight(gas_weight),
        )
        .expect("batch action function call should succeed");
    }
    let outcome = logic.compute_outcome_and_distribute_gas();

    // Verify that all gas was consumed (assumes at least one ratio is provided)
    assert!(outcome.used_gas == gas_limit);
}

#[cfg(feature = "protocol_feature_function_call_weight")]
#[test]
fn function_call_weight_single_smoke_test() {
    // Single function call
    function_call_weight_check([(0, 1)]);

    // Single function with static gas
    function_call_weight_check([(888, 1)]);

    // Large weight
    function_call_weight_check([(0, 88888)]);

    // Weight larger than gas limit
    function_call_weight_check([(0, 11u64.pow(14))]);

    // Split two
    function_call_weight_check([(0, 3), (0, 2)]);

    // Split two with static gas
    function_call_weight_check([(1_000_000, 3), (3_000_000, 2)]);

    // Many different gas weights
    function_call_weight_check([
        (1_000_000, 3),
        (3_000_000, 2),
        (0, 1),
        (1_000_000_000, 0),
        (0, 4),
    ]);

    // Weight over u64 bounds
    function_call_weight_check([(0, u64::MAX), (0, 1000)]);
}

impl VMLogicBuilder {
    fn max_gas_burnt(mut self, max_gas_burnt: Gas) -> Self {
        self.config.limit_config.max_gas_burnt = max_gas_burnt;
        self
    }

    fn build_with_prepaid_gas(&mut self, prepaid_gas: Gas) -> VMLogic<'_> {
        let mut context = get_context(vec![], false);
        context.prepaid_gas = prepaid_gas;
        self.build(context)
    }
}

/// Given the limit in gas, compute the corresponding limit in wasm ops for use
/// with [`VMLogic::gas`] function.
fn op_limit(gas_limit: Gas) -> u32 {
    (gas_limit / (VMConfig::test().regular_op_cost as u64)) as u32
}
