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
fn function_call_weight_check(function_calls: &[(Gas, u64)]) {
    use crate::receipt_manager::ReceiptMetadata;
    use near_primitives::transaction::{Action, FunctionCallAction};
    use near_primitives::types::GasWeight;

    let gas_limit = 10u64.pow(14);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit);

    let mut ratios = vec![];

    // Schedule all function calls
    for (static_gas, gas_weight) in function_calls {
        let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
        promise_batch_action_function_call_weight(
            &mut logic,
            index,
            0,
            *static_gas,
            GasWeight(*gas_weight),
        )
        .expect("batch action function call should succeed");
        ratios.push((index, *gas_weight));
    }

    fn assert_with_gas(receipt: &ReceiptMetadata, cb: impl Fn(Gas) -> bool) {
        if let Action::FunctionCall(FunctionCallAction { gas, .. }) = receipt.actions()[0] {
            assert!(cb(gas));
        } else {
            panic!("expected function call action");
        }
    }

    let (gas_per_weight, remainder_gas) = {
        let gas_to_distribute = logic.gas_counter().remaining_gas();
        let weight_sum: u64 = function_calls
            .iter()
            .map(|(_, weight)| weight)
            // Saturating sum of weights
            .fold(0, |sum, val| sum.checked_add(*val).unwrap_or(u64::MAX));
        (gas_to_distribute / weight_sum, gas_to_distribute % weight_sum)
    };

    // Test static gas assigned before
    let receipts = logic.receipt_manager().action_receipts.0.iter().map(|(_, rec)| rec);
    for (receipt, (static_gas, _)) in receipts.zip(function_calls) {
        assert_with_gas(receipt, |gas| gas == *static_gas);
    }

    let outcome = logic.compute_outcome_and_distribute_gas();

    // Test gas is distributed after outcome calculated.
    let receipts = outcome.action_receipts().0.iter().map(|(_, rec)| rec);

    // Assert lengths are equal for zip
    assert_eq!(receipts.len(), function_calls.len());

    // Assert sufficient amount was given to
    for (receipt, (static_gas, weight)) in receipts.zip(function_calls) {
        assert_with_gas(receipt, |gas| gas >= static_gas + weight * gas_per_weight);
    }

    // Verify last receipt received all remaining gas
    let (static_gas, weight) = function_calls.last().unwrap();
    let (_, last_receipt) = outcome.action_receipts().0.last().unwrap();
    assert_with_gas(last_receipt, |gas| {
        gas == static_gas + weight * gas_per_weight + remainder_gas
    });

    // Verify that all gas was consumed (assumes at least one ratio is provided)
    assert_eq!(outcome.used_gas, gas_limit);
}

#[cfg(feature = "protocol_feature_function_call_weight")]
#[test]
fn function_call_weight_single_prop_test() {
    // Single function call
    function_call_weight_check(&[(0, 1)]);

    // Single function with static gas
    function_call_weight_check(&[(888, 1)]);

    // Large weight
    function_call_weight_check(&[(0, 88888)]);

    // Weight larger than gas limit
    function_call_weight_check(&[(0, 11u64.pow(14))]);

    // Split two
    function_call_weight_check(&[(0, 3), (0, 2)]);

    // Split two with static gas
    function_call_weight_check(&[(1_000_000, 3), (3_000_000, 2)]);

    // Many different gas weights
    function_call_weight_check(&[
        (1_000_000, 3),
        (3_000_000, 2),
        (0, 1),
        (1_000_000_000, 0),
        (0, 4),
    ]);

    // Weight over u64 bounds
    function_call_weight_check(&[(0, u64::MAX), (0, 1000)]);

    // Weights with one zero and one non-zero
    function_call_weight_check(&[(0, 0), (0, 1)])
}

#[cfg(feature = "protocol_feature_function_call_weight")]
#[test]
fn function_call_no_weight_refund() {
    use near_primitives::types::GasWeight;

    let gas_limit = 10u64.pow(14);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build_with_prepaid_gas(gas_limit);

    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    promise_batch_action_function_call_weight(&mut logic, index, 0, 1000, GasWeight(0))
        .expect("batch action function call should succeed");

    let outcome = logic.compute_outcome_and_distribute_gas();

    // Verify that unused gas was not allocated to function call
    assert!(outcome.used_gas < gas_limit);
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
